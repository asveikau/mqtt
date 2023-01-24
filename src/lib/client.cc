/*
 Copyright (C) 2023-2024, 2026 Andrew Sveikauskas

 Permission to use, copy, modify, and distribute this software for any
 purpose with or without fee is hereby granted, provided that the above
 copyright notice and this permission notice appear in all copies.
*/

#include <mqtt/client.h>
#include <mqtt/messages.h>
#include <mqtt/reader.h>
#include <mqtt/writer.h>
#include <mqtt/util.h>
#include <mqtt/error.h>

#include <common/fieldoffset.h>
#include <common/minmax.h>
#include <common/c++/new.h>

#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <stdlib.h>
#include <string.h>

namespace
{
   template<int N>
   class HeapBufferList
   {
      void *buffers[N];
      void **current;
   public:
      HeapBufferList() : current(buffers) {}
      HeapBufferList(const HeapBufferList &) = delete;
      ~HeapBufferList()
      {
         Release();
      }
      void Add(void *p)
      {
         if (current - buffers < N)
            *current++ = p;
         else
            throw std::bad_alloc();
      }
      void Add(void *p, error *err)
      {
         try
         {
            Add(p);
         }
         catch (const std::bad_alloc &)
         {
            error_set_nomem(err);
         }
      }
      void Release()
      {
         while (current > buffers)
         {
            free(*--current);
         }
      }
   };

   class MessageIdSpace
   {
      std::unordered_set<uint16_t> OutstandingIds;
      uint16_t NextId;
   public:
      MessageIdSpace() : NextId(1) {}
      MessageIdSpace(const MessageIdSpace &) = delete;

      uint16_t
      GetId()
      {
         if (OutstandingIds.size() >= 0xfffe)
            return 0;
         uint16_t attempt;
         do
         {
            attempt = NextId++;
         } while (!attempt || OutstandingIds.find(attempt) != OutstandingIds.end());
         try
         {
            OutstandingIds.insert(attempt);
         }
         catch (const std::bad_alloc&)
         {
            return 0;
         }
         return attempt;
      }

      void
      Reclaim(uint16_t id)
      {
         OutstandingIds.erase(id);
      }
   };

   struct CbWrapper
   {
      void (*OnResponse)(void *, MqttPacket *, error *err);
      void *OnResponseCtx;
      void (*OnResponseFree)(void*);

      CbWrapper() :
         OnResponse(nullptr),
         OnResponseCtx(nullptr),
         OnResponseFree(nullptr)
      {
      }
      CbWrapper(
         void (*on_resp)(void *, MqttPacket*, error *),
         void *ctx,
         void (*on_free)(void*)
      ) : OnResponse(on_resp), OnResponseCtx(ctx), OnResponseFree(on_free)
      {
      }
      CbWrapper(const CbWrapper &) = delete;
      CbWrapper(CbWrapper &&other) :
         OnResponse(nullptr),
         OnResponseCtx(nullptr),
         OnResponseFree(nullptr)
      {
         *this = std::move(other);
      }
      ~CbWrapper()
      {
         Destroy();
      }

      void
      Destroy()
      {
         if (OnResponseFree)
            OnResponseFree(OnResponseCtx);
         Detach();
      }

      void
      Detach()
      {
         OnResponse = nullptr;
         OnResponseCtx = nullptr;
         OnResponseFree = nullptr;
      }

      CbWrapper &
      operator = (CbWrapper &&other)
      {
         if (&other != this)
         {
            Destroy();
            OnResponse = other.OnResponse;
            OnResponseCtx = other.OnResponseCtx;
            OnResponseFree = other.OnResponseFree;
            other.OnResponse = nullptr;
            other.OnResponseCtx = nullptr;
            other.OnResponseFree = nullptr;
         }
         return *this;
      }

      void operator ()(MqttPacket *pkt, error *err)
      {
         if (OnResponse)
            OnResponse(OnResponseCtx, pkt, err);
      }
   };

   struct
   ErrorCbWrapper
   {
      ErrorCbWrapper() : OnError(nullptr), OnErrorContext(nullptr), OnErrorContextFree(nullptr) {}
      ErrorCbWrapper(const ErrorCbWrapper &) = delete;
      ~ErrorCbWrapper()
      {
         if (OnErrorContextFree)
            OnErrorContextFree(OnErrorContext);
      }

      void (*OnError)(void *ctx, error *err,bool *shouldRetry);
      void *OnErrorContext;
      void (*OnErrorContextFree)(void*);

      void operator ()(error *err, bool *shouldRetry)
      {
         if (OnError)
            OnError(OnErrorContext, err, shouldRetry);
      }
   };

   struct Sub
   {
      std::vector<std::string> Topics;
      std::vector<unsigned char> Options;

      CbWrapper publishCb;
      CbWrapper responseCb;
      CbWrapper unsubCb;

      bool written;
      bool pendingDelete;
      uint16_t packetId;

      Sub() : written(false), pendingDelete(false), packetId(0) {}
   };

   struct PublishState
   {
      MqttPacket pkt;
      std::vector<unsigned char> payload;
      static const int NumStrings = 2;
      HeapBufferList<NumStrings> strings;
      MessageIdSpace *ids;
      int lastAck;

      CbWrapper responseCb;

      PublishState() : ids(nullptr), lastAck(0)
      {
         memset(&pkt, 0, sizeof(pkt));
      }
      PublishState(const PublishState&) = delete;
      ~PublishState()
      {
         if (pkt.Info.Publish.PacketId && ids)
            ids->Reclaim(pkt.Info.Publish.PacketId);
      }
   };
}

struct MqttClient_
{
   MqttClientIo *io;
   MessageIdSpace outgoingSubIds, &outgoingPubIds = outgoingSubIds;
   std::vector<uint16_t> idsToClearAtDisconnect;
   std::vector<char> packetOnHeap;
   std::vector<char> argsHeap;
   const char *username;
   const char *password;
   const char *clientid;
   char *clientIdFromServer;
   uint16_t keepAlive;
   bool sentPing;
   bool sawConnAck;
   bool reconnect;
   bool retainSupported;
   bool subIdSupported;
   unsigned char maximumQos;

   std::unordered_map<MqttPacketType, std::unordered_map<uint16_t, CbWrapper>> pendingReplies;
   std::unordered_map<MqttPacketType, CbWrapper> pendingRepliesWithoutId;
   std::unordered_map<uint64_t, Sub> subs;
   std::unordered_map<uint16_t, std::shared_ptr<PublishState>> publishes;
   std::vector<std::shared_ptr<PublishState>> pendingQos0Publishes;
   std::unordered_set<uint16_t> incomingPacketIds;

   ErrorCbWrapper errorCb;

   MqttClient_() :
      io(nullptr),
      username(nullptr),
      password(nullptr),
      clientid(nullptr),
      clientIdFromServer(nullptr),
      keepAlive(0),
      sentPing(false),
      sawConnAck(false),
      reconnect(false),
      retainSupported(true),
      subIdSupported(true),
      maximumQos(0)
   {
   }

   ~MqttClient_()
   {
      if (io)
         io->Funcs->close(io);
      free(clientIdFromServer);
   }

   void
   OnConnected(error *err)
   {
      SendConnect(!reconnect, err);
      reconnect = true;
      sentPing = false;
   }

   void
   SendConnect(bool clean, error *err)
   {
      MqttPacket pkt;
      HeapBufferList<3> heapBuffers;
      unsigned char requestResponseInfo = 1;

      memset(&pkt, 0, sizeof(pkt));

      auto copyString = [&] (const char *s, error *err) -> MqttString*
      {
         MqttString *r = MqttCopyString(s);
         if (!r)
            ERROR_SET(err, nomem);
         try
         {
            heapBuffers.Add(r);
         }
         catch (const std::bad_alloc &)
         {
            free(r);
            r = nullptr;
            ERROR_SET(err, nomem);
         }
      exit:
         return r;
      };

      pkt.Base.Type = CONNECT;
      auto &Connect = pkt.Info.Connect;

      Connect.ProtocolName = copyString("MQTT", err);
      ERROR_CHECK(err);

      Connect.Version = 5;
      Connect.KeepAlive = keepAlive;
      Connect.Prop_RequestResponseInfo = &requestResponseInfo;

      if (clean)
         Connect.Flags |= CleanStart;

      if (username)
      {
         Connect.UserName = copyString(username, err);
         ERROR_CHECK(err);
         Connect.Flags |= UserNameFlag;
      }

      if (password)
      {
         Connect.Password = copyString(password, err);
         ERROR_CHECK(err);
         Connect.Flags |= PasswordFlag;
      }

      if (clientid)
      {
         Connect.ClientId = copyString(clientid, err);
         ERROR_CHECK(err);
      }
      else
      {
         static MqttString empty = {0};
         Connect.ClientId = &empty;
      }

      WritePacket(
         &pkt,
         [] (void *ctx, MqttPacket *pkt, error *err) -> void
         {
            auto This = (MqttClient_*)ctx;

            This->sawConnAck = true;

            if (pkt->Info.ConnAck.Reason)
            {
               ERROR_SET(err, mqtt, (MqttReason)pkt->Info.ConnAck.Reason, pkt->Info.ConnAck.Prop_ReasonString);
            }

            This->retainSupported = (!pkt->Info.ConnAck.Prop_RetainAvailable ||
                                    *pkt->Info.ConnAck.Prop_RetainAvailable);

            This->subIdSupported = (!pkt->Info.ConnAck.Prop_SubIdAvail ||
                                   *pkt->Info.ConnAck.Prop_SubIdAvail);


            This->maximumQos = pkt->Info.ConnAck.Prop_MaximumQos ? MIN(2,*pkt->Info.ConnAck.Prop_MaximumQos) : 2;

            if (pkt->Info.ConnAck.Prop_AssignedClientId)
            {
               auto oldHeapValue = This->clientIdFromServer;
               This->clientIdFromServer = MqttStringToCString(pkt->Info.ConnAck.Prop_AssignedClientId);
               if (!This->clientIdFromServer)
               {
                  This->clientIdFromServer = oldHeapValue;
                  ERROR_SET(err, nomem);
               }
               This->clientid = This->clientIdFromServer;
               free(oldHeapValue);
            }

            {
               auto pair = This->subs.begin();
               for (; pair != This->subs.end();)
               {
                  auto subid = pair->first;
                  auto &sub = pair->second;

                  if (sub.written && (pkt->Info.ConnAck.Flags&SessionPresent))
                  {
                     ++pair;
                     continue;
                  }

                  if (sub.pendingDelete)
                  {
                     if ((pkt->Info.ConnAck.Flags&SessionPresent))
                     {
                        CbWrapper tmp = std::move(sub.unsubCb);

                        This->Unsubscribe(subid, tmp.OnResponse, tmp.OnResponseCtx, tmp.OnResponseFree, err);
                        tmp.Detach();
                        ERROR_CHECK(err);
                        ++pair;
                     }
                     else
                     {
                        sub.unsubCb(nullptr, err);
                        pair = This->subs.erase(pair);
                        ERROR_CHECK(err);
                     }
                     continue;
                  }

                  This->WriteSubscribe(subid, sub, err);
                  ERROR_CHECK(err);

                  ++pair;
               }
            }

            for (auto &state : This->pendingQos0Publishes)
            {
               This->WritePacket(&state->pkt, err);
               ERROR_CHECK(err);

               This->LinkCallbackToFlush(state->responseCb, err);
               ERROR_CHECK(err);
            }
            This->pendingQos0Publishes.resize(0);

            {
               auto pair = This->publishes.begin();
               while (pair != This->publishes.end())
               {
                  auto &state = pair->second;

                  if (This->WritePublish(state.get(), err))
                     pair = This->publishes.erase(pair);
                  else
                     ++pair;
                  ERROR_CHECK(err);
               }
            }
         exit:;
         },
         this,
         nullptr,
         err
      );
      ERROR_CHECK(err);
   exit:;
   }

   MqttPacketType
   CorrespondingPacketType(MqttPacket *pkt)
   {
      switch (pkt->Base.Type)
      {
      case CONNECT:
         return CONNACK;
      case SUBSCRIBE:
         return SUBACK;
      case UNSUBSCRIBE:
         return UNSUBACK;
      case PINGREQ:
         return PINGRESP;
      case PUBREC:
         return PUBREL;
      case PUBREL:
         return PUBCOMP;
      case PUBLISH:
         switch (MqttGetQosLevel(pkt->Base.Flags))
         {
         case 1:
            return PUBACK;
         case 2:
            return PUBREC;
         }
         // Fall through
      default:
         return (MqttPacketType)0;
      }
   }

   void
   OnIncomingPacket(MqttPacket *pkt, error *err)
   {
      auto itWithId = pendingReplies.find(pkt->Base.Type);
      auto itWithoutId = pendingRepliesWithoutId.find(pkt->Base.Type);
      bool hit = false;
      MqttPacket ackPkt;

      memset(&ackPkt, 0, sizeof(ackPkt));

      // Check if there's a pending response callback, i.e. acking
      // a packet.
      //
      if (itWithId != pendingReplies.end())
      {
         auto id = MqttGetPacketId(pkt);
         if (id)
         {
            auto &map = itWithId->second;
            auto it = map.find(*id);
            if (it != map.end())
            {
               it->second(pkt, err);
               map.erase(it);
               if (map.size() == 0)
                  pendingReplies.erase(itWithId);
               ERROR_CHECK(err);
            }
         }
      }

      if (itWithoutId != pendingRepliesWithoutId.end())
      {
         itWithoutId->second(pkt, err);
         pendingRepliesWithoutId.erase(itWithoutId);
         ERROR_CHECK(err);
      }

      // Seperately from that, process message..
      //
      switch (pkt->Base.Type)
      {
      case PUBLISH:
         // For QoS 2, we'll de-dupe.
         //
         if (MqttGetQosLevel(pkt->Base.Flags) == 2)
         {
            auto it = incomingPacketIds.find(pkt->Info.Publish.PacketId);
            if (it != incomingPacketIds.end())
            {
               if (!(pkt->Base.Flags & Dup))
                  ackPkt.Info.PubRec.Reason = PacketIdInUse;
               break;
            }
            try
            {
               incomingPacketIds.insert(pkt->Info.Publish.PacketId);
            }
            catch (const std::bad_alloc &)
            {
               ERROR_SET(err, nomem);
            }
         }

         if (!subIdSupported || !pkt->Info.Publish.Prop_SubscriptionId)
         {
            // Sub ID not supported, not present, or no match on sub ID.
            // Look for a matching topic.
            //
         matchPath:
            char *pktTopic = MqttStringToCString(pkt->Info.Publish.Topic);
            if (!pktTopic)
               ERROR_SET(err, nomem);
            for (auto &subPair : subs)
            {
               auto &sub = subPair.second;
               for (auto &topic : sub.Topics)
               {
                  if (MqttWildcardMatch(topic.c_str(), pktTopic))
                  {
                     sub.publishCb(pkt, err);
                     if (ERROR_FAILED(err))
                        goto localError;
                     break;
                  }
               }
            }
         localError:
            free(pktTopic);
            ERROR_CHECK(err);
            break; // don't fall through to subId path
         }

         // Look up all subscription IDs (there may be mutliple)
         //
         for (int i=0; i<pkt->Info.Publish.nProps; ++i)
         {
            auto prop = pkt->Info.Publish.Props + i;
            if (prop->Id == SubscriptionId)
            {
               auto subId = prop->Vbi;
               auto it = subs.find(subId);
               if (it != subs.end())
               {
                  auto &sub = it->second;
                  sub.publishCb(pkt, err);
                  ERROR_CHECK(err);
                  hit = true;
               }
            }
         }

         // If no match, try the string matching path.
         //
         if (!hit)
            goto matchPath;
         break;
      case PUBREL:
         {
            auto it = incomingPacketIds.find(pkt->Info.PubRel.PacketId);
            if (it == incomingPacketIds.end())
            {
               ackPkt.Info.PubComp.PacketId = PacketIdNotFound;
            }
            else
            {
               incomingPacketIds.erase(it);
            }
         }
         break;
      case PUBACK:
         {
            auto it = publishes.find(pkt->Info.PubAck.PacketId);
            if (it != publishes.end())
            {
               it->second->responseCb(pkt, err);
               publishes.erase(it);
               ERROR_CHECK(err);
            }
         }
         break;
      case PUBREC:
         {
            bool error = pkt->Info.PubRec.Reason >= 0x80;

            auto it = publishes.find(pkt->Info.PubRec.PacketId);
            if (it == publishes.end())
            {
               if (!error)
                  ackPkt.Info.PubRel.Reason = PacketIdNotFound;
            }
            else
            {
               if (error)
               {
                  it->second->responseCb(pkt, err);
                  publishes.erase(it);
                  ERROR_CHECK(err);
               }
               else
                  it->second->lastAck = PUBREC;
            }

            if (!error)
            {
               ackPkt.Base.Type = PUBREL;
               ackPkt.Info.PubRel.PacketId = pkt->Info.PubRec.PacketId;
            }
         }
         break;
      case PUBCOMP:
         {
            auto it = publishes.find(pkt->Info.PubComp.PacketId);
            if (it != publishes.end())
            {
               it->second->responseCb(pkt, err);
               publishes.erase(it);
               ERROR_CHECK(err);
            }
         }
         break;
      case DISCONNECT:
         ERROR_SET(err, mqtt, (MqttReason)pkt->Info.Disconnect.Reason, pkt->Info.Disconnect.Prop_ReasonString);
         break;
      }

      // Send some responses.
      //
      switch (pkt->Base.Type)
      {
      case PINGREQ:
         ackPkt.Base.Type = PINGRESP;
         break;
      case PUBLISH:
         switch (MqttGetQosLevel(pkt->Base.Flags))
         {
         case 1:
            ackPkt.Base.Type = PUBACK;
            ackPkt.Info.PubAck.PacketId = pkt->Info.Publish.PacketId;
            break;
         case 2:
            ackPkt.Base.Type = PUBREC;
            ackPkt.Info.PubRec.PacketId = pkt->Info.Publish.PacketId;
            break;
         }
         break;
      case PUBREL:
         ackPkt.Base.Type = PUBCOMP;
         ackPkt.Info.PubComp.PacketId = pkt->Info.PubRel.PacketId;
         break;
      }

      if (ackPkt.Base.Type)
      {
         WritePacket(&ackPkt, err);
         ERROR_CHECK(err);
      }
   exit:;
   }

   void
   OnBytesIn(void *buf, size_t len, error *err)
   {
      bool onHeap = false;
      MqttReader reader = {0};
      MqttPacket pkt;

      memset(&pkt, 0, sizeof(pkt));

      if (packetOnHeap.size())
      {
         try
         {
            packetOnHeap.insert(packetOnHeap.end(), (char*)buf, (char*)buf+len);
         }
         catch(const std::bad_alloc &)
         {
            ERROR_SET(err, nomem);
         }
         buf = packetOnHeap.data();
         len = packetOnHeap.size();
         onHeap = true;
      }

      reader.buf = (unsigned char*)buf;
      reader.remaining = len;

      while (reader.remaining)
      {
         free(pkt.HeapBuffer);
         memset(&pkt, 0, sizeof(pkt));

         auto status = MqttParsePacket(&reader, &pkt);
         switch (status)
         {
         case MqttParseStatus_Success:
            OnIncomingPacket(&pkt, err);
            ERROR_CHECK(err);
            break;
         case MqttParseStatus_IncompletePacket:
            goto loopEnd;
         case MqttParseStatus_ParseError:
            ERROR_SET(err, unknown, "mqtt parse error");
         case MqttParseStatus_Nomem:
            ERROR_SET(err, nomem);
         }
      }
   loopEnd:

      buf = reader.buf;
      len = reader.remaining;

      // Handle trailing data
      //
      if (onHeap)
      {
         // Move to front of buffer.
         //
         if (len && len != packetOnHeap.size())
            memmove(packetOnHeap.data(), buf, len);
         packetOnHeap.resize(len);
      }
      else if (len)
      {
         // Packet is from caller buffer.  Buffer the data.
         //
         try
         {
            packetOnHeap.insert(packetOnHeap.end(), (char*)buf, (char*)buf+len);
         }
         catch (const std::bad_alloc &)
         {
            ERROR_SET(err, nomem);
         }
      }

      if (keepAlive)
      {
         io->Funcs->set_timer(io, keepAlive, err);
         ERROR_CHECK(err);
      }
      sentPing = false;
   exit:
      free(pkt.HeapBuffer);
   }

   void
   WritePacket(MqttPacket *pkt, error *err)
   {
      WritePacket(pkt, nullptr, nullptr, nullptr, err);
   }

   void
   WritePacket(MqttPacket *pkt, CbWrapper &wrap, error *err)
   {
      WritePacket(pkt, wrap.OnResponse, wrap.OnResponseCtx, wrap.OnResponseFree, err);
      wrap.Detach();
   }

   void
   WritePacket(
      MqttPacket *pkt,
      void (*on_response)(void *, MqttPacket *, error *),
      void *on_response_ctx,
      void (*response_ctx_free)(void*),
      error *err
   )
   {
      MqttWriter writer = {0};
      CbWrapper callbacks;
      bool cb = false;

      if (on_response || response_ctx_free)
      {
         callbacks = std::move(CbWrapper(on_response, on_response_ctx, response_ctx_free));
         cb = true;
      }

      // Verify that we're not sending unsupported features over the socket.
      // We want to do this late because at the original call site, we may be
      // defering the packet write until later on, i.e. we need the bits parsed
      // by CONNACK to decide this.
      //
      switch (pkt->Base.Type)
      {
      case PUBLISH:
         {
            int qos;

            if (!retainSupported && (pkt->Base.Flags & Retain))
               pkt->Base.Flags = (MqttPacketFlags)(((int)pkt->Base.Flags) & ~Retain);
            qos = MqttGetQosLevel(pkt->Base.Flags);
            if (qos > maximumQos)
               MqttSetQosLevel(&pkt->Base.Flags, maximumQos);
         }
         break;
      }

      if (!MqttWritePacket(&writer, pkt))
         ERROR_SET(err, nomem);

      if (cb)
      {
         auto type = CorrespondingPacketType(pkt);
         auto id = MqttGetPacketId(pkt);

         try
         {
            if (id)
            {
               auto it = pendingReplies.find(type);
               if (it == pendingReplies.end())
               {
                  auto pair = std::make_pair(*id, std::move(callbacks));
                  std::unordered_map<uint16_t, CbWrapper> map;
                  map.insert(std::move(pair));
                  pendingReplies.insert(std::make_pair(type, std::move(map)));
               }
               else
               {
                  auto it2 = it->second.find(*id);
                  if (it2 != it->second.end())
                     it2->second = std::move(callbacks);
                  else
                     it->second.insert(std::make_pair(*id, std::move(callbacks)));
               }
            }
            else
            {
               auto it = pendingRepliesWithoutId.find(type);
               if (it != pendingRepliesWithoutId.end())
                  it->second = std::move(callbacks);
               else
                  pendingRepliesWithoutId.insert(std::make_pair(type, std::move(callbacks)));
            }
         }
         catch (const std::bad_alloc &)
         {
            ERROR_SET(err, nomem);
         }
      }

      Write(writer.buf, writer.len, err);
      ERROR_CHECK(err);
   exit:
      free(writer.buf);
   }

   void
   Write(const void *buf, size_t len, error *err)
   {
      if (!io)
         ERROR_SET(err, unknown, "i/o callbacks not set");
      io->Funcs->write(io, buf, len, nullptr, nullptr, err);
      ERROR_CHECK(err);

      if (keepAlive)
      {
         io->Funcs->set_timer(io, keepAlive, err);
         ERROR_CHECK(err);
      }
   exit:;
   }

   bool ShouldRetry(error *err)
   {
      switch (err->source)
      {
      case (int)ERROR_SRC_MQTT:
         switch ((MqttReason)err->code)
         {
         case BadUserOrPassword:
         case NotAuthorized:
         case BadAuthMethod:
         case Banned:
            return false;
         }
      }
      return true;
   }

   void OnCloseCommon(error *err, bool retry)
   {
      errorCb(err, &retry);
      if (!retry)
      {
         if (io)
         {
            io->Funcs->close(io);
            io = nullptr;
         }
         return;
      }

      sawConnAck = false;
      for (auto id : idsToClearAtDisconnect)
      {
         outgoingSubIds.Reclaim(id);
         ReclaimSubPacketIdPendingReplies(id);
      }
      idsToClearAtDisconnect.resize(0);
   }

   void
   OnTimerEllapsed(error *err)
   {
      if (!sentPing)
      {
         MqttPacket pkt;

         memset(&pkt, 0, sizeof(pkt));
         pkt.Base.Type = PINGREQ;

         WritePacket(&pkt, err);
         ERROR_CHECK(err);

         sentPing = true;
         return;
      }
      ERROR_SET(err, unknown, "Keep-alive timeout ellapsed");
   exit:;
   }

   void
   OnError(error *err)
   {
      OnCloseCommon(err, ShouldRetry(err));
   }

   void
   OnClose()
   {
      error err;
      OnCloseCommon(&err, true);
   }

   void
   WriteSubscribe(
      uint64_t subid,
      Sub &sub,
      error *err
   )
   {
      MqttPacket pkt;
      memset(&pkt, 0, sizeof(pkt));

      bool written = false;

      auto list = pkt.Info.Subscribe.List;
      list = nullptr;

      pkt.Base.Type = SUBSCRIBE;

      if (sub.packetId)
      {
         ReclaimSubPacketId(sub.packetId, true);
         sub.packetId = 0;
      }

      pkt.Info.Subscribe.PacketId = GetSubPacketId(err);
      ERROR_CHECK(err);

      *(void**)&list = calloc(sub.Topics.size(), sizeof(*list));
      if (!list)
         ERROR_SET(err, nomem);

      if (subIdSupported)
         pkt.Info.Subscribe.Prop_SubscriptionId = &subid;

      pkt.Info.Subscribe.ListSize = sub.Topics.size();
      pkt.Info.Subscribe.List = list;

      for (int i=0; i<sub.Topics.size(); ++i)
      {
         list[i].TopicFilter = MqttCopyString(sub.Topics[i].c_str());
         if (!list[i].TopicFilter)
            ERROR_SET(err, nomem);
         if (sub.Options.size() > i)
            list[i].SubOptions = sub.Options[i];
      }

      {
         auto subp = &sub;
         sub.packetId = pkt.Info.Subscribe.PacketId;
         MqttClient_WritePacket(
            this,
            &pkt,
            [this, subp] (MqttPacket *pkt, error *err) -> void
            {
               subp->written = true;
               ReclaimSubPacketId(subp->packetId, false);
               subp->packetId = 0;
               subp->responseCb(pkt, err);
            },
            err
         );
         ERROR_CHECK(err);
      }

      written = true;

   exit:
      if (list)
         for (int i=0; i<sub.Topics.size(); ++i)
            free(list[i].TopicFilter);
      free(list);
      if (pkt.Info.Subscribe.PacketId && !written)
      {
         ReclaimSubPacketId(pkt.Info.Subscribe.PacketId, true);
         sub.packetId = 0;
      }
   }

   uint16_t GetSubPacketId(error *err)
   {
      uint16_t id = 0;
      id = outgoingSubIds.GetId();
      if (!id)
         ERROR_SET(err, unknown, "Failed to get packet ID");
      try
      {
         idsToClearAtDisconnect.push_back(id);
      }
      catch (const std::bad_alloc &)
      {
         ERROR_SET(err, nomem);
      }
   exit:
      if (ERROR_FAILED(err) && id)
      {
         ReclaimSubPacketId(id, false);
         id = 0;
      }
      return id;
   }

   void ReclaimSubPacketId(uint16_t packetId, bool processPendingReplies=false)
   {
      auto &vec = idsToClearAtDisconnect;
      auto it = std::find(vec.begin(), vec.end(), packetId);
      if (it != vec.end())
         vec.erase(it);

      outgoingSubIds.Reclaim(packetId);

      if (processPendingReplies)
      {
         ReclaimSubPacketIdPendingReplies(packetId);
      }
   }

   void ReclaimSubPacketIdPendingReplies(uint16_t packetId)
   {
      for (auto type : { SUBACK, UNSUBACK })
      {
         auto it = pendingReplies.find(type);
         if (it != pendingReplies.end())
         {
            auto it2 = it->second.find(packetId);
            if (it2 != it->second.end())
            {
               it->second.erase(it2);
               if (!it->second.size())
                  pendingReplies.erase(it);
            }
         }
      }
   }

   void
   Subscribe(
      const char **topics,
      int nTopics,
      const unsigned char *subscriptionOptions,
      uint64_t *sub_id_out_opt,
      void (*on_publish)(void *, struct MqttPacket_ *, error *),
      void *publish_ctx,
      void (*publish_ctx_free)(void*),
      void (*on_response)(void *, struct MqttPacket_ *, error *err),
      void *on_response_ctx,
      void (*response_ctx_free)(void*),
      error *err
   )
   {
      CbWrapper pubCb(on_publish, publish_ctx, publish_ctx_free),
                respCb(on_response, on_response_ctx, response_ctx_free);

      uint64_t subid = subs.size()+1;
      while (subs.find(subid) != subs.end())
         ++subid;

      try
      {
         Sub sub;

         sub.publishCb = std::move(pubCb);
         sub.responseCb = std::move(respCb);

         for (int i=0; i<nTopics; ++i)
         {
            sub.Topics.push_back(std::string(topics[i]));
         }

         if (subscriptionOptions)
         {
            for (int i=0; i<nTopics; ++i)
               sub.Options.push_back(subscriptionOptions[i]);
         }

         subs[subid] = std::move(sub);
      }
      catch (const std::bad_alloc &)
      {
         ERROR_SET(err, nomem);
      }

      if (sawConnAck)
         WriteSubscribe(subid, subs[subid], err);
   exit:;
   }

   void
   Unsubscribe(
      uint64_t subid,
      void (*on_response)(void *, struct MqttPacket_ *, error *err),
      void *on_response_ctx,
      void (*response_ctx_free)(void*),
      error *err
   )
   {
      MqttPacket pkt;
      memset(&pkt, 0, sizeof(pkt));
      auto list = pkt.Info.Unsubscribe.TopicFilterList;
      list = nullptr;
      int ntopics = 0;
      uint16_t packetId = 0;

      CbWrapper cb(on_response, on_response_ctx, response_ctx_free);

      auto it = subs.find(subid);
      if (it == subs.end())
         ERROR_SET(err, unknown, "subid not found");

      it->second.pendingDelete = true;

      it->second.unsubCb = std::move(cb);

      if (!it->second.written)
      {
         it->second.unsubCb(nullptr, err);
         subs.erase(it);
         goto exit;
      }

      if (!sawConnAck)
      {
         goto exit;
      }

      ntopics = it->second.Topics.size();

      memset(&pkt, 0, sizeof(pkt));

      pkt.Base.Type = UNSUBSCRIBE;

      packetId = pkt.Info.Unsubscribe.PacketId = GetSubPacketId(err);
      ERROR_CHECK(err);

      *(void**)&list = calloc(ntopics, sizeof(*list));
      if (!list)
         ERROR_SET(err, nomem);

      pkt.Info.Unsubscribe.TopicFilterList = list;

      for (int i=0; i<ntopics; ++i)
      {
         list[i] = MqttCopyString(it->second.Topics[i].c_str());
         if (!list[i])
            ERROR_SET(err, nomem);
      }

      MqttClient_WritePacket(
         this,
         &pkt,
         [this, subid, packetId] (MqttPacket *pkt, error *err) -> void
         {
            ReclaimSubPacketId(packetId, false);
            auto it = subs.find(subid);
            if (it != subs.end())
            {
               it->second.unsubCb(pkt, err);
               subs.erase(it);
            }
         },
         err
      );
      packetId = 0;

   exit:
      for (int i=0; i<ntopics; ++i)
         free(list[i]);
      free(list);
      if (packetId)
         ReclaimSubPacketId(packetId, true);
   }

   void
   Publish(
      MqttPublishArgs *args,
      void (*on_response)(void *, struct MqttPacket_ *, error *err),
      void *on_response_ctx,
      void (*response_ctx_free)(void*),
      error *err
   )
   {
      std::shared_ptr<PublishState> onHeap;
      MqttPacket packetLocal;
      MqttPacket *pkt = &packetLocal;
      HeapBufferList<PublishState::NumStrings> localStrings, *strings = &localStrings;
      const void *payload = args->Payload;
      size_t payloadLength = args->Length;

      auto copyString = [&] (const char *str) -> MqttString*
      {
         auto p = MqttCopyString(str);

         if (!p)
            error_set_nomem(err);
         else
            strings->Add(p, err);

         if (ERROR_FAILED(err))
         {
            free(p);
            p = nullptr;
         }
         return p;
      };

      if (args->QosLevel || !sawConnAck)
      {
         common::New(onHeap, err);
         ERROR_CHECK(err);

         if (!sawConnAck)
            onHeap->lastAck = -1;

         pkt = &onHeap->pkt;
         onHeap->ids = &outgoingPubIds;

         strings = &onHeap->strings;

         onHeap->responseCb.OnResponse = on_response;
         onHeap->responseCb.OnResponseCtx = on_response_ctx;
         onHeap->responseCb.OnResponseFree = response_ctx_free;
         response_ctx_free = nullptr;

         if (payloadLength)
         {
            try
            {
               onHeap->payload.resize(payloadLength);
            }
            catch (const std::bad_alloc &)
            {
               ERROR_SET(err, nomem);
            }
            memcpy(onHeap->payload.data(), payload, payloadLength);
            payload = onHeap->payload.data();
         }
      }

      memset(pkt, 0, sizeof(*pkt));

      pkt->Base.Type = PUBLISH;
      MqttSetQosLevel(&pkt->Base.Flags, args->QosLevel);

      if (args->Retain)
         pkt->Base.Flags = (MqttPacketFlags)(((int)pkt->Base.Flags) | Retain);

      pkt->Info.Publish.Topic = copyString(args->Topic);
      ERROR_CHECK(err);

      if (args->ContentType)
      {
         pkt->Info.Publish.Prop_ContentType = copyString(args->ContentType);
         ERROR_CHECK(err);
      }

      if (args->Utf8)
      {
         static unsigned char utf8FormatIndicator = 1;

         pkt->Info.Publish.Prop_PayloadFormatIndicator = &utf8FormatIndicator;
      }

      pkt->AdditionalPayload = (const unsigned char*)payload;
      pkt->AdditionalPayloadLength = payloadLength;

      if (args->QosLevel)
      {
         if (!(pkt->Info.Publish.PacketId = onHeap->ids->GetId()))
            ERROR_SET(err, unknown, "Could not allocate packet ID");

         try
         {
            publishes[pkt->Info.Publish.PacketId] = onHeap;
         }
         catch (const std::bad_alloc &)
         {
            ERROR_SET(err, nomem);
         }
      }
      else if (onHeap.get())
      {
         try
         {
            pendingQos0Publishes.push_back(onHeap);
         }
         catch (const std::bad_alloc &)
         {
            ERROR_SET(err, nomem);
         }
      }

      if (sawConnAck)
      {
         WritePacket(pkt, err);
         ERROR_CHECK(err);

         CbWrapper cb(on_response, on_response_ctx, response_ctx_free);
         response_ctx_free = nullptr;

         LinkCallbackToFlush(cb, err);
         ERROR_CHECK(err);
      }
   exit:
      if (response_ctx_free)
         response_ctx_free(on_response_ctx);
   }

   void
   LinkCallbackToFlush(CbWrapper &cb, error *err)
   {
      CbWrapper *p = new (std::nothrow) CbWrapper(std::move(cb));
      if (!p)
         ERROR_SET(err, nomem);

      io->Funcs->write(
         io, nullptr, 0,
         [] (void *ctx, error *err)
         {
            auto p = (CbWrapper*)ctx;
            if (!ERROR_FAILED(err))
               (*p)(nullptr, err);
            delete p;
         },
         p,
         err
      );
      ERROR_CHECK(err);
      p = nullptr;

   exit:
      if (p)
         delete p;
   }

   bool
   WritePublish(PublishState *state, error *err)
   {
      // This is called when a QoS PUBLISH packet needs to retransmit
      // or send an ACK.  If we return true, the caller is expected to
      // remove the state from the global list and reclaim the ID.
      //

      switch (state->lastAck)
      {
      case 0:
         state->pkt.Base.Flags = (MqttPacketFlags)(((int)state->pkt.Base.Flags) | Dup);
      case -1: // !sawConnAck write, so no dup flag
         WritePacket(&state->pkt, err);
         ERROR_CHECK(err);
         break;
      case PUBREC:
         {
            MqttPacket pkt;
            memset(&pkt, 0, sizeof(pkt));
            pkt.Base.Type = PUBREL;
            pkt.Info.PubRel.PacketId = state->pkt.Info.Publish.PacketId;
            WritePacket(&pkt, err);
            ERROR_CHECK(err);
         }
         break;
      case PUBACK:
      case PUBCOMP:
         return true;
      }

   exit:
      return false;
   }
};

void
MqttClient_New(MqttClient *client, MqttClientArgs *args, error *err)
{
   MqttClient r = nullptr;
   size_t userOffset = 0, passOffset = 0, clidOffset = 0;

   if (!args)
      ERROR_SET(err, unknown, "args is null");
   if (!args->Io || !args->Io->Funcs)
      ERROR_SET(err, unknown, "i/o callbacks not set");

   r = new (std::nothrow) MqttClient_();
   if (!r)
      ERROR_SET(err, nomem);

   r->errorCb.OnError = args->OnError;
   r->errorCb.OnErrorContext = args->OnErrorContext;
   r->errorCb.OnErrorContextFree = args->OnErrorContextFree;
   args->OnErrorContextFree = nullptr;

   try
   {
      auto copyString = [&] (const char *s) -> size_t
      {
         size_t l = strlen(s)+1;
         size_t off = r->argsHeap.size();
         r->argsHeap.resize(off + l);
         memcpy(r->argsHeap.data() + off, s, l);
         return off;
      };

      if (args->Username)
         userOffset = copyString(args->Username);
      if (args->Password)
         passOffset = copyString(args->Password);
      if (args->ClientId && *args->ClientId)
         clidOffset = copyString(args->ClientId);
   }
   catch (const std::bad_alloc &)
   {
      ERROR_SET(err, nomem);
   }

   if (args->Username)
      r->username = r->argsHeap.data() + userOffset;
   if (args->Password)
      r->password = r->argsHeap.data() + passOffset;
   if (args->ClientId && *args->ClientId)
      r->clientid = r->argsHeap.data() + clidOffset;

   r->keepAlive = args->KeepAlive;

   r->io = args->Io;
   args->Io = nullptr;
   r->io->Funcs->set_client(r->io, r, err);
   ERROR_CHECK(err);
exit:
   if (ERROR_FAILED(err))
   {
      if (args && args->Io)
      {
         args->Io->Funcs->close(args->Io);
         args->Io = nullptr;
      }
      MqttClient_Release(r);
      r = nullptr;
   }
   if (args && args->OnErrorContextFree)
      args->OnErrorContextFree(args->OnErrorContext);
   *client = r;
}

void
MqttClient_Subscribe(
   MqttClient client,
   const char **topics,
   int nTopics,
   const unsigned char *subscriptionOptions,
   uint64_t *sub_id_out_opt,
   void (*on_publish)(void *, struct MqttPacket_ *, error *),
   void *publish_ctx,
   void (*publish_ctx_free)(void*),
   void (*on_response)(void *, struct MqttPacket_ *, error *err),
   void *on_response_ctx,
   void (*response_ctx_free)(void*),
   error *err
)
{
   client->Subscribe(
      topics, nTopics,
      subscriptionOptions,
      sub_id_out_opt,
      on_publish, publish_ctx, publish_ctx_free,
      on_response, on_response_ctx, response_ctx_free,
      err
   );
}

void
MqttClient_Unsubscribe(
   MqttClient client,
   uint64_t subid,
   void (*on_response)(void *, struct MqttPacket_ *, error *err),
   void *on_response_ctx,
   void (*response_ctx_free)(void*),
   error *err
)
{
   client->Unsubscribe(subid, on_response, on_response_ctx, response_ctx_free, err);
}

void
MqttClient_Publish(
   MqttClient client,
   MqttPublishArgs *args,
   void (*on_response)(void *, struct MqttPacket_ *, error *err),
   void *on_response_ctx,
   void (*response_ctx_free)(void*),
   error *err
)
{
   client->Publish(args, on_response, on_response_ctx, response_ctx_free, err);
}

void
MqttClient_Release(MqttClient client)
{
   if (client)
      delete client;
}

void
MqttClient_OnConnected(MqttClient client, error *err)
{
   client->OnConnected(err);
}

void
MqttClient_WritePacket(
   MqttClient cli,
   MqttPacket *packet,
   void (*on_response)(void *, MqttPacket *, error *),
   void *on_response_ctx,
   void (*response_ctx_free)(void*),
   error *err
)
{
   cli->WritePacket(packet, on_response, on_response_ctx, response_ctx_free, err);
}

void
MqttClient_OnBytesIn(MqttClient cli, void *buf, size_t len, error *err)
{
   cli->OnBytesIn(buf, len, err);
}

void
MqttClient_OnTimerEllapsed(MqttClient cli, error *err)
{
   cli->OnTimerEllapsed(err);
}

void
MqttClient_OnError(MqttClient cli, error *err)
{
   cli->OnError(err);
}

void
MqttClient_OnClose(MqttClient cli)
{
   cli->OnClose();
}
