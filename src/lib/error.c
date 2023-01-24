/*
 Copyright (C) 2026 Andrew Sveikauskas

 Permission to use, copy, modify, and distribute this software for any
 purpose with or without fee is hereby granted, provided that the above
 copyright notice and this permission notice appear in all copies.
*/

#include <mqtt/error.h>
#include <common/size.h>

#include <stdlib.h>

static const char *
error_mqtt_get_string(error *err)
{
   return err->context ? err->context : "<Unknown error>";
}

static const char *
static_reason_string(MqttReason reason)
{
   switch (reason)
   {
#define MAP(CODE, STRING)   case CODE: return STRING; break

   MAP(DisconnectWithWill,          "Disconnect with Will Message");
   MAP(NoSubscribers,               "No matching subscribers");
   MAP(NoSuchSubscription,          "No subscription existed");
   MAP(ContinueAuth,                "Continue authentication");
   MAP(ReAuth,                      "Re-authenticate");

   MAP(GenericError,                "Unspecified error");
   MAP(MalformedPacket,             "Malformed packet");
   MAP(ProtocolError,               "Protocol error");
   MAP(ImplementationSpecificError, "Implementation specific error");
   MAP(UnsupportedProtocol,         "Unsupported protocol");
   MAP(BadClientId,                 "Bad client ID");
   MAP(BadUserOrPassword,           "Bad user or password");
   MAP(NotAuthorized,               "Not authorized");
   MAP(ServerUnavailable,           "Server unavailable");
   MAP(ServerBusy,                  "Server busy");
   MAP(Banned,                      "Banned");
   MAP(ServerShuttingDown,          "Server shutting down");
   MAP(BadAuthMethod,               "Bad authentication method");
   MAP(KeepAliveTimeout,            "Keep alive timeout");
   MAP(SessionTakenOver,            "Session taken over");
   MAP(TopicFilterInvalid,          "Topic filter invalid");
   MAP(TopicNameInvalid,            "Topic name invalid");
   MAP(PacketIdInUse,               "Packet ID in use");
   MAP(PacketIdNotFound,            "Packet ID not found");
   MAP(ReceiveMaximumExceeded,      "Receive maximum exceeded");
   MAP(TopicAliasInvalid,           "Topic alias invalid");
   MAP(PacketTooLarge,              "Packet too large");
   MAP(MessageRateTooHigh,          "Message rate too high");
   MAP(QuotaExceeded,               "Quota exceeded");
   MAP(AdministrativeAction,        "Administrative action");
   MAP(PayloadFormatInvalid,        "Payload format invalid");
   MAP(RetainNotSupported,          "Retain not supported");
   MAP(QosNotSupported,             "QoS not supported");
   MAP(UseAnotherServer,            "Use another server");
   MAP(ServerMoved,                 "Server moved");
   MAP(SharedSubNotSupported,       "Shared subscriptions not supported");
   MAP(ConnRateExceeded,            "Connection rate exceeded");
   MAP(SubIdNotSupported,           "Subscription IDs not supported");
   MAP(WildcardSubNotSupported,     "Wildcard subscriptions not supported");
#undef MAP
   }

   return NULL;
}

void
error_set_mqtt(error *err, MqttReason reason, MqttString *str)
{
   const char *desc = NULL;
   char *buf = NULL;
   size_t l = 0;

   size_t ls[3];
   const char *ss[3];
   int idx = 0;
   int i;

   error_clear(err);

   err->source = ERROR_SRC_MQTT;
   err->code = reason;
   err->get_string = error_mqtt_get_string;

   desc = static_reason_string(reason);

   if (desc && !str)
   {
      err->context = (char*)desc;
      return;
   }

   if (desc)
   {
      ss[idx] = desc;
      ls[idx] = strlen(desc);
      ++idx;
   }

   if (str)
   {
      if (idx)
      {
         ss[idx] = ": ";
         ls[idx] = 2;
         ++idx;
      }
      ss[idx] = str->Payload;
      ls[idx] = MqttStringGetLength(str);
      ++idx;
   }

   if (!idx)
      return;

   for (i=0; i<idx; ++i)
   {
      if (size_add(l, ls[i], &l))
         return;
   }

   if (size_add(l, 1, &l))
      return;

   buf = malloc(l);
   if (!buf)
      return;
   err->context = buf;
   err->free_fn = free;

   for (i=0; i<idx; ++i)
   {
      memcpy(buf, ss[i], ls[i]);
      buf += ls[i];
   }
   *buf = 0;
}
