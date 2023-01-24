/*
 Copyright (C) 2023 Andrew Sveikauskas

 Permission to use, copy, modify, and distribute this software for any
 purpose with or without fee is hereby granted, provided that the above
 copyright notice and this permission notice appear in all copies.
*/

#include <mqtt/reader.h>
#include <mqtt/messages.h>

#include "pool.h"

#include <string.h>
#include <limits.h>

bool
MqttReadPropertyList(MqttReader *r, MqttReader *listOut)
{
   MqttReader bak = *r;
   bool ret = false;
   uint64_t len = 0;

   if (!(ret = MqttReadVbi(r, &len)))
      goto exit;

   listOut->buf = r->buf;
   listOut->remaining = (size_t)len;
   if (len > (uint64_t)listOut->remaining || len > r->remaining)
   {
      ret = false;
      goto exit;
   }
   ret = MqttReadSkip(r, listOut->remaining);
exit:
   if (!ret)
      *r = bak;
   return ret;
}

bool
MqttReadProperty(
   MqttReader *list,
   uint64_t *propId,
   MqttPropertyType *typeOut,
   size_t *size_opt
)
{
   if (!MqttReadVbi(list, propId))
      return false;

   switch (*propId)
   {
   case PayloadFormatIndicator:
   case RequestProblemInfo:
   case RequestResponseInfo:
   case MaximumQos:
   case RetainAvailable:
   case WildcardSubAvail:
   case SubIdAvail:
   case SharedSubAvail:
      *typeOut = Byte;
      if (size_opt)
         *size_opt = 1;
      break;
   case ServerKeepAlive:
   case ReceiveMaximum:
   case TopicAliasMaximum:
   case TopicAlias:
      *typeOut = U16;
      if (size_opt)
         *size_opt = 2;
      break;
   case MessageExpiryInterval:
   case SessionExpiryInterval:
   case WillDelayInterval:
   case MaximumPacketSize:
      *typeOut = U32;
      if (size_opt)
         *size_opt = 4;
      break;
   case ContentType:
   case ResponseTopic:
   case AssignedClientId:
   case AuthMethod:
   case ResponseInfo:
   case ServerReference:
   case ReasonString:
      *typeOut = String;
   stringSizeCheck:
      if (size_opt)
      {
         MqttReader nest = *list;
         MqttString *dummy;
         if (!MqttReadString(&nest, &dummy))
            return false;
         *size_opt = nest.buf - list->buf;
      }
      break;
   case CorrelationData:
   case AuthData:
      *typeOut = Blob;
      goto stringSizeCheck;
   case SubscriptionId:
      *typeOut = Vbi;
      if (size_opt)
      {
         MqttReader nest = *list;
         uint64_t dummy;
         if (!MqttReadVbi(&nest, &dummy))
            return false;
         *size_opt = nest.buf - list->buf;
      }
      break;
   case UserProp:
      *typeOut = StringPair;
      if (size_opt)
      {
         MqttReader nest = *list;
         MqttString *dummy;
         if (!MqttReadString(&nest, &dummy))
            return false;
         if (!MqttReadString(&nest, &dummy))
            return false;
         *size_opt = nest.buf - list->buf;
      }
   default:
      return false;
   }

   return true;
}

bool
MqttReadByte(MqttReader *r, unsigned char *out)
{
   if (!r->remaining)
      return false;
   *out = *(r->buf++);
   --(r->remaining);
   return true;
}

bool
MqttReadU16(MqttReader *r, uint16_t *out)
{
   if (r->remaining < 2)
      return false;

   *out = *r->buf++;
   (*out) <<= 8;
   *out |= *r->buf++;
   r->remaining -= 2;
   return true;
}

bool
MqttReadU32(MqttReader *r, uint32_t *out)
{
   if (r->remaining < 4)
      return false;
   *out = *r->buf++;
   (*out) <<= 8;
   *out |= *r->buf++;
   (*out) <<= 8;
   *out |= *r->buf++;
   (*out) <<= 8;
   *out |= *r->buf++;
   r->remaining -= 4;
   return true;
}

bool
MqttReadString(MqttReader *r, MqttString **out)
{
   MqttString *str = NULL;
   uint16_t len = 0;

   if (r->remaining < 2)
      return false;

   str = (MqttString*)r->buf;
   len = MqttStringGetLength(str);

   if (r->remaining - 2 < len)
      return false;

   *out = str;
   r->remaining -= (2 + len);
   r->buf += (2 + len);
   return true;
}

bool
MqttReadVbi(MqttReader *r, uint64_t *out)
{
   size_t outlen = 0;
   if (!MqttParseVbi(r->buf, r->remaining, out, &outlen))
      return false;
   r->buf += outlen;
   r->remaining -= outlen;
   return true;
}

bool
MqttReadSkip(MqttReader *r, size_t skip)
{
   if (r->remaining < skip)
      return false;
   r->buf += skip;
   r->remaining -= skip;
   return true;
}

static bool
MqttParseProperty(MqttReader *reader, MqttPropertyId id, MqttPropertyType type, MqttProperty *prop)
{
   prop->Id = id;
   prop->Type = type;

   switch (prop->Type)
   {
   case Blob:
   case String:
      if (!MqttReadString(reader, &prop->String))
         return false;
      break;
   case StringPair:
      if (!MqttReadString(reader, &prop->StringPair.First))
         return false;
      if (!MqttReadString(reader, &prop->StringPair.Second))
         return false;
      break;
   case U32:
      if (!MqttReadU32(reader, &prop->I32))
         return false;
      break;
   case U16:
      if (!MqttReadU16(reader, &prop->I16))
         return false;
      break;
   case Byte:
      if (!MqttReadByte(reader, &prop->Byte))
         return false;
      break;
   case Vbi:
      if (!MqttReadVbi(reader, &prop->Vbi))
         return false;
      break;
   default:
      return false;
   }
   return true;
}

struct MqttParserState_
{
   MqttParseStatus status;
   struct pool pool;
};

#define WRITER(X)
#define READER(X) (X)

#include "generated/messages-parser.c"

bool
MqttParseFixedHeader(MqttReader *r, bool *foundPacket, MqttFixedHeader *out, MqttReader *rOut)
{
   MqttReader bak = *r;
   bool ret = false;
   bool completePacket = false;
   unsigned char typeAndFlags = 0;

   if (!(ret = MqttReadByte(r, &typeAndFlags)))
      goto exit;
   if (!(ret = MqttReadVbi(r, &out->RemainingLength)))
      goto exit;
   if (out->RemainingLength > r->remaining)
   {
      ret = false;
      goto exit;
   }
   out->Type = MQTT_GET_TYPE(typeAndFlags);
   out->Flags = MQTT_GET_FLAGS(typeAndFlags);
   completePacket = true;
   rOut->buf = r->buf;
   rOut->remaining = (size_t)out->RemainingLength;
   if (out->RemainingLength > (uint64_t)rOut->remaining)
   {
      ret = false;
      goto exit;
   }
   if (!(ret = MqttReadSkip(r, rOut->remaining)))
      goto exit;

   ret = true;
exit:
   if (!ret)
   {
      if (!completePacket)
      {
         memset(out, 0, sizeof(*out));
         memset(rOut, 0, sizeof(*rOut));
      }
      *r = bak;
   }
   *foundPacket = completePacket;
   return ret;
}

MqttParseStatus
MqttParsePacket(
   MqttReader *reader,
   MqttPacket *out
)
{
   MqttReader payloadReader = {0};
   MqttParserState state = {0};
   bool foundPacket = false;

   memset(out, 0, sizeof(*out));
   state.status = MqttParseStatus_ParseError;

   if (!MqttParseFixedHeader(reader, &foundPacket, &out->Base, &payloadReader))
   {
      if (!foundPacket)
         state.status = MqttParseStatus_IncompletePacket;
      goto exit;
   }

   if (!MqttParsePacketDispatch(&payloadReader, &state, &out->Base, &out->Info))
      goto exit;

   out->HeapBuffer = pool_finalize(&state.pool);
   out->AdditionalPayloadLength = payloadReader.remaining;
   if (out->AdditionalPayloadLength)
      out->AdditionalPayload = payloadReader.buf;
   state.status = MqttParseStatus_Success;
exit:
   pool_discard(&state.pool);
   return state.status;
}
