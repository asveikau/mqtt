/*
 Copyright (C) 2023 Andrew Sveikauskas

 Permission to use, copy, modify, and distribute this software for any
 purpose with or without fee is hereby granted, provided that the above
 copyright notice and this permission notice appear in all copies.
*/

#ifndef mqtt_messages_h_
#define mqtt_messages_h_

#include "proto.h"

#if defined(__cplusplus)
extern "C" {
#endif

struct MqttReader_;
struct MqttParserState_;
typedef struct MqttParserState_ MqttParserState;

struct MqttWriter_;

typedef enum
{
   MqttParseStatus_Success = 0,
   MqttParseStatus_IncompletePacket,
   MqttParseStatus_ParseError,
   MqttParseStatus_Nomem = -100,
} MqttParseStatus;

typedef struct
{
   MqttPacketType Type;
   MqttPacketFlags Flags;
   uint64_t RemainingLength;
} MqttFixedHeader;

#include "generated/messages.h"

typedef struct MqttPacket_
{
   MqttFixedHeader Base;
   MqttPacketStorage Info;

   // Potentially filled in by parser.  Caller's responsibility to call
   // free() upon it.
   //
   void *HeapBuffer;

   // Reader: filled in by parser, allocated from packet input
   // Writer: initialized by caller to be written as part of packet
   //
   const unsigned char *AdditionalPayload;
   size_t AdditionalPayloadLength;
} MqttPacket;

bool
MqttParseFixedHeader(
   struct MqttReader_* reader,
   bool *foundPacket,
   MqttFixedHeader *out,
   struct MqttReader_* packetReaderOut
);

MqttParseStatus
MqttParsePacket(
   struct MqttReader_* reader,
   MqttPacket *out
);

bool
MqttWritePacket(struct MqttWriter_ *writer, MqttPacket *in);

static inline
uint16_t *
MqttGetPacketId(MqttPacket *packet)
{
   switch (packet->Base.Type)
   {
   case PUBLISH:
      return (packet->Base.Flags & (QosBit0|QosBit1)) ? &packet->Info.Publish.PacketId : NULL;
   case PUBACK:
      return &packet->Info.PubAck.PacketId;
   case PUBREC:
      return &packet->Info.PubRec.PacketId;
   case PUBREL:
      return &packet->Info.PubRel.PacketId;
   case PUBCOMP:
      return &packet->Info.PubComp.PacketId;
   case SUBSCRIBE:
      return &packet->Info.Subscribe.PacketId;
   case SUBACK:
      return &packet->Info.SubAck.PacketId;
   case UNSUBSCRIBE:
      return &packet->Info.Unsubscribe.PacketId;
   case UNSUBACK:
      return &packet->Info.UnsubAck.PacketId;
   default:
      break;
   }
   return NULL;
}

#if defined(__cplusplus)
}
#endif
#endif
