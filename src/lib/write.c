/*
 Copyright (C) 2023 Andrew Sveikauskas

 Permission to use, copy, modify, and distribute this software for any
 purpose with or without fee is hereby granted, provided that the above
 copyright notice and this permission notice appear in all copies.
*/

#include <mqtt/writer.h>
#include <mqtt/messages.h>

#include <limits.h>
#include <stdlib.h>
#include <string.h>

#define VBI_BUFFER_SIZE 8

bool
MqttWriteRawBytes(MqttWriter *writer, const void *buf, size_t n)
{
   size_t alloc = 0;

   if (!n)
      return true;

   if (SIZE_MAX - writer->len < n)
      return false;

   alloc = writer->capacity;
   if (!alloc)
      alloc = 32;
   while (alloc - writer->len < n)
   {
      if (SIZE_MAX/2 >= alloc)
         alloc *= 2;
      else
         alloc = writer->len + n;
   }
   if (alloc != writer->capacity)
   {
      void *p = NULL;
      if (writer->buf)
         p = realloc(writer->buf, alloc);
      else
         p = malloc(alloc);
      if (!p)
         return false;
      writer->buf = p;
      writer->capacity = alloc;
   }

   memcpy(writer->buf + writer->len, buf, n);
   writer->len += n;

   return true;
}

bool
MqttBeginPropertyList(MqttWriter *writer, MqttPropertyWriter *out)
{
   out->parent = writer;
   out->startingOffset = writer->len;
   return true;
}

bool
MqttEndPropertyList(MqttPropertyWriter *writer)
{
   unsigned char vbi[VBI_BUFFER_SIZE];
   size_t vbiLen = 0;
   size_t packetLen = writer->parent->len - writer->startingOffset;

   if (!MqttEncodeVbi(vbi, sizeof(vbi), packetLen, &vbiLen) ||
       !MqttWriteRawBytes(writer->parent, vbi, vbiLen))
      return false;

   memmove(writer->parent->buf + writer->startingOffset + vbiLen,
           writer->parent->buf + writer->startingOffset,
           packetLen);
   memcpy(writer->parent->buf + writer->startingOffset, vbi, vbiLen);

   return true;
}

bool
MqttWriteProperty(MqttPropertyWriter *writer, MqttProperty *prop)
{
   if (!MqttWriteVbi(writer->parent, prop->Id))
      return false;
   switch (prop->Type)
   {
   case String:
      return MqttWriteString(writer->parent, prop->String);
   case StringPair:
      return MqttWriteString(writer->parent, prop->StringPair.First) &&
             MqttWriteString(writer->parent, prop->StringPair.Second);
   case Blob:
      return MqttWriteBlob(writer->parent, prop->Blob);
   case U32:
      return MqttWriteU32(writer->parent, prop->I32);
   case U16:
      return MqttWriteU16(writer->parent, prop->I16);
   case Byte:
      return MqttWriteByte(writer->parent, prop->Byte);
   case Vbi:
      return MqttWriteVbi(writer->parent, prop->Vbi);
   }
   return false;
}

bool
MqttBeginPacket(MqttWriter *writer, MqttPacketType type, MqttPacketFlags flags, MqttPacketWriterState *stateOut)
{
   unsigned char typeAndFlags;

   MQTT_SET_TYPE_AND_FLAGS(typeAndFlags, type, flags);

   if (!MqttWriteByte(writer, typeAndFlags))
      return false;

   if (!MqttBeginPropertyList(writer, stateOut))
      return false;

   return true;
}

bool
MqttEndPacket(MqttWriter *writer, MqttPacketWriterState *state)
{
   return MqttEndPropertyList(state);
}

bool
MqttWriteByte(MqttWriter *writer, unsigned char value)
{
   return MqttWriteRawBytes(writer, &value, 1);
}

bool
MqttWriteU16(MqttWriter *writer, uint16_t value)
{
   unsigned char bytes[] =
   {
      value >> 8,
      value & 0xff
   };
   return MqttWriteRawBytes(writer, bytes, sizeof(bytes));
}

bool
MqttWriteU32(MqttWriter *writer, uint32_t value)
{
   unsigned char bytes[] =
   {
      value >> 24,
      value >> 16,
      value >> 8,
      value & 0xff
   };
   return MqttWriteRawBytes(writer, bytes, sizeof(bytes));
}

bool
MqttWriteString(MqttWriter *writer, MqttString *str)
{
   return MqttWriteRawBytes(writer, str, 2 + MqttStringGetLength(str));
}

#define MqttWriteBlob MqttWriteString

bool
MqttWriteVbi(MqttWriter *writer, uint64_t value)
{
   unsigned char bytes[VBI_BUFFER_SIZE];
   size_t len = 0;

   return MqttEncodeVbi(bytes, sizeof(bytes), value, &len) &&
          MqttWriteRawBytes(writer, bytes, len);
}

#define WRITER(X) (X)
#define READER(X)

#include "generated/messages-writer.c"

bool
MqttWritePacket(MqttWriter *writer, MqttPacket *in)
{
   MqttPacketWriterState state;
   int extraFlags = 0;

   switch (in->Base.Type)
   {
   case SUBSCRIBE:
   case UNSUBSCRIBE:
   case PUBREL:
      extraFlags = 2;
   default:;
   }

   return MqttBeginPacket(writer, in->Base.Type, in->Base.Flags | extraFlags, &state) &&
          MqttWritePacketDispatch(writer, &in->Base, &in->Info) &&
          MqttWriteRawBytes(writer, in->AdditionalPayload, in->AdditionalPayloadLength) &&
          MqttEndPacket(writer, &state);
}
