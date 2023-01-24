/*
 Copyright (C) 2023 Andrew Sveikauskas

 Permission to use, copy, modify, and distribute this software for any
 purpose with or without fee is hereby granted, provided that the above
 copyright notice and this permission notice appear in all copies.
*/

#include "proto.h"

#ifndef mqttwriter_h_
#define mqttwriter_h_

#if defined(__cplusplus)
extern "C"{
#endif

typedef struct MqttWriter_
{
   unsigned char *buf;
   size_t len;
   size_t capacity;
} MqttWriter;

typedef struct
{
   MqttWriter *parent;
   size_t startingOffset;
} MqttPropertyWriter, MqttPacketWriterState;

bool
MqttWriteRawBytes(MqttWriter *writer, const void *buf, size_t n);

bool
MqttWriteByte(MqttWriter *writer, unsigned char);

bool
MqttWriteU16(MqttWriter *writer, uint16_t);

bool
MqttWriteU32(MqttWriter *writer, uint32_t);

bool
MqttWriteString(MqttWriter *writer, MqttString *str);

#define MqttWriteBlob MqttWriteString

bool
MqttWriteVbi(MqttWriter *writer, uint64_t);

bool
MqttBeginPropertyList(MqttWriter *writer, MqttPropertyWriter *out);

bool
MqttWriteProperty(MqttPropertyWriter *writer, MqttProperty *prop);

bool
MqttEndPropertyList(MqttPropertyWriter *writer);

bool
MqttBeginPacket(MqttWriter *writer, MqttPacketType type, MqttPacketFlags flags, MqttPacketWriterState *stateOut);

bool
MqttEndPacket(MqttWriter *writer, MqttPacketWriterState *state);

#if defined(__cplusplus)
}
#endif
#endif
