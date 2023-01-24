/*
 Copyright (C) 2023 Andrew Sveikauskas

 Permission to use, copy, modify, and distribute this software for any
 purpose with or without fee is hereby granted, provided that the above
 copyright notice and this permission notice appear in all copies.
*/

#ifndef mqttreader_h_
#define mqttreader_h_

#include "proto.h"

#if defined(__cplusplus)
extern "C" {
#endif

typedef struct MqttReader_
{
   unsigned char *buf;
   size_t remaining;
} MqttReader;

bool
MqttReadByte(MqttReader *r, unsigned char *out);

bool
MqttReadU16(MqttReader *r, uint16_t *out);

bool
MqttReadU32(MqttReader *r, uint32_t *out);

bool
MqttReadString(MqttReader *r, MqttString **out);

#define MqttReadBlob MqttReadString

bool
MqttReadVbi(MqttReader *r, uint64_t *out);

bool
MqttReadPropertyList(MqttReader *r, MqttReader *listOut);

bool
MqttReadProperty(
   MqttReader *listReader,
   uint64_t *propId,
   MqttPropertyType *typeOut,
   size_t *size_opt
);

bool
MqttReadSkip(MqttReader *r, size_t skip);

#if defined(__cplusplus)
}
#endif
#endif
