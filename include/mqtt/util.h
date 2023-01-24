/*
 Copyright (C) 2023 Andrew Sveikauskas

 Permission to use, copy, modify, and distribute this software for any
 purpose with or without fee is hereby granted, provided that the above
 copyright notice and this permission notice appear in all copies.
*/

#ifndef mqttutil_h_
#define mqttutil_h_

#include "proto.h"
#include <stdbool.h>

#if defined(__cplusplus)
extern "C" {
#endif

MqttBlob *
MqttAllocateBlob(size_t capacity);

MqttString *
MqttCopyString(const char *str);

char *
MqttStringToCString(MqttString *str);

bool
MqttWildcardMatch(const char *filter, const char *topic);

#if defined(__cplusplus)
}

#include <string>

static inline
std::string
MqttStringToString(MqttString *str)
{
   if (!str) return std::string();
   return std::string(str->Payload, MqttStringGetLength(str));
}
#endif
#endif
