/*
 Copyright (C) 2026 Andrew Sveikauskas

 Permission to use, copy, modify, and distribute this software for any
 purpose with or without fee is hereby granted, provided that the above
 copyright notice and this permission notice appear in all copies.
*/

#ifndef mqtt_error_h_
#define mqtt_error_h_ 1

#include <mqtt/proto.h>
#include <common/error.h>

#define ERROR_SRC_MQTT \
   (('m' | (((unsigned)'q') << 8) | (((unsigned)'q') << 16) | ((unsigned)'t' << 24)))

#if defined(__cplusplus)
extern "C" {
#endif

void
error_set_mqtt(error *err, MqttReason reason, MqttString *reasonString);

#if defined(__cplusplus)
}
#endif
#endif
