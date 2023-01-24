/*
 Copyright (C) 2023 Andrew Sveikauskas

 Permission to use, copy, modify, and distribute this software for any
 purpose with or without fee is hereby granted, provided that the above
 copyright notice and this permission notice appear in all copies.
*/

#include <mqtt/util.h>

#include <stdlib.h>
#include <string.h>

MqttBlob *
MqttAllocateBlob(size_t capacity)
{
   MqttBlob *blob = NULL;
   if (capacity <= 0xffff)
      blob = malloc(2 + capacity);
   if (blob)
      MqttBlobSetLength(blob, (uint16_t)capacity);
   return blob;
}

MqttString *
MqttCopyString(const char *str)
{
   MqttString *r = NULL;
   size_t len = 0;
   if (str)
      len = strlen(str);
   r = MqttAllocateBlob(len);
   if (r)
      memcpy(r->Payload, str, len);
   return r;
}

char *
MqttStringToCString(MqttString *str)
{
   char *r = NULL;
   size_t l = 0;

   if (str)
   {
      size_t i;

      l = MqttStringGetLength(str);
      for (i=0; i<l; ++i)
         if (!str->Payload[i])
            goto exit;

      r = malloc(l + 1);
   }
   if (r)
   {
      memcpy(r, str->Payload, l);
      r[l] = 0;
   }
exit:
   return r;
}
