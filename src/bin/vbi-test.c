/*
 Copyright (C) 2026 Andrew Sveikauskas

 Permission to use, copy, modify, and distribute this software for any
 purpose with or without fee is hereby granted, provided that the above
 copyright notice and this permission notice appear in all copies.
*/

#include <mqtt/proto.h>

#include <common/arraysize.h>
#include <common/size.h>

#include <stdlib.h>
#include <stdio.h>

#include <inttypes.h>

int main(void)
{
   static const uint64_t test_integers[] =
   {
      1, 37,
      1024*1024,
      ~0ULL/0x80
   };

   int fails = 0;
   int total = 0;

   for (int i=0; i<ARRAY_SIZE(test_integers); ++i, ++total)
   {
      unsigned char buf[16];
      size_t len;
      uint64_t decoded;
      size_t len_decoded;

      if (!MqttEncodeVbi(buf, sizeof(buf), test_integers[i], &len))
      {
         fprintf(stderr, "FAILED to encode %" PRIu64 "\n", test_integers[i]);
      }
      else if (!MqttParseVbi(buf, len, &decoded, &len_decoded))
      {
         fprintf(stderr, "FAILED to decode %" PRIu64 "\n", test_integers[i]);
      }
      else if (len != len_decoded)
      {
         fprintf(stderr, "FAILED %" PRIu64 ", encoded at length %" SIZE_T_FMT_d ", got length %" SIZE_T_FMT_d "\n", test_integers[i], len, len_decoded);
      }
      else if (test_integers[i] != decoded)
      {
         fprintf(stderr, "FAILED, expected %" PRIu64 ", got %" PRIu64 "\n", test_integers[i], decoded);
      }
      else
      {
         continue;
      }
      ++fails;
   }

   printf("%d of %d tests failed\n", fails, total);
   return fails ? 1 : 0;
}
