/*
 Copyright (C) 2026 Andrew Sveikauskas

 Permission to use, copy, modify, and distribute this software for any
 purpose with or without fee is hereby granted, provided that the above
 copyright notice and this permission notice appear in all copies.
*/

#include <mqtt/util.h>
#include <string.h>

static const char *
FindSlashOrEnd(const char *base)
{
   const char *slash = strchr(base, '/');
   return slash ? slash : base + strlen(base);
}

bool
MqttWildcardMatch(const char *filter, const char *topic)
{
   if (strchr("#+", *filter) && *topic == '$')
      return false;

   while (*filter)
   {
      const char *nextSlash = FindSlashOrEnd(filter);
      int n = nextSlash - filter;

      const char *topicSlash = FindSlashOrEnd(topic);
      int topicN = topicSlash - topic;

      if (n == 1 && *filter == '#')
      {
         // The wildcard needs to be the last element.
         //
         if (filter[1])
            return false;

         // If we made it this far, no need to compare.
         //
         return true;
      }

      // Filter has path element, topic does not?
      //
      if (!topicN)
         return false;

      // Compare current path element.
      //
      if (!(n == 1 && *filter == '+') && // wildcard?
          !(n == topicN && !strncmp(filter, topic, n)))
      {
         return false;
      }

      if ((*nextSlash != *topicSlash) &&
          !(*nextSlash && nextSlash[1] == '#'))
         return false;
 
     filter = nextSlash;
      if (*filter)
        ++filter;
      topic = topicSlash;
      if (*topic)
         ++topic;
   }

   if (*topic)
      return false;

   return true;
}
