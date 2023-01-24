/*
 Copyright (C) 2026 Andrew Sveikauskas

 Permission to use, copy, modify, and distribute this software for any
 purpose with or without fee is hereby granted, provided that the above
 copyright notice and this permission notice appear in all copies.
*/

#include <mqtt/util.h>

#include <stdbool.h>
#include <stdio.h>

const char *b2s(bool b)
{
   return b ? "true" : "false";
}

int main(void)
{
   struct test_case
   {
      const char *filter;
      const char *topic;
      bool match;
   };
   static const struct test_case cases[] =
   {
      {"",              "foo",               false},
      {"",              "$foo",              false},
      {"foo/bar",       "foo/bar",           true},
      {"a//b",          "a/b",               false},
      {"a/",            "a",                 false},
      {"a/#",           "a",                 true},
      {"#",             "goose",             true},
      {"a",             "a/b",               false},
      {"a",             "a/",                false},
      {"#",             "$SYS/foo",          false},
      {"#",             "SYS/foo",           true},
      {"home/+/status", "home/dryer/status", true},
      {"+",             "a/b",               false},
      {NULL,NULL,false}
   };
   const struct test_case *p = cases;

   int fails = 0;
   int total = 0;

   for (; p->filter; ++p, ++total)
   {
      if (MqttWildcardMatch(p->filter, p->topic) != p->match)
      {
         fprintf(stderr, "FAIL! MqttWildcardMatch(\"%s\", \"%s\") => %s, expected %s\n", p->filter, p->topic, b2s(!p->match), b2s(p->match));
         ++fails;
      }
   }

   printf("%d of %d tests failed\n", fails, total);
   return fails ? 1 : 0;
}
