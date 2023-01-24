/*
 Copyright (C) 2023 Andrew Sveikauskas

 Permission to use, copy, modify, and distribute this software for any
 purpose with or without fee is hereby granted, provided that the above
 copyright notice and this permission notice appear in all copies.
*/

#include "pool.h"

#include <stdlib.h>
#include <stdint.h>
#include <string.h>

#include <limits.h>

static bool
check_capacity(void *bufp, size_t len, size_t *capacity, size_t newsize, size_t unit)
{
   void **buf = bufp;
   size_t alloc = *capacity;

   if (newsize < len)
      return true;

   if (SIZE_MAX/unit < newsize)
      return false;

   newsize *= unit;

   if (!alloc)
      alloc = 32;

   while (alloc < newsize)
   {
      if (SIZE_MAX/2 < alloc)
      {
         alloc = newsize;
         break;
      }
      alloc *= 2;
   }

   if (alloc != *capacity)
   {
      void *p;
      if (*buf)
         p = realloc(*buf, alloc);
      else
         p = malloc(alloc);
      if (!p)
         return false;
      *buf = p;
      *capacity = alloc;
   }

   return true;
}

static void
check_move(void **p, intptr_t min, intptr_t max, char *newbuf)
{
   intptr_t ip = (intptr_t)*p;
   if (ip >= min && ip < max)
   {
      *p = newbuf + (ip - min);
   }
}

static void
patch_after_realloc(struct pool *pool, char *oldbuf)
{
   intptr_t oldbuf_min = (intptr_t)oldbuf;
   intptr_t oldbuf_max = oldbuf_min + pool->len;
   size_t i;

   for (i=0; i<pool->nallocations; ++i)
   {
      check_move(&pool->prior_allocations[i], oldbuf_min, oldbuf_max, pool->buf);
      check_move(pool->prior_allocations[i], oldbuf_min, oldbuf_max, pool->buf);
   }
}

bool
pool_alloc(struct pool *pool, void **dst, size_t len)
{
   size_t mod = 0;
   void *oldbuf = NULL;
   const int align = 8;

   if (!pool || SIZE_MAX - 1 == pool->nallocations || SIZE_MAX - len < pool->len)
      return false;

   if ((mod = len % align))
   {
      int delta = (int)(align-mod);
      if (SIZE_MAX - delta < len)
         return false;
      len += delta;
   }

   oldbuf = pool->buf;

   if (!check_capacity(&pool->buf, pool->len, &pool->capacity, pool->len + len, 1))
      return false;

   if (oldbuf != pool->buf)
      patch_after_realloc(pool, oldbuf);

   if (!check_capacity(&pool->prior_allocations, pool->nallocations, &pool->nallocations_capacity, pool->nallocations+1, sizeof(void*)))
      return false;

   *dst = pool->buf + pool->len;
   pool->len += len;
   pool->prior_allocations[pool->nallocations++] = dst;

   return true;
}

bool
pool_add_ptr(struct pool *pool, void **alloc)
{
  if (!pool || SIZE_MAX - 1 == pool->nallocations)
      return false;

   if (!check_capacity(&pool->prior_allocations, pool->nallocations, &pool->nallocations_capacity, pool->nallocations+1, sizeof(void*)))
      return false;

   pool->prior_allocations[pool->nallocations++] = alloc;
   return true;
}

void *
pool_finalize(struct pool *pool)
{
   void *r = NULL;

   if (pool)
   {
      // Try to shrink the buffer.
      //
      if (pool->buf)
      {
         void *p = realloc(pool->buf, pool->len);
         if (p)
         {
            if (p != pool->buf)
            {
               void *old = pool->buf;
               pool->buf = p;
               patch_after_realloc(pool, old);
            }
         }
      }

      r = pool->buf;
      pool->buf = NULL;

      pool_discard(pool);
   }
   return r;
}

void
pool_discard(struct pool *pool)
{
   if (pool)
   {
      free(pool->buf);
      free(pool->prior_allocations);
      memset(pool, 0, sizeof(*pool));
   }
}
