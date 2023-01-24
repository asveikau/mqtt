#ifndef mqtt_pool_h_
#define mqtt_pool_h_

#include <stddef.h>
#include <stdbool.h>

#if defined(__cplusplus)
extern "C" {
#endif

struct pool
{
   char *buf;
   size_t len, capacity;
   void **prior_allocations;
   size_t nallocations;
   size_t nallocations_capacity;
};

bool
pool_alloc(struct pool *pool, void **dst, size_t len);

bool
pool_add_ptr(struct pool *pool, void **alloc);

void *
pool_finalize(struct pool *pool);

void
pool_discard(struct pool *pool);

#if defined(__cplusplus)
}
#endif
#endif
