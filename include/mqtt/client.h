/*
 Copyright (C) 2023-2024, 2026 Andrew Sveikauskas

 Permission to use, copy, modify, and distribute this software for any
 purpose with or without fee is hereby granted, provided that the above
 copyright notice and this permission notice appear in all copies.
*/

#ifndef mqttclient_h_
#define mqttclient_h_

#include <stdbool.h>
#include <stddef.h>
#include <inttypes.h>

#include <common/error.h>

#include <new>
#include <memory>

#if defined(__cplusplus)
extern "C" {
#endif

struct MqttClient_;
typedef struct MqttClient_ *MqttClient;

typedef struct
{
   void (*set_client)(void *ctx, MqttClient cli, error *err);
   void (*write)(void *ctx, const void *buf, size_t len, void (*on_complete)(void *cctx, error *err), void *cctx, error *err);
   void (*set_timer)(void *ctx, uint16_t seconds, error *err);
   void (*close)(void *ctx);
} MqttClientIoFuncs;

typedef struct
{
   const MqttClientIoFuncs *Funcs;
} MqttClientIo;

void
MqttClientIo_New(
   MqttClientIo **iop,
   const char *hostname,
   bool tls,
   int port, // -1 for default
   error *err
);

typedef struct
{
   MqttClientIo *Io;
   const char *ClientId;
   const char *Username;
   const char *Password;

   uint16_t KeepAlive;

   // This callback will be called:
   // * On connection closed (ERROR_FAILED(err) will be false for this case)
   // * On a network error
   // * On certain global protocol errors, i.e. login failed
   //   But typically NOT from errors that can be paired back to a response callback.
   // * If a response callback stores an error into the (err) object.
   //   I.e. this is ultimately the caller's callback initializing an error.
   // The *shouldRetry parameter should be pre-loaded with the library's idea of
   // whether or not this is a retryable error.  The callback storing false into
   // it will abort the retry loop.
   // If *shouldRetry is true, client-transport will continue its reconnect loop.
   //
   void (*OnError)(
      void *ctx,
      error *err,
      bool *shouldRetry
   );
   void *OnErrorContext;
   void (*OnErrorContextFree)(void*);
} MqttClientArgs;

void
MqttClient_New(MqttClient *client, MqttClientArgs *args, error *err);

void
MqttClient_WritePacket(
   MqttClient client,
   struct MqttPacket_ *packet,
   // NB: only some packet types support responses.
   void (*on_response)(void *, struct MqttPacket_ *, error *err),
   void *on_response_ctx,
   void (*response_ctx_free)(void*),
   error *err
);

void
MqttClient_Subscribe(
   MqttClient client,
   const char **topics,
   int nTopics,
   const unsigned char *subscriptionOptions,
   uint64_t *sub_id_out_opt,
   void (*on_publish)(void *, struct MqttPacket_ *, error *),
   void *publish_ctx,
   void (*publish_ctx_free)(void*),
   void (*on_response)(void *, struct MqttPacket_ *, error *err),
   void *on_response_ctx,
   void (*response_ctx_free)(void*),
   error *err
);

void
MqttClient_Unsubscribe(
   MqttClient client,
   uint64_t subid,
   void (*on_response)(void *, struct MqttPacket_ *, error *err),
   void *on_response_ctx,
   void (*response_ctx_free)(void*),
   error *err
);

typedef struct
{
   const char *Topic;

   const void *Payload;
   size_t Length;

   const char *ContentType;
   bool Utf8;

   bool Retain;
   int QosLevel;
} MqttPublishArgs;

void
MqttClient_Publish(
   MqttClient client,
   MqttPublishArgs *args,
   void (*on_response)(void *, struct MqttPacket_ *, error *err),
   void *on_response_ctx,
   void (*response_ctx_free)(void*),
   error *err
);

void
MqttClient_Release(MqttClient client);

//
// Called by I/O implementation
//

void
MqttClient_OnConnected(MqttClient cli, error *err);

void
MqttClient_OnBytesIn(MqttClient cli, void *buf, size_t len, error *err);

void
MqttClient_OnTimerEllapsed(MqttClient cli, error *err);

void
MqttClient_OnError(MqttClient cli, error *err);

void
MqttClient_OnClose(MqttClient cli);

#if defined(__cplusplus)
}
#endif

#if defined(__cplusplus)

template <typename T>
void MqttClient_SetErrorCallback(MqttClientArgs *args, const T &cb, error *err)
{
   auto p = new (std::nothrow) T(std::move(cb));
   if (!p)
   {
      error_set_nomem(err);
      return;
   }
   args->OnErrorContext = p;
   args->OnError = [] (void *ctx, error *err, bool *retry) -> void
   {
      auto p = (T*)ctx;
      (*p)(err, retry);
   };
   args->OnErrorContextFree = [] (void *ctx) -> void
   {
      auto p = (T*)ctx;
      delete p;
   };
}

struct MqttClient_CbSet
{
   void (*on_response)(void *, struct MqttPacket_ *, error *err);
   void *ctx;
   void (*ctx_free)(void*);

   MqttClient_CbSet() : on_response(nullptr), ctx(nullptr), ctx_free(nullptr) {}
   MqttClient_CbSet(const MqttClient_CbSet&) = delete;
   ~MqttClient_CbSet() { if (ctx_free) ctx_free(ctx); }
};

template<typename T>
void
MqttClient_WrapCppCb(
   const T &cbIn,
   MqttClient_CbSet *cbSet,
   error *err
)
{
   T *p = nullptr;
   try
   {
      p = new T(cbIn);
   }
   catch (const std::bad_alloc &)
   {
      ERROR_SET(err, nomem);
   }
   cbSet->ctx = p;
   cbSet->ctx_free = [] (void *pp) -> void { auto p = (T*)pp; delete p; };
   cbSet->on_response = [] (void *pp, struct MqttPacket_ *pkt, error *err) -> void
   {
      auto p = (T*)pp;
      (*p)(pkt, err);
   };
exit:;
}

template<typename T>
void
MqttClient_WritePacket(
   MqttClient client,
   struct MqttPacket_ *packet,
   const T &cbIn,
   error *err
)
{
   MqttClient_CbSet cbSet;

   MqttClient_WrapCppCb(cbIn, &cbSet, err);
   ERROR_CHECK(err);

   MqttClient_WritePacket(
      client,
      packet,
      cbSet.on_response,
      cbSet.ctx,
      cbSet.ctx_free,
      err
   );
   cbSet.ctx_free = nullptr;
exit:;
}

template<typename T1, typename T2>
void
MqttClient_Subscribe(
   MqttClient client,
   const char **topics,
   int nTopics,
   const unsigned char *subscriptionOptions,
   uint64_t *sub_id_out_opt,
   const T1 &cbPublish,
   const T2 &cbResponse,
   error *err
)
{
   MqttClient_CbSet publishCb, responseCb;

   MqttClient_WrapCppCb(cbPublish, &publishCb, err);
   ERROR_CHECK(err);

   MqttClient_WrapCppCb(cbResponse, &responseCb, err);
   ERROR_CHECK(err);

   MqttClient_Subscribe(
      client,
      topics,
      nTopics,
      subscriptionOptions,
      sub_id_out_opt,
      publishCb.on_response,
      publishCb.ctx,
      publishCb.ctx_free,
      responseCb.on_response,
      responseCb.ctx,
      responseCb.ctx_free,
      err
   );
   publishCb.ctx_free = responseCb.ctx_free = nullptr;
exit:;
}

template<typename T>
void
MqttClient_Unsubscribe(
   MqttClient client,
   uint64_t subid,
   const T &cb,
   error *err
)
{
   MqttClient_CbSet cbSet;

   MqttClient_WrapCppCb(cb, &cbSet, err);
   ERROR_CHECK(err);

   MqttClient_Unsubscribe(
      client,
      subid,
      cbSet.on_response,
      cbSet.ctx,
      cbSet.ctx_free,
      err
   );
   cbSet.ctx_free = nullptr;
exit:;
}

template<typename T>
void
MqttClient_Publish(
   MqttClient client,
   MqttPublishArgs *args,
   const T &cb,
   error *err
)
{
   MqttClient_CbSet cbSet;

   MqttClient_WrapCppCb(cb, &cbSet, err);
   ERROR_CHECK(err);

   MqttClient_Publish(
      client,
      args,
      cbSet.on_response,
      cbSet.ctx,
      cbSet.ctx_free,
      err
   );
   cbSet.ctx_free = nullptr;
exit:;
}

#endif

#endif
