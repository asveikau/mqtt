/*
 Copyright (C) 2023, 2026 Andrew Sveikauskas

 Permission to use, copy, modify, and distribute this software for any
 purpose with or without fee is hereby granted, provided that the above
 copyright notice and this permission notice appear in all copies.
*/

#include <mqtt/client.h>
#include <mqtt/error.h>
#include <pollster/sockapi.h>
#include <common/logger.h>
#include <common/minmax.h>

#include <stdio.h>

#include <string>
#include <memory>

namespace {

struct MqttClientIoImpl;

struct RefCountedState
{
   // This is basically a poor man's weak pointer, since this object will
   // be refcounted it allows us to safely track in a callback whether or
   // not the object has died, i.e. This has been set to null.
   //
   MqttClientIoImpl *This;

   RefCountedState(MqttClientIoImpl *t) : This(t) {}
};

template <int InitialValue, int Max>
class FibonacciBackoff
{
   int i, j;
public:
   FibonacciBackoff() { ResetTimer(); }

   int GetDelay()
   {
      auto r = i;
      i = j;
      j += r;
      return MIN(i, Max);
   }

   void ResetTimer()
   {
      i = j = InitialValue;
   }
};

struct MqttClientIoImpl : public MqttClientIo
{
   bool useTls;
   std::string hostname;
   int port;
   std::shared_ptr<pollster::StreamSocket> sock;
   std::shared_ptr<RefCountedState> refCntState;
   FibonacciBackoff<500, 30000> backoff;
   common::Pointer<pollster::waiter> waiter;
   common::Pointer<pollster::event> timer;
   MqttClient cli;

   void
   SetClient(MqttClient cli, error *err)
   {
      char portbuf[8];
      auto rc = refCntState;

      if (port < 0)
         port = useTls ? 8883 : 1883;

      snprintf(portbuf, sizeof(portbuf), "%d", port);

      log_printf("mqtt-client: Connecting to %s port %s (%stls) ...\n", hostname.c_str(), portbuf, useTls ? "": "no ");

      try
      {
         sock = std::make_shared<pollster::StreamSocket>();
      }
      catch (const std::bad_alloc &)
      {
         ERROR_SET(err, nomem);
      }

      sock->on_connect_progress = [cli, rc] (pollster::ConnectAsyncStatus status, const char *arg, error *err) -> void
      {
         LogConnectAsyncStatus(status, arg);
         if (status == pollster::Connected)
         {
            if (rc->This)
               rc->This->backoff.ResetTimer();
            MqttClient_OnConnected(cli, err);
         }
      };

      sock->on_error = [cli, rc] (error *err) -> void
      {
         log_printf("mqtt-client: Hit error\n");

         MqttClient_OnError(cli, err);
         if (rc->This)
            rc->This->TryRetry(cli);
      };
      sock->on_closed = [cli, rc] (error *err) -> void
      {
         log_printf("mqtt-client: Connection closed\n");

         MqttClient_OnClose(cli);
         if (rc->This)
            rc->This->TryRetry(cli);
      };

      sock->on_recv = [cli] (const void *buf, size_t len, error *err) -> void
      {
         MqttClient_OnBytesIn(cli, (void*)buf, len, err);
      };

      if (useTls)
      {
         pollster::SslArgs args;

         args.HostName = hostname.c_str();

         CreateSslFilter(args, sock->filter, err);
         ERROR_CHECK(err);
      }

      sock->Connect(hostname.c_str(), portbuf);

      this->cli = cli;
   exit:;
   }

   void TryRetry(MqttClient cli)
   {
      auto rc = refCntState;
      error err;
      auto delay = backoff.GetDelay();

      log_printf("mqtt-client: No connection or fatal error occurred, retrying in %d ms ...\n", delay);

      pollster::sleep(
         nullptr,
         backoff.GetDelay(),
         [rc, cli] (error *err) -> void
         {
            if (rc->This)
               rc->This->SetClient(cli, err);
         },
         &err
      );
   }

   void
   Write(const void *buf, size_t len, void (*on_complete)(void *ctx, error *err), void *ctx, error *err)
   {
      if (on_complete)
      {
         struct FnWrapper
         {
            void (*on_complete)(void *ctx, error *err);
            void *ctx;

            FnWrapper(void (*on_complete_)(void *ctx, error *err), void *ctx_)
               : on_complete(on_complete_), ctx(ctx_) {}

            FnWrapper(const FnWrapper &) = delete;

            ~FnWrapper()
            {
               if (on_complete)
               {
                  error err;
                  error_set_unknown(&err, "Completion callback deallocated without call");
                  (*this)(&err);
               }
            }

            void operator()(error *err)
            {
               if (on_complete)
               {
                  auto fn = on_complete;
                  on_complete = nullptr;
                  fn(ctx, err);
                  ctx = nullptr;
               }
            }
         };
         try
         {
            auto fnw = std::make_shared<FnWrapper>(on_complete, ctx);
            sock->Write(buf, len, [fnw] (error *err) { (*fnw)(err); });
         }
         catch (const std::bad_alloc &)
         {
            ERROR_SET(err, nomem);
         }
      }
      else
      {
         sock->Write(buf, len);
      }
   exit:;
   }

   void
   SetTimer(uint16_t seconds, error *err)
   {
      auto rc = refCntState;

      if (!waiter.Get())
      {
         pollster::get_common_queue(waiter.GetAddressOf(), err);
         ERROR_CHECK(err);
      }

      ResetTimer();
      waiter->add_timer(
         1000 * (uint64_t)seconds,
         false,
         [rc] (pollster::event *ev, error *err)
         {
            ev->on_signal = [rc, ev] (error *err)
            {
               if (rc->This && rc->This->cli)
                  MqttClient_OnTimerEllapsed(rc->This->cli, err);

               ev->on_signal = std::function<void(error *err)>();
            };
            ev->on_error = [rc, ev] (error *err)
            {
               if (rc->This && rc->This->sock)
               {
                  auto cb = std::move(rc->This->sock->on_error);
                  rc->This->sock->Close();
                  rc->This->sock = nullptr;
                  cb(err);
                  return;
               }
               ev->on_error = std::function<void(error *err)>();
            };
         },
         timer.GetAddressOf(),
         err
      );
      ERROR_CHECK(err);
   exit:;
   }

   void
   ResetTimer()
   {
      if (timer.Get())
      {
         error err;
         timer->on_signal = timer->on_error = std::function<void(error *)>();
         timer->remove(&err);
      }
      timer = nullptr;
   }

   void
   Close()
   {
      refCntState->This = nullptr;
      ResetTimer();
      if (sock)
         sock->Close();
      delete this;
   }

   MqttClientIoImpl(bool tls, int port_) :
      useTls(tls),
      port(port_),
      refCntState(std::make_shared<RefCountedState>(this)),
      cli(nullptr)
   {
      static const MqttClientIoFuncs funcs =
      {
         [] (void *ctx, MqttClient cli, error *err) -> void
         {
            auto This = (MqttClientIoImpl*)ctx;
            This->SetClient(cli, err);
         },
         [] (void *ctx, const void *buf, size_t len, void (*on_complete)(void *ctx, error *err), void *cctx, error *err) -> void
         {
            auto This = (MqttClientIoImpl*)ctx;
            This->Write(buf, len, on_complete, cctx, err);
         },
         [] (void *ctx, uint16_t seconds, error *err) -> void
         {
            auto This = (MqttClientIoImpl*)ctx;
            This->SetTimer(seconds, err);
         },
         [] (void *ctx) -> void
         {
            auto This = (MqttClientIoImpl*)ctx;
            This->Close();
         }
      };
      Funcs = &funcs;
   }
};

}

void
MqttClientIo_New(
   MqttClientIo **iop,
   const char *hostname,
   bool tls,
   int port,
   error *err
)
{
   auto io = new (std::nothrow) MqttClientIoImpl(tls, port);
   if (!io)
      ERROR_SET(err, nomem);
   try
   {
      io->hostname = hostname;
   }
   catch (const std::bad_alloc &)
   {
      ERROR_SET(err, nomem);
   }
exit:
   if (ERROR_FAILED(err) && io)
   {
      delete io;
      io = nullptr;
   }
   *iop = io;
}
