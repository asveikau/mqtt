/*
 Copyright (C) 2023 Andrew Sveikauskas

 Permission to use, copy, modify, and distribute this software for any
 purpose with or without fee is hereby granted, provided that the above
 copyright notice and this permission notice appear in all copies.
*/

#include <pollster/socket.h>
#include <pollster/pollster.h>
#include <common/logger.h>
#include <common/path.h>

#include <mqtt/client.h>
#include <mqtt/messages.h>
#include <mqtt/error.h>
#include <mqtt/util.h>

#include <stdio.h>

#include <vector>

static int
usage()
{
   error err;
   const char *argv0 = get_program_path(&err);
   const char *p;
   while (argv0 && (p = strpbrk(argv0, "/\\")))
      argv0 = p+1;
   if (!argv0)
      argv0 = "mqtt-client";

   fprintf(stderr, "usage: %s [-u user] [-p passwordfile] [-tls] host[:port]\n"
                   "          [-subscribe topic]\n"
                   "          [-publish topic -utf8 string [-retain]]\n"
                   "          [-qos <0-2>]\n"
                   "          [-keepalive <seconds>]\n"
                   "          [-wait]\n", argv0);
   return 1;
}

int
main(int argc, char **argv)
{
   error err;
   common::Pointer<pollster::waiter> loop;
   common::Pointer<pollster::auto_reset_signal> ev;
   MqttClient cli = nullptr;
   MqttClientArgs args = {0};
   char *host = nullptr;
   int port = -1;
   bool tls = false;
   char passwdBuf[4096];
   std::vector<const char *> subTopics;
   std::vector<std::pair<const char *, const char*>> publishes;
   int qos = 0;
   bool retain = false;
   bool cont = true;
   bool wait = false;
   int waitRefCnt = 0;
   auto waitRef = [&] (int i = 1)
   {
      waitRefCnt += i;
   };
   auto waitUnref = [&] (int i = 1)
   {
      if (!wait && (waitRefCnt -= i) <= 0)
      {
         cont = false;
      }
   };

   libcommon_set_argv0(argv[0]);

   log_register_callback(
      [] (void *np, const char *p) -> void { fputs(p, stderr); },
      nullptr
   );

   pollster::create(loop.GetAddressOf(), &err);
   ERROR_CHECK(&err);

   pollster::set_common_queue(loop.Get());

   // XXX
   // Workaround for dumb bug in libpollster.  A pending async host lookup leaves
   // us with an empty FD set.  Add an extra FD to not fall in that state.
   //
   loop->add_auto_reset_signal(false, ev.GetAddressOf(), &err);
   ERROR_CHECK(&err);

   for (int i=1; i<argc; ++i)
   {
      auto arg = argv[i];
      if (*arg == '-')
      {
         ++arg;
         if (!strcmp(arg, "u") || !strcmp(arg, "user"))
         {
            if (i+1 >= argc)
               return usage();
            if (args.Username)
            {
               fprintf(stderr, "Username specified twice\n");
               return usage();
            }
            args.Username = argv[++i];
         }
         else if (!strcmp(arg, "p") || !strcmp(arg, "passwordfile"))
         {
            if (i+1 >= argc)
               return usage();
            if (args.Password)
            {
               fprintf(stderr, "Password specified twice\n");
               return usage();
            }
            const char *file = argv[++i];
            FILE *f = nullptr;
            if (!strcmp(file, "-"))
            {
               printf("Enter password: ");
               fflush(stdout);
               f = stdin;
            }
            else
            {
               f = fopen(file, "r");
               if (!f)
               {
                  perror(file);
                  return 1;
               }
            }
            args.Password = fgets(passwdBuf, sizeof(passwdBuf), f);
            if (!args.Password)
            {
               perror(file);
               if (f != stdin)
                  fclose(f);
               return 1;
            }
            char *p = (char*)strpbrk(args.Password, "\r\n");
            if (p)
               *p = 0;
            if (f != stdin)
               fclose(f);
         }
         else if (!strcmp(arg, "tls") || !strcmp(arg, "ssl"))
         {
            tls = true;
         }
         else if (!strcmp(arg, "subscribe"))
         {
            if (i+1 >= argc)
               return usage();
            const char *topic = argv[++i];
            try
            {
               subTopics.push_back(topic);
            }
            catch (const std::bad_alloc &)
            {
               ERROR_SET(&err, nomem);
            }
         }
         else if (!strcmp(arg, "publish"))
         {
            if (i+1 >= argc)
               return usage();
            ++i;
            const char *topic = argv[i];
            if (i+1 >= argc)
               return usage();
            ++i;
            const char *innerArg = argv[i];
            if (i+1 >= argc)
            {
               return usage();
            }
            ++i;
            const char *payload = argv[i];
            if (*innerArg != '-')
               return usage();
            ++innerArg;
            if (!strcmp(innerArg, "utf8"))
            {
               try
               {
                  publishes.push_back(std::make_pair(topic, payload));
               }
               catch (const std::bad_alloc&)
               {
                  ERROR_SET(&err, nomem);
               }
            }
            else
            {
               return usage();
            }
         }
         else if (!strcmp(arg, "qos"))
         {
            if (i+1 >= argc)
               return usage();
            ++i;
            if (!check_atoi(argv[i], &qos) ||
                qos < 0 || qos > 2)
            {
               return usage();
            }
         }
         else if (!strcmp(arg, "retain"))
         {
            retain = true;
         }
         else if (!strcmp(arg, "wait"))
         {
            wait = true;
         }
         else if (!strcmp(arg, "keepalive"))
         {
            int keepAlive = 0;
            if (i+1 >= argc)
               return usage();
            ++i;
            if (!check_atoi(argv[i], &keepAlive))
               return usage();
            args.KeepAlive = keepAlive;
         }
         else
         {
            return usage();
         }
      }
      else
      {
         if (host)
            return usage();

         host = argv[i];
         char *portStart = host;
         if (*host == '[')
         {
            char *p = strchr(host, ']');
            if (p)
            {
               *p++ = 0;
               ++host;
               portStart = p;
            }
         }
         if ((portStart = strchr(portStart, ':')))
         {
            if (!check_atoi(portStart + 1, &port) ||
                port < 0 || port > 0xffff)
               return usage();
            *portStart = 0;
         }
      }
   }

   if (!host)
      return usage();

   MqttClient_SetErrorCallback(
      &args,
      [&] (error *err, bool *shouldRetry) -> void
      {
         // Terminate the loop if we're not in wait mode.
         // Otherwise, retry the connect loop on all errors.
         //
         if (!wait)
            cont = false;
         else
            *shouldRetry = true;
      },
      &err
   );

   MqttClientIo_New(&args.Io, host, tls, port, &err);
   ERROR_CHECK(&err);

   MqttClient_New(&cli, &args, &err);
   ERROR_CHECK(&err);

   if (subTopics.size())
   {
      std::vector<unsigned char> options;
      unsigned char *opts = nullptr;

      if (qos)
      {
         unsigned char ch;

         MqttSetSubscriptionOptions(&ch, qos, false, false, SendAllRetained);

         try
         {
            options.resize(subTopics.size(), ch);
         }
         catch (const std::bad_alloc &)
         {
            ERROR_SET(&err, nomem);
         }
         opts = options.data();
      }

      auto nTopics = subTopics.size();
      waitRef(nTopics);

      MqttClient_Subscribe(
         cli,
         subTopics.data(),
         subTopics.size(),
         opts,
         nullptr,
         [&waitUnref] (MqttPacket *pkt, error *err) -> void
         {
            auto &publish = pkt->Info.Publish;
            char *topic = nullptr;

            topic = MqttStringToCString(publish.Topic);
            if (!topic)
               ERROR_SET(err, nomem);

            printf("%s: ", topic);
            for (size_t i=0; i<pkt->AdditionalPayloadLength; ++i)
            {
               fputc(pkt->AdditionalPayload[i], stdout);
            }
            puts("");

         exit:
            waitUnref();
            free(topic);
         },
         [&waitUnref, nTopics] (MqttPacket *pkt, error *err) -> void
         {
            auto &subAck = pkt->Info.SubAck;
            for (int i = 0; i<subAck.ListSize; ++i)
            {
               auto reason = subAck.ReasonList[i];
               if (reason >= 0x80)
                  ERROR_SET(err, mqtt, (MqttReason)reason, subAck.Prop_ReasonString);
            }
         exit:
            if (ERROR_FAILED(err))
               waitUnref(nTopics);
         },
         &err
      );
      ERROR_CHECK(&err);
   }

   for (auto &pair : publishes)
   {
      MqttPublishArgs args = {0};

      args.Topic = pair.first;
      args.Utf8 = true;
      args.Retain = retain;
      args.Payload = pair.second;
      args.Length = strlen(pair.second);
      args.QosLevel = qos;

      auto topic = args.Topic;
      waitRef();

      MqttClient_Publish(
         cli,
         &args,
         [topic, &waitRef, &waitUnref] (MqttPacket *pkt, error *err)
         {
            unsigned char reason = 0;
            MqttString *reasonString = nullptr;

            // QOS 0 may call this with a null packet to indicate the PUBLISH
            // has made it to the socket.
            //
            if (!pkt)
               goto done;

            switch (pkt->Base.Type)
            {
            case PUBACK:
               reason = pkt->Info.PubAck.Reason;
               reasonString = pkt->Info.PubAck.Prop_ReasonString;
               break;
            case PUBREC:
               reason = pkt->Info.PubRec.Reason;
               reasonString = pkt->Info.PubRec.Prop_ReasonString;
               if (reason < 0x80)
                  waitRef();
               break;
            case PUBCOMP:
               reason = pkt->Info.PubComp.Reason;
               reasonString = pkt->Info.PubComp.Prop_ReasonString;
            }
            if (reason >= 0x80)
            {
               fprintf(stderr, "Failed to send topic: %s\n", topic);
               ERROR_SET(err, mqtt, (MqttReason)reason, reasonString);
            }
         done:
            printf("Successfully published: %s\n", topic);
         exit:
            waitUnref();
         },
         &err
      );
   }

   while (cont)
   {
      loop->exec(&err);
      ERROR_CHECK(&err);
   }
exit:
   return ERROR_FAILED(&err) ? 1 : 0;
}

