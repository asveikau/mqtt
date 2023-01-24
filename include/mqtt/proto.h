/*
 Copyright (C) 2023 Andrew Sveikauskas

 Permission to use, copy, modify, and distribute this software for any
 purpose with or without fee is hereby granted, provided that the above
 copyright notice and this permission notice appear in all copies.
*/

#ifndef mqttproto_h_
#define mqttproto_h_

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __APPLE__
// Hack for system header
#include <MacTypes.h>
#define Byte Byte_SILLY_GOOSE_MACINTOSH
#endif

#if defined(__cplusplus)
extern "C" {
#endif

#pragma pack(push, 1)
#if defined(_MSC_VER) && defined(__cplusplus)
#pragma warning(push)
#pragma warning(disable: 4200)
#endif

typedef struct
{
   uint16_t Length;
   char Payload[];
} MqttString;

typedef MqttString MqttBlob;

#pragma pack(pop)
#if defined(_MSC_VER) && defined(__cplusplus)
#pragma warning(pop)
#endif

typedef struct
{
   MqttString *First, *Second;
} MqttStringPair;

#define MQTT_GET_TYPE(TYPEANDFLAGS)       ((TYPEANDFLAGS) >> 4)
#define MQTT_SET_TYPE(TYPEANDFLAGS, VAL)  \
   ((TYPEANDFLAGS) = (((VAL) << 4) | MQTT_GET_FLAGS(TYPEANDFLAGS)))
#define MQTT_GET_FLAGS(TYPEANDFLAGS)      ((TYPEANDFLAGS) & 0xf)
#define MQTT_SET_FLAGS(TYPEANDFLAGS, VAL) \
   ((TYPEANDFLAGS) = ((TYPEANDFLAGS) & 0xf0) | (VAL))
#define MQTT_SET_TYPE_AND_FLAGS(TYPEANDFLAGS, TYPE, FLAGS) \
   ((TYPEANDFLAGS) = ((TYPE) << 4) | (FLAGS))

typedef enum
{
   CONNECT = 1,
   CONNACK,
   PUBLISH,
   PUBACK,
   PUBREC,
   PUBREL,
   PUBCOMP,
   SUBSCRIBE,
   SUBACK,
   UNSUBSCRIBE,
   UNSUBACK,
   PINGREQ,
   PINGRESP,
   DISCONNECT,
   AUTH,
} MqttPacketType;

typedef enum
{
// For publish
   Dup     = (1 << 3),
   QosBit0 = (1 << 1),
   QosBit1 = (1 << 2),
   Retain  = (1 << 0),
} MqttPacketFlags;

typedef enum
{
   PayloadFormatIndicator = 0x1,    // byte
   MessageExpiryInterval  = 0x2,    // u32
   ContentType            = 0x3,    // string
   ResponseTopic          = 0x8,    // string
   CorrelationData        = 0x09,   // binary data
   SubscriptionId         = 0x0b,   // vbi
   SessionExpiryInterval  = 0x11,   // u32
   AssignedClientId       = 0x12,   // string
   ServerKeepAlive        = 0x13,   // u16
   AuthMethod             = 0x15,   // string
   AuthData               = 0x16,   // binary data
   RequestProblemInfo     = 0x17,   // byte
   RequestResponseInfo    = 0x19,   // byte
   ResponseInfo           = 0x1a,   // string
   ServerReference        = 0x1c,   // string
   ReasonString           = 0x1f,   // string
   ReceiveMaximum         = 0x21,   // u16
   TopicAliasMaximum      = 0x22,   // u16
   TopicAlias             = 0x23,   // u16
   MaximumQos             = 0x24,   // byte
   RetainAvailable        = 0x25,   // byte
   UserProp               = 0x26,   // string pair
   MaximumPacketSize      = 0x27,   // u32
   WildcardSubAvail       = 0x28,   // byte
   SubIdAvail             = 0x29,   // byte
   SharedSubAvail         = 0x2a,   // byte

   WillDelayInterval      = 0x18,   // u32
} MqttPropertyId;

typedef enum
{
   // ISSUE: missing qos values
   DisconnectWithWill = 0x4,
   NoSubscribers      = 0x10,
   NoSuchSubscription = 0x11,
   ContinueAuth       = 0x18,
   ReAuth             = 0x19,

   GenericError                = 0x80,
   MalformedPacket             = 0x81,
   ProtocolError               = 0x82,
   ImplementationSpecificError = 0x83,
   UnsupportedProtocol         = 0x84,
   BadClientId                 = 0x85,
   BadUserOrPassword           = 0x86,
   NotAuthorized               = 0x87,
   ServerUnavailable           = 0x88,
   ServerBusy                  = 0x89,
   Banned                      = 0x8a,
   ServerShuttingDown          = 0x8b,
   BadAuthMethod               = 0x8c,
   KeepAliveTimeout            = 0x8d,
   SessionTakenOver            = 0x8e,
   TopicFilterInvalid          = 0x8f,
   TopicNameInvalid            = 0x90,
   PacketIdInUse               = 0x91,
   PacketIdNotFound            = 0x92,
   ReceiveMaximumExceeded      = 0x93,
   TopicAliasInvalid           = 0x94,
   PacketTooLarge              = 0x95,
   MessageRateTooHigh          = 0x96,
   QuotaExceeded               = 0x97,
   AdministrativeAction        = 0x98,
   PayloadFormatInvalid        = 0x99,
   RetainNotSupported          = 0x9a,
   QosNotSupported             = 0x9b,
   UseAnotherServer            = 0x9c,
   ServerMoved                 = 0x9d,
   SharedSubNotSupported       = 0x9e,
   ConnRateExceeded            = 0x9f,
   SubIdNotSupported           = 0xa1,
   WildcardSubNotSupported     = 0xa2,
} MqttReason;

typedef enum
{
   CleanStart   = (1 << 1),
   WillFlag     = (1 << 2),
   WillQosBit0  = (1 << 3),
   WillQosBit1  = (1 << 4),
   WillRetain   = (1 << 5),
   PasswordFlag = (1 << 6),
   UserNameFlag = (1 << 7),
} MqttConnectFlags;

typedef enum
{
   SessionPresent = (1 << 0),
} MqttConnAckFlags;

typedef enum
{
   String,
   StringPair,
   Blob,
   U32,
   U16,
   Byte,
   Vbi,
} MqttPropertyType;

typedef struct
{
   MqttPropertyId Id;
   MqttPropertyType Type;
   union
   {
      MqttString *String;
      MqttStringPair StringPair;
      MqttBlob *Blob;
      uint32_t I32;
      uint16_t I16;
      unsigned char Byte;
      uint64_t Vbi;
   };
} MqttProperty;

static inline int
MqttGetQosLevel(MqttPacketFlags flags)
{
   return (((unsigned int)flags) >> 1) & 0x3;
}

static inline void
MqttSetQosLevel(MqttPacketFlags *fl, int qos)
{
   // NB: Weird casts because of C++.
   *fl = (MqttPacketFlags)((~(QosBit0 | QosBit1) & (unsigned int)*fl) | (qos << 1));
}

enum MqttRetainHandling
{
   SendAllRetained = 0,
   SendNewRetained = 1,
   DoNotSendRetained = 2,
};

static inline void
MqttSetSubscriptionOptions(
   unsigned char *out,
   int maxQosLevel,
   bool noLocal,
   bool retainAsPublished,
   enum MqttRetainHandling retainHandling
)
{
   *out = 0;
   if (maxQosLevel < 0)
      maxQosLevel = 0;
   if (maxQosLevel > 2)
      maxQosLevel = 2;
   *out |= maxQosLevel;
   if (noLocal)
      *out |= (1 << 2);
   if (retainAsPublished)
      *out |= (1 << 3);
   *out |= (((int)retainHandling) << 4);
}

static inline void
MqttGetSubscriptionOptions(
   unsigned char in,
   int *maxQosLevel,
   bool *noLocal,
   bool *retainAsPublished,
   enum MqttRetainHandling *retainHandling
)
{
   *maxQosLevel = (in & 0x3);
   *noLocal = (in & (1<<2));
   *retainAsPublished = (in & (1<<3));
   *retainHandling = (enum MqttRetainHandling)((in >> 4) & 0x3);
}

static inline
bool
MqttParseVbi(const unsigned char *p, size_t len, uint64_t *out, size_t *bytesIn)
{
   const unsigned char *q = p;
   uint64_t m = 1;

   *out = 0;
   do
   {
      if (!len || *out > ((uint64_t)(~0ULL)) / 0x80)
         return false;
      --len;
      *out += (*p & ~0x80) * m;
      m *= 0x80;
   } while ((*p++) & 0x80);

   *bytesIn = (p - q);
   return true;
}

static inline
bool
MqttEncodeVbi(unsigned char *p, size_t len, uint64_t in, size_t *bytesOut)
{
   unsigned char *q = p;

   do
   {
      unsigned char next = (in & 0x7f);

      if (!len)
         return false;
      --len;

      in -= next;
      if (in)
      {
         next |= 0x80;
         in /= 0x80;
      }
      *p++ = next;
   } while (in);

   *bytesOut = (p - q);
   return true;
}

static inline
uint16_t
MqttStringGetLength(MqttString *str)
{
   const unsigned char *p = (const unsigned char*)&str->Length;
   return p[1] | ((uint16_t)p[0]) << 8;
}

static inline void
MqttStringSetLength(MqttString *str, uint16_t len)
{
   unsigned char *p = (unsigned char*)&str->Length;
   p[0] = len >> 8;
   p[1] = len & 0xff;
}

#define MqttBlobGetLength MqttStringGetLength
#define MqttBlobSetLength MqttStringSetLength

#if defined(__cplusplus)
}
#endif
#endif
