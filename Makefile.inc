MAKEFILES_ROOT?=$(MQTT_ROOT)submodules/makefiles/
-include $(MAKEFILES_ROOT)Makefile.inc

-include $(MQTT_ROOT)config.mk

CFLAGS += -I$(MQTT_ROOT)include

MQTT_SRC := \
   $(MQTT_ROOT)src/lib/read.c \
   $(MQTT_ROOT)src/lib/pool.c \
   $(MQTT_ROOT)src/lib/util.c \
   $(MQTT_ROOT)src/lib/wildcard.c \
   $(MQTT_ROOT)src/lib/write.c

ifdef USE_MQTT_CLIENT
USE_LIBCOMMON:=y

ifdef USE_MQTT_CLIENT_TRANSPORT
USE_POLLSTER:=y

MQTT_SRC += \
   $(MQTT_ROOT)src/lib/client-transport.cc
endif # client transport
MQTT_SRC += \
   $(MQTT_ROOT)src/lib/client.cc
endif # client

ifdef USE_LIBCOMMON
LIBCOMMON_ROOT:=$(MQTT_ROOT)submodules/common/
include $(LIBCOMMON_ROOT)Makefile.inc
MQTT_CFLAGS += -I$(LIBCOMMON_ROOT)include

MQTT_SRC += \
   $(MQTT_ROOT)src/lib/error.c
endif
ifdef USE_POLLSTER
LIBPOLLSTER_ROOT:=$(MQTT_ROOT)submodules/pollster/
include $(LIBPOLLSTER_ROOT)Makefile.inc
MQTT_CXXFLAGS += -I$(LIBPOLLSTER_ROOT)include
endif

MQTT_OBJS := $(shell $(SRC2OBJ) $(MQTT_SRC))

MQTT=$(MQTT_ROOT)$(LIBPREFIX)mqtt$(LIBSUFFIX)

$(MQTT): $(MQTT_OBJS)
	$(LIBWRAPPER) $(MQTT_OBJS)

$(MQTT_ROOT)generated-files: $(MQTT_ROOT)scripts/gen-message-types.pl $(MQTT_ROOT)message-types
	rm -f "$@"
	rm -f $(MQTT_ROOT)src/lib/generated/*.c $(MQTT_ROOT)include/mqtt/generated/*.h
	mkdir -p $(MQTT_ROOT)src/lib/generated $(MQTT_ROOT)include/mqtt/generated
	(cd $(or $(MQTT_ROOT),.); perl scripts/gen-message-types.pl < message-types; cd -)
	touch "$@"

$(MQTT_ROOT)include/mqtt/generated/messages.h: $(MQTT_ROOT)generated-files
$(MQTT_ROOT)src/lib/generated/messages-parser.c: $(MQTT_ROOT)generated-files
$(MQTT_ROOT)src/lib/generated/messages-writer.c: $(MQTT_ROOT)generated-files

-include $(LIBC_ROOT)depend.mk
