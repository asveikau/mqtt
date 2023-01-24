.PHONY: all all-phony clean depend

all: all-phony

WINDOWS_SUBSYSTEM=console

include Makefile.inc

CFLAGS += -Wall

ifndef WINDOWS
CFLAGS += -pedantic
CFLAGS += -fPIC
endif

LDFLAGS_PREFIX += -L. -lmqtt

ifdef USE_POLLSTER
LIB_DEPENDS += $(LIBPOLLSTER)
LDFLAGS_PREFIX += -L$(LIBPOLLSTER_ROOT) -lpollster
TESTS += mqtt-client$(EXESUFFIX)
endif
ifdef USE_LIBCOMMON
LIB_DEPENDS += $(LIBCOMMON)
LDFLAGS_PREFIX += -L$(LIBCOMMON_ROOT) -lcommon
endif

TESTS += vbi-test$(EXESUFFIX)
TESTS += wildcard-test$(EXESUFFIX)

all-phony: $(MQTT) $(LIB_DEPENDS) $(TESTS)

mqtt-client$(EXESUFFIX): src/bin/client.cc $(MQTT) $(XP_SUPPORT_OBJS)
	${CXX} -o $@ ${CFLAGS} ${MQTT_CFLAGS} ${MQTT_CXXFLAGS} src/bin/client.cc ${LDFLAGS_PREFIX} ${LDFLAGS}
vbi-test${EXESUFFIX}: src/bin/vbi-test.c $(MQTT) $(XP_SUPPORT_OBJS)
	${CC} -o $@ ${CFLAGS} ${MQTT_CFLAGS} src/bin/vbi-test.c ${LDFLAGS_PREFIX} ${LDFLAGS}
wildcard-test$(EXESUFFIX): src/bin/wildcard-test.c $(MQTT) $(XP_SUPPORT_OBJS)
	${CC} -o $@ ${CFLAGS} ${MQTT_CFLAGS} src/bin/wildcard-test.c ${LDFLAGS_PREFIX} ${LDFLAGS}

clean:
	rm -f $(MQTT) $(LIB_DEPENDS) $(TESTS)
	rm -f $(MQTT_OBJS)
	rm -f include/mqtt/generated/*.h src/lib/generated/*.c
	rm -f generated-files
	git submodule foreach git clean -dfx .

export
depend: generated-files
	env PROJECT=MQTT $(DEPEND) \
	src/lib/*.c src/lib/*.cc tests/*.cc \
		> depend.mk
