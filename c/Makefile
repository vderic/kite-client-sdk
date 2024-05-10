ARCH=$(shell uname -m)
prefix ?= /usr/local
CC ?= gcc
CXX ?= g++
CFLAGS = -I../ext/include -Wmissing-declarations -Wall -Wextra -MMD -fPIC
ifeq ($(ARCH), x86_64)
	MARCH ?= broadwell
	CFLAGS += -march=$(MARCH)
else ifeq ($(ARCH), aarch64)
	CFLAGS += -D__ARM_NEON__
else
	$(error unsupported arch $(ARCH))
endif


ifdef DEBUG
    CFLAGS += -O0 -g
else
    CFLAGS += -O3 -funroll-loops -ftree-vectorize -DNDEBUG 
endif

ifdef PROFILE
    CFLAGS += -pg
endif

CFLAGS += -std=c99
CXXFLAGS += $(filter-out -std=c99, $(CFLAGS))  -std=c++17 -static-libstdc++
LDFLAGS = -L../ext/lib -Wl,-rpath='$$ORIGIN'/../ext/lib
LDLIBS = -levent -lsproto -lxrg -lcrypto -lz -llz4 -lpthread -ldl

CFILES = stringbuffer.c kite_result.c kitesdk.c
CXXFILES = 

OBJS = $(CXXFILES:.cpp=.o) $(CFILES:.c=.o)
EXECS = 

all: $(OBJS) $(EXECS) xcli libkitesdk.a

xcli: xcli.c  $(OBJS)
	$(CC) $(CFLAGS) -o $@ $(filter-out %.hpp %.h, $^) $(LDFLAGS) $(LDLIBS)


libkitesdk.a: $(OBJS)
	ar -rcs $@ $^

-include $(OBJS:%.o=%.d) $(EXECS:%=%.d)

install: all
	install -d ${prefix} ${prefix}/include ${prefix}/lib
	install -m 0644 -t ${prefix}/include kitesdk.h
	install -m 0644 -t ${prefix}/lib libkitesdk.a


clean:
	rm -f *.a *.o *.d $(EXECS)

format:
	clang-format -i *.c *.h

.PHONY: all clean format

