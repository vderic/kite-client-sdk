CC ?= gcc
CXX ?= g++
CFLAGS += -I../ext/include -Wmissing-declarations -Wall -Wextra -mavx2 -mfma -mbmi2 -MMD

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

CFILES = kite_client.c kitesdk.c
CXXFILES = 

OBJS = $(CXXFILES:.cpp=.o) $(CFILES:.c=.o)
EXECS = 

all: $(OBJS) $(EXECS) xcli

xcli: xcli.c  $(OBJS)
	$(CC) $(CFLAGS) -o $@ $(filter-out %.hpp %.h, $^) $(LDFLAGS) $(LDLIBS)


libkiteclient.a: $(OBJS)
	ar -rcs $@ $^

sql_parser/libpg_query.a:
	make -C sql_parser

-include $(OBJS:%.o=%.d) $(EXECS:%=%.d)

clean:
	rm -f *.a *.o *.d $(EXECS)
#	make -C sql_parser clean

format:
	clang-format -i *.cpp *.hpp *.c *.h

.PHONY: all clean format

