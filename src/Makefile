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
LDLIBS = -llibevent -lxrg -lcrypto -lz -llz4 -lpthread -ldl

CFILES = kite_client.c kitesdk.c
CXXFILES = 

OBJS = $(CXXFILES:.cpp=.o) $(CFILES:.c=.o)
EXECS = 

all: $(OBJS) $(EXECS) 

xsanitize: xsanitize.cpp libexx.a sql_parser/libpg_query.a
	$(CXX) $(CXXFLAGS) -o $@ $(filter-out %.hpp %.h, $^) $(LDFLAGS) $(LDLIBS)

xevaluate: xevaluate.cpp libexx.a sql_parser/libpg_query.a
	$(CXX) $(CXXFLAGS) -o $@ $(filter-out %.hpp %.h, $^) $(LDFLAGS) $(LDLIBS)

xparse: xparse.cpp libexx.a sql_parser/libpg_query.a
	$(CXX) $(CXXFLAGS) -o $@ $(filter-out %.hpp %.h, $^) $(LDFLAGS) $(LDLIBS)

xplan: xplan.cpp libexx.a sql_parser/libpg_query.a
	$(CXX) $(CXXFLAGS) -o $@ $(filter-out %.hpp %.h, $^) $(LDFLAGS) $(LDLIBS)

xbench: xbench.cpp libexx.a sql_parser/libpg_query.a
	$(CXX) $(CXXFLAGS) -o $@ $(filter-out %.hpp %.h, $^) $(LDFLAGS) $(LDLIBS)

xrun: xrun.cpp libexx.a sql_parser/libpg_query.a
	$(CXX) $(CXXFLAGS) -o $@ $(filter-out %.hpp %.h, $^) $(LDFLAGS) $(LDLIBS)

xcompact: xcompact.cpp compact.cpp
	$(CXX) $(CXXFLAGS) -o $@ $(filter-out %.hpp %.h, $^) $(LDFLAGS) $(LDLIBS)

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

