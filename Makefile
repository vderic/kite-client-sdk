.NOTPARALLEL:

prefix ?= /usr/local
#DIRS = src tests
DIRS = ext c

BUILDDIRS = $(DIRS:%=build-%)
CLEANDIRS = $(DIRS:%=clean-%)
FORMATDIRS = $(DIRS:%=format-%)

all: $(BUILDDIRS)

$(DIRS): $(BUILDDIRS)

$(BUILDDIRS):
	$(MAKE) -C $(@:build-%=%)

install: all
	install -d ${prefix} ${prefix}/bin ${prefix}/include ${prefix}/lib
	install -m 0644 -t ${prefix}/include src/kitesdk.h
	cp -r ext/include/event2 ext/include/ev*.h ext/include/xrg.h ${prefix}/include
	install -m 0644 -t ${prefix}/lib ext/lib/libevent*.a ext/lib/libxrg.a ext/lib/libsproto.a src/libkitesdk.a

format: $(FORMATDIRS)

clean: $(CLEANDIRS)

$(CLEANDIRS):
	$(MAKE) -C $(@:clean-%=%) clean

$(FORMATDIRS):
	$(MAKE) -C $(@:format-%=%) format

.PHONY: $(DIRS) $(BUILDDIRS) $(CLEANDIRS) $(FORMATDIRS)
.PHONY: all install format
