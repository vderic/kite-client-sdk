TOPDIR = ..
XRGTAR = xrg-r230828.zip
XRGDIR = $(XRGTAR:.zip=)
SOCKSTREAMTAR = sockstream-r220819.zip
SOCKSTREAMDIR = $(SOCKSTREAMTAR:.zip=)
LIBEVENTTAR = libevent-2.1.12-stable.tar.gz
LIBEVENTDIR = $(LIBEVENTTAR:.tar.gz=)

all: $(SUBDIR) include/xrg.h include/sockstream.h include/event2/event.h

$(SUBDIR):
	mkdir $@

include/event2/event.h : tarball/$(LIBEVENTTAR)
	rm -rf $(LIBEVENTDIR)
	tar zxvf tarball/$(LIBEVENTTAR)
	(cd $(LIBEVENTDIR) && mkdir -p build && cd build && cmake -DCMAKE_C_FLAGS=-fPIC -DEVENT__LIBRARY_TYPE=STATIC -DCMAKE_INSTALL_PREFIX=../.. .. && make && make install)

include/xrg.h: tarball/$(XRGTAR)
	rm -rf $(XRGDIR)
	unzip -q tarball/$(XRGTAR)
	make -C $(XRGDIR) install prefix=$(shell pwd)

include/sockstream.h : tarball/$(SOCKSTREAMTAR)
	rm -rf $(SOCKSTREAMDIR)
	unzip -q tarball/$(SOCKSTREAMTAR)
	make -C $(SOCKSTREAMDIR) install prefix=$(shell pwd)

include/rapidjson: tarball/rapidjson-master.zip
	rm -rf rapidjson-master
	unzip -q tarball/rapidjson-master.zip
	cp -r rapidjson-master/include/rapidjson include
	rm -rf rapidjson-master


wipe:
	rm -rf bin include lib share $(XRGDIR) $(SOCKSTREAMDIR) $(LIBEVENTDIR)
