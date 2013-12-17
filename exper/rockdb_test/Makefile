CPP        = g++
# CPPFLAGS   = -Wall -pedantic -O2 -g -c -std=c++0x
CPPFLAGS   = -Wall -pedantic -O3 -std=c++0x -I ../../deps/rocksdb/include -I ../../deps/rocksdb/ -DROCKSDB_PLATFORM_POSIX  -DOS_LINUX
# LIB=-lboost_program_options -lthrift -lthriftnb -levent
LIB=-lpthread -ltcmalloc


SOURCES    := ${wildcard *.cpp}
HEADERS    := ${wildcard *.h}
OBJECTS    := ${SOURCES:.cpp=.o}

EXES := $(SOURCES:.cpp=)

%: %.cpp Makefile
  # (cd ../../deps/rocksdb && make )
	$(CPP) $(CFLAGS) $(CPPFLAGS) -o $@ $< $(LIB) ../../deps/rocksdb/librocksdb.a

all: $(EXES)

run: test
	LD_PRELOAD=/home/feng/workspace/gperftools-httpd-read-only/libghttpd-preload.so ./test
  # google-pprof ./test http://localhost:9999/pprof/profile

clean:
	rm -vf $(EXES)
