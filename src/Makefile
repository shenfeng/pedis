BIN=listdb-server

default: $(BIN)

SOURCES    := ${wildcard *.cpp}
HEADERS    := ${wildcard *.hpp}
OBJECTS    := ${SOURCES:.cpp=.o}

CPP        = g++
# CPPFLAGS   = -Wall -pedantic -O2 -g -c -std=c++0x
CPPFLAGS   = -Wall -pedantic -O3 -c -std=c++11
INCLUDE =-I ../deps/rocksdb/include -I ../deps/rocksdb  -I ../deps/redis/src
LIB=-lpthread
DEPS=../deps/rocksdb/librocksdb.a ae.o anet.o zmalloc.o
# DEPS=ae.o anet.o zmalloc.o



TARGET  := $(shell uname -s | tr [A-Z] [a-z] 2>/dev/null || echo unknown)
ifeq ($(TARGET), linux)
	CPPFLAGS += -DROCKSDB_PLATFORM_POSIX  -DOS_LINUX
else ifeq ($(TARGET), darwin)
	CPPFLAGS += -DROCKSDB_PLATFORM_POSIX  -DOS_MACOSX
endif

%.o: %.cpp $(HEADERS) $(SOURCES)
	$(CPP) $(CPPFLAGS) $< -o $@ $(INCLUDE)

ae.o: ae.c anet.c zmalloc.c
	gcc ae.c anet.c zmalloc.c -c -O3

$(BIN): $(OBJECTS) ae.o
#	 (cd ../deps/rocksdb && make librocksdb.a)
	$(CPP) -g $(OBJECTS) -o $(BIN) $(LIB) $(INCLUDE) $(LIB) $(DEPS)

run: $(BIN)
	LD_PRELOAD=/home/feng/workspace/gperftools-httpd-read-only/libghttpd-preload.so ./$(BIN)

clean:
	rm -rf *.o ae/*.o $(BIN)

tp: threadpool.cpp
	g++ -std=c++11 -c threadpool.cpp -DDEBUG_TP
