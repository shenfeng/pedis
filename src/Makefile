BIN=pedis_server

default: $(BIN)

CPP        = g++
INCLUDE =-I ../deps/rocksdb/include -I ../deps/rocksdb -I network
LIB=-lpthread ../deps/rocksdb/librocksdb.a

# CPPFLAGS   = -Wall -pedantic -O2 -g -c -std=c++0x
CPPFLAGS  = -Wall -pedantic -O3 -std=c++11
TARGET    := $(shell uname -s | tr [A-Z] [a-z] 2>/dev/null || echo unknown)
ifeq ($(TARGET), linux)
	CPPFLAGS += -DROCKSDB_PLATFORM_POSIX  -DOS_LINUX
else ifeq ($(TARGET), darwin)
	CPPFLAGS += -DROCKSDB_PLATFORM_POSIX  -DOS_MACOSX
endif

%.c.o: %.c
	gcc $< -o $@ -c -O3 -I network

%.cpp.o: %.cpp
	$(CPP) $(CPPFLAGS) -c $< -o $@ $(INCLUDE)

# compile network code
NET_SOURCES    := ${wildcard network/*.c}
NET_HEADERS    := ${wildcard network/*.h}
NET_OBJECTS    := ${NET_SOURCES:.c=.c.o}

_network: $(NET_OBJECTS)  $(NET_HEADERS)

# compile util code
UTIL_SOURCES    := ${wildcard util/*.cpp}
UTIl_HEADERS    := ${wildcard util/*.hpp}
UTIL_OBJECTS    := ${UTIL_SOURCES:.cpp=.cpp.o}

_util:  $(UTIL_OBJECTS)  $(UTIl_HEADERS)

# pedis server code
SOURCES    := ${wildcard *.cpp}
HEADERS    := ${wildcard *.hpp}
OBJECTS    := ${SOURCES:.cpp=.cpp.o}

_pedis: $(OBJECTS) $(HEADERS)


$(BIN): _network _util _pedis pedis_server.cc
	$(CPP) $(CPPFLAGS) $(NET_OBJECTS) $(UTIL_OBJECTS) $(OBJECTS) -o $(BIN) $(INCLUDE) $(LIB) pedis_server.cc

clean:
	rm -rvf *.o network/*.o util/*.o $(BIN)
