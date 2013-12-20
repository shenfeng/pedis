default: listdb_server

SOURCES    := ${wildcard *.cpp}
HEADERS    := ${wildcard *.h}
OBJECTS    := ${SOURCES:.cpp=.o}

CPP        = g++
# CPPFLAGS   = -Wall -pedantic -O2 -g -c -std=c++0x
CPPFLAGS   = -Wall -pedantic -O3 -c -std=c++11
INCLUDE =-I ../deps/rocksdb/include -I ../deps/rocksdb  -I ../deps/redis/src
LIB=-lpthread
DEPS=../deps/rocksdb/librocksdb.a ae.o anet.o ../deps/redis/src/networking.o


%.o: %.cpp $(HEADERS)
	$(CPP) $(CPPFLAGS) $< -o $@ $(INCLUDE)

ae.o: ae.c anet.c
	gcc ae.c anet.c -c -O3

listdb_server: $(OBJECTS) redis_code ae.o
#	 (cd ../deps/rocksdb && make librocksdb.a)
	$(CPP) -O3 $(OBJECTS) -o listdb_server $(LIB) $(INCLUDE) $(LIB) $(DEPS)

clean:
	rm -rf *.o ae/*.o listdb_server
