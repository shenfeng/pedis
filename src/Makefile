default: listdb_server

SOURCES    := ${wildcard *.cpp}
HEADERS    := ${wildcard *.h}
OBJECTS    := ${SOURCES:.cpp=.o}

CPP        = g++
# CPPFLAGS   = -Wall -pedantic -O2 -g -c -std=c++0x
CPPFLAGS   = -Wall -pedantic -O3 -c -std=c++11
INCLUDE =-I ../deps/rocksdb/include -I ../deps/rocksdb


%.o: %.cpp $(HEADERS)
	$(CPP) $(CPPFLAGS) $< -o $@ $(INCLUDE)

ae/ae.o: ae/ae.c
	(cd ae &&	gcc -std=c99 -O3 ae.c -o ae.o -c)

ae/anet.o: ae/anet.c
	(cd ae &&	gcc -std=c99 -O3 anet.c -o anet.o -c)

redis_code: ae/ae.o ae/anet.o

listdb_server: $(OBJECTS) redis_code
	# (cd ../deps/rocksdb && make librocksdb.a)
	$(CPP) -O3 $(OBJECTS) -o listdb_server $(LIB) $(INCLUDE)  ../deps/rocksdb/librocksdb.a

clean:
	rm -rf *.o ae/*.o listdb_server
