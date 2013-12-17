CPP        = g++
# CPPFLAGS   = -Wall -pedantic -O2 -g -c -std=c++0x
CPPFLAGS   = -Wall -pedantic -O3 -std=c++0x -I ~/workspace/leveldb/include
# LIB=-lboost_program_options -lthrift -lthriftnb -levent
LIB=-lpthread


SOURCES    := ${wildcard *.cpp}
HEADERS    := ${wildcard *.h}
OBJECTS    := ${SOURCES:.cpp=.o}

EXES := $(SOURCES:.cpp=)

%: %.cpp Makefile
  # (cd ~/workspace/leveldb && make )
	$(CPP) $(CFLAGS) $(CPPFLAGS) -o $@ $< $(LIB) ~/workspace/leveldb/libleveldb.a

all: $(EXES)

clean:
	rm -vf $(EXES)
