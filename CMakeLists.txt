cmake_minimum_required(VERSION 2.8)
project(listdb)

set(CMAKE_CXX_STANDARD 14)

set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g -O2")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++0x -Wall -pedantic")

include_directories(
        deps/rocksdb/include
        deps/rocksdb
        deps/gen-cpp
        /usr/include/thrift
        /usr/local/include/thrift/
)

#SET(CMAKE_FIND_LIBRARY_SUFFIXES ".a")
#SET(CMAKE_FIND_LIBRARY_SUFFIXES ".dylib")
#SET(BUILD_SHARED_LIBRARIES OFF)
#SET(CMAKE_EXE_LINKER_FLAGS "-static")

set(SOURCE_FILES listdb_thrift.cc logger.cpp deps/gen-cpp/Listdb.cpp deps/gen-cpp/api_types.cpp listdb_server.cpp)
add_executable(listdb ${SOURCE_FILES})

#ADD_LIBRARY(/Users/feng/workspace/listdb/deps/rocksdb/librocksdb.5.3.0.dylib STATIC IMPORTED)


set(LIBS thrift boost_program_options pthread bz2 lz4  z snappy)
#target_include_directories(listdb  /Users/feng/workspace/listdb/deps/rocksdb)
target_link_libraries(listdb /Users/feng/workspace/listdb/deps/rocksdb/librocksdb.a)
target_link_libraries(listdb ${LIBS})
