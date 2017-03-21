cmake_minimum_required(VERSION 3.6)
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


set(SOURCE_FILES listdb_thrift.cc logger.cpp deps/gen-cpp/Listdb.cpp deps/gen-cpp/api_types.cpp)

add_executable(listdb ${SOURCE_FILES})

set(LIBS thrift boost_program_options pthread)

target_link_libraries(listdb ${LIBS})