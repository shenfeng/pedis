cmake_minimum_required(VERSION 2.8)
project(listdb)

set(CMAKE_CXX_STANDARD 14)

set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3 -DNDEBUG")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g")


set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++0x -Wall -pedantic -DROCKSDB_PLATFORM_POSIX")

if (LINUX)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DOS_LINUX")
elseif (APPLE)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DOS_MACOSX")
endif ()

include_directories(
        deps/rocksdb/include
        deps/rocksdb
        deps/gen-cpp
        /usr/include/thrift
        /usr/local/include/thrift/
)

set(SOURCE_FILES listdb_thrift.cc logger.cpp deps/gen-cpp/Listdb.cpp deps/gen-cpp/api_types.cpp listdb_server.cpp)
add_executable(listdb ${SOURCE_FILES})

get_filename_component(LIBROCKSDB deps/rocksdb/librocksdb.a REALPATH)
set(LIBS thrift boost_program_options pthread bz2 lz4 z snappy)

target_link_libraries(listdb ${LIBROCKSDB})
target_link_libraries(listdb ${LIBS})