cmake_minimum_required(VERSION 2.8)
project(logdb_v2)

set(CMAKE_CXX_STANDARD 14)

set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3 -DNDEBUG")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g")

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++0x -Wall -pedantic -DROCKSDB_PLATFORM_POSIX")

if (APPLE)
    # thriftnb 在 osx 下没搞对
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DOS_MACOSX")
    set(LIBS thrift boost_program_options pthread bz2 lz4 z snappy)
else ()
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DOS_LINUX")
    set(LIBS thrift thriftnb event boost_program_options pthread bz2 lz4 z snappy)
endif ()


include_directories(
        deps/rocksdb/include
        deps/rocksdb
        deps/gen-cpp
        /usr/include/thrift
        /usr/local/include/thrift
)

get_filename_component(LIBROCKSDB deps/rocksdb/librocksdb.a REALPATH)

set(SOURCE_FILES logger.cpp logger.cpp deps/gen-cpp/logdb_types.cpp deps/gen-cpp/logdb_api.cpp zp_logdb.cpp)
add_executable(logdb_v2 ${SOURCE_FILES})

target_link_libraries(logdb_v2 ${LIBROCKSDB})
target_link_libraries(logdb_v2 ${LIBS})