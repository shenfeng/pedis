#! /bin/bash

if [ ! -d deps/rocksdb ]; then
    echo "cloning rocksdb from github"
    (mkdir -p deps && cd deps && git clone git@github.com:facebook/rocksdb.git && cd rocksdb && make -j)
else
    echo "update rocksdb from github"
    (cd deps/rocksdb && git pull && make -j)
fi

if [ ! -d deps/leveldb ]; then
    echo "cloning leveldb from google code"
    (mkdir -p deps && cd deps && git clone https://code.google.com/p/leveldb/ && cd leveldb && make -j)
else
    echo "update leveldb from google code"
    (cd deps/leveldb && git pull && make -j)
fi


if [ ! -d deps/redis ]; then
    echo "cloning redis from github"
    (mkdir -p deps && cd deps && git clone git@github.com:antirez/redis.git && cd redis && make -j)
else
    echo "update redis from github"
    (cd deps/redis && git pull && make -j)
fi
