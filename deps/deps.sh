#!/usr/bin/env bash

set -e
set -u


if [ ! -d deps/rocksdb ]; then
    echo "cloning rocksdb from github"
    (mkdir -p deps && cd deps && git clone https://github.com/facebook/rocksdb.git && cd rocksdb && make static_lib -j4)
else
    echo "update rocksdb from github"
#    (cd deps/rocksdb && git pull && make static_lib -j4)
fi


(cd deps && rm -rf gen-cpp && thrift -gen cpp ../api.thrift)