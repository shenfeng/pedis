#! /bin/bash

if [ ! -d deps/rocksdb ]; then
    echo "cloning rocksdb from github"
    (cd deps && git clone git@github.com:facebook/rocksdb.git)
fi
