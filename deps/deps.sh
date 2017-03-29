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
(cd tests && rm -rf gen-py && thrift -gen py ../api.thrift)
(cd tests/importdb && rm -rf gen-java && thrift -gen java ../../api.thrift)

(cd deps/java-client/ && rm -rf gen-java && thrift -gen java ../../api.thrift)
(cd deps/java-client/ && python ~/workspace/api-kit/thrift_client/gen_clients.py \
    --file ../../api.thrift \
    --package com.listdb \
    --thrift_package com.listdb \
    --cls ListdbClient \
    --dir src/main/java \
    --name Listdb --timeout 3000 --retry 2)



#  sudo yum install zlib-devel zlib bzip2 bzip2-devel lz4 lz4-devel snappy-devel snappy

# sudo yum install zlib-devel zlib bzip2 bzip2-devel lz4 lz4-devel snappy-devel snappy thrift-devel thrift


# cd /lib64/ && ln -s /usr/local/lib/libthriftnb-0.9.1.so
