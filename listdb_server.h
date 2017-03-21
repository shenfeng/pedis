//
// Created by Feng Shen on 3/20/17.
//

#ifndef LISTDB_LISTDB_ARG_H
#define LISTDB_LISTDB_ARG_H


#include <string>
#include <vector>
#include "rocksdb/db.h"
//#include "rocksdb/filter_policy.h"
//#include "rocksdb/cache.h"
//#include "rocksdb/env.h"
//#include "util/coding.h"
//#include "rocksdb/merge_operator.h"


struct ListPushArg {
    std::string key;
    std::vector<std::string> datas;
};

struct ListRangeArg {
    std::string key;
    int start;
    int limit;
};

struct ListServerConf {
    int verbosity;
    int mode;

    size_t threads;                // how many thrift worker threads
    int port;                      //  which port to listen to
};

class ListDb {
public:
    void Push(ListPushArg &arg);
    std::vector<std::string> LRange(ListRangeArg &arg);
private:
    rocksdb::DB *db;
};


#endif //LISTDB_LISTDB_ARG_H
