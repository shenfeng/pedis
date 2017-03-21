//
// Created by Feng Shen on 3/20/17.
//

#ifndef LISTDB_LISTDB_ARG_H
#define LISTDB_LISTDB_ARG_H


#include <string>
#include <vector>
#include <ratio>
#include <chrono>
#include "rocksdb/db.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/cache.h"
#include "rocksdb/env.h"
//#include "util/coding.h"
#include "rocksdb/merge_operator.h"

using namespace std::chrono;

struct ListPushArg {
    int db;
    std::string key;
    std::vector<std::string> datas;
};

class Watch {
private:
    system_clock::time_point last;

public:
    Watch() : last(system_clock::now()) {}

    void tap() { last = system_clock::now(); }

    double tic() {
        auto now = system_clock::now();
        auto t = duration_cast<duration<double>>(now - last).count();
        last = now;
        return t * 1000; // in ms
    }
};

struct ListRangeArg {
    int db;
    int start;
    int limit;
    std::string key;
};

struct ListServerConf {
    int verbosity;
    int mode;

    size_t threads;                // how many thrift worker threads
    int port;                      //  which port to listen to

    std::string db_dir;            // 数据库的目录
    int db_count;                  // 多少个数据库
};

class ListMergeOperator : public rocksdb::MergeOperator {
public:
    virtual ~ListMergeOperator() {}

//    bool FullMerge(const rocksdb::Slice &key,
//                   const rocksdb::Slice *existing_value,
//                   const std::deque<std::string> &operand_list,
//                   std::string *new_value,
//                   rocksdb::Logger *logger) const {
//
//        return true;
//    }

    bool FullMergeV2(const MergeOperationInput &merge_in, MergeOperationOutput *merge_out) const {
        return true;
    }

    bool PartialMerge(const rocksdb::Slice &key, const rocksdb::Slice &left_operand,
                      const rocksdb::Slice &right_operand, std::string *new_value,
                      rocksdb::Logger *logger) const {
        return false;
    }

    const char *Name() const {
        return "list-merge";
    }
};

class ListDb {
public:
    void Push(ListPushArg &arg);

    void LRange(ListRangeArg &arg, std::vector<std::string> &result);

    void Delete(std::string &key, int32_t db);

    ListDb(ListServerConf &conf) : mConf(conf) {};

    int Open();

private:
    rocksdb::DB **mDbs;
    ListServerConf &mConf;
};


#endif //LISTDB_LISTDB_ARG_H
