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
#include "util/coding.h"
#include "logger.hpp"
#include "rocksdb/merge_operator.h"

using namespace std::chrono;

struct ListPushArg {
    const int db;
    const std::string &key;
    const std::vector<std::string> &datas;

    ListPushArg(int db, const std::string &key, const std::vector<std::string> &datas) :
            db(db), key(key), datas(datas) {}
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
    const int db;
    const int start;
    const int last;
    const std::string &key;

    ListRangeArg(int db, int start, int last, const std::string &key) : db(db), start(start), last(last), key(key) {}
};

struct ListServerConf {
    int verbosity;

    size_t threads;                //  how many thrift worker threads
    int port;                      //  which port to listen to

    std::string db_dir;            // 数据库的目录
    int db_count;                  // 多少个数据库
};

class ListMergeOperator : public rocksdb::MergeOperator {
public:
    virtual ~ListMergeOperator() {}

    bool FullMergeV2(const MergeOperationInput &merge_in, MergeOperationOutput *merge_out) const {
        merge_out->new_value.clear();

        // Compute the space needed for the final result.
        size_t numBytes = 0;
        for (auto &s : merge_in.operand_list) {
            numBytes += s.size() + rocksdb::VarintLength(s.size());

#ifndef NDEBUG
            if (merge_in.existing_value) {
                listdb::log_debug("FullMergeV2, exits %s, %s, total bytes %d", merge_in.existing_value->data(),
                                  s.data(), numBytes);
            } else {
                listdb::log_debug("FullMergeV2, no exits, %s, total bytes %d", s.data(), numBytes);
            }
#endif
        }

        if (merge_in.existing_value) {
            merge_out->new_value.reserve(numBytes + merge_in.existing_value->size());
        } else {
            merge_out->new_value.reserve(numBytes);
        }

        for (auto &s: merge_in.operand_list) {
            // encode list item as: size + data
            rocksdb::PutVarint32(&merge_out->new_value, (uint32_t) s.size()); // put size
            merge_out->new_value.append(s.data_, s.size());
        }

#ifndef NDEBUG
        if (merge_in.existing_value) {
            listdb::log_debug("FullMergeV2, exits %s, get %s %d/%d", merge_in.existing_value->data(),
                              merge_out->new_value.data(), numBytes, merge_out->new_value.size());
        } else {
            listdb::log_debug("FullMergeV2, no exits, get %s %d/%d", merge_out->new_value.data(), numBytes,
                              merge_out->new_value.size());
        }
#endif


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
    ListDb(ListServerConf &conf) : mConf(conf) {};

    void Push(const ListPushArg &arg);

    void LRange(const ListRangeArg &arg, std::vector<std::string> &result);

    void Delete(const std::string &key, int32_t db);

    int Open();

private:
    rocksdb::DB **mDbs;
    ListServerConf &mConf;
};


#endif //LISTDB_LISTDB_ARG_H
