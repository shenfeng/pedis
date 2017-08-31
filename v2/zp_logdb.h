//
// Created by Feng Shen on 8/31/17.
//

#ifndef LISTDB_V2_LOGDB_H
#define LISTDB_V2_LOGDB_H


#include <string>
#include <vector>
#include <ratio>
#include <chrono>
#include <cstdlib>     /* srand, rand */
#include <mutex>
#include "rocksdb/db.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/cache.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/env.h"
#include "table/block_based_table_factory.h"
#include "rocksdb/table.h"
#include "util/coding.h"
#include <utility>



struct ZpLogDbConf {
    int verbosity;
    int port;                      //  which port to listen to
    size_t threads;                //  how many thrift worker threads
    size_t max_val;                 // 单个key的最大长度，过长，则踢掉老的数据


    std::string db_dir;            // 数据库的目录
    size_t cache_size;             // block cache的大小，m
};

class ListMergeOperator : public rocksdb::MergeOperator {
private:
    size_t max_val;
public:
    ListMergeOperator(size_t max_val): max_val(max_val) {
    }

    virtual ~ListMergeOperator() {}

    bool FullMergeV2(const MergeOperationInput &merge_in, MergeOperationOutput *merge_out) const {
        merge_out->new_value.clear();

        // Compute the space needed for the final result.
        size_t numBytes = 0;
        for (auto &s : merge_in.operand_list) {
            numBytes += s.size() + rocksdb::VarintLength(s.size());

        }

        if (merge_in.existing_value) {
            merge_out->new_value.reserve(numBytes + merge_in.existing_value->size());
            merge_out->new_value.append(merge_in.existing_value->data(), merge_in.existing_value->size());
        } else {
            merge_out->new_value.reserve(numBytes);
        }

        for (auto &s: merge_in.operand_list) {
            // encode list item as: size + data
            rocksdb::PutVarint32(&merge_out->new_value, (uint32_t) s.size()); // put size
            merge_out->new_value.append(s.data_, s.size());
        }

        if (max_val > 0 && merge_out->new_value.size() > max_val) { // 超过了大小限制， 去掉前面的
            size_t to_ignore = merge_out->new_value.size() - max_val;

            auto p = merge_out->new_value.data(), start = merge_out->new_value.data(), end = merge_out->new_value.data() + merge_out->new_value.size();
            while (p < end) {
                uint32_t size = 0;
                auto t = rocksdb::GetVarint32Ptr(p, p + 5, &size);
                t += size;
                if (t - start > to_ignore) break;
                p = t;
            }

            listdb::log_debug("FullMergeV2, trim %d -> %d/%d", merge_out->new_value.size(), to_ignore, merge_out->new_value.size() - (p - start));
            if(p != start)
                merge_out->new_value = merge_out->new_value.substr(p - start);
        }

        return true;
    }

    bool PartialMerge(const rocksdb::Slice &key, const rocksdb::Slice &left_operand,
                      const rocksdb::Slice &right_operand, std::string *new_value,
                      rocksdb::Logger *logger) const {
        return false;
    }

    bool PartialMergeMulti(const rocksdb::Slice &key, const std::deque<rocksdb::Slice>& operand_list,
                           std::string *new_value,
                           rocksdb::Logger *logger) const {
        return false;
    }


    const char *Name() const {
        return "list-merge";
    }
};


#endif //LISTDB_V2_LOGDB_H
