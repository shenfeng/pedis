//
// Created by Feng Shen on 3/20/17.
//

#include "listdb_server.h"

int ListDb::Open() {
    mDbs = new rocksdb::DB *[mConf.db_count];

    rocksdb::Options options;
    auto merger = new ListMergeOperator();
    options.create_if_missing = true;
    options.write_buffer_size = 1024 * 1024 * 256; // 64M buffer
    options.target_file_size_base = 1024 * 1024 * 64; // 32M file size
//    options.db_log_dir = "log";
//    options.wal_dir = "wal";
    options.compression = rocksdb::kSnappyCompression;
    options.merge_operator.reset(merger);
//        options.row_cache =
//        options.filter_policy = NewBloomFilterPolicy(10);
//        options.block_cache = NewLRUCache(1024 << 20);

    for (int i = 0; i < mConf.db_count; i++) {
        rocksdb::DB *db;
        Watch w;
        auto dir = this->mConf.db_dir + "/db" + std::to_string(i);
        rocksdb::Status status = rocksdb::DB::Open(options, dir, &db);
        if (!status.ok()) {
            listdb::log_fatal("open db %s, %s", dir.data(), status.ToString().data());
            return -1;
        }
        listdb::log_info("open db %d, takes %dms", i, w.tic());
        mDbs[i] = db;
    }
    return 0;
}

void ListDb::Push(const ListPushArg &arg) {
    if (arg.db < mConf.db_count) {
        rocksdb::DB *db = this->mDbs[arg.db];
        for (const auto &v : arg.datas) {
            db->Merge(rocksdb::WriteOptions(), arg.key, v);
        }
#ifndef NDEBUG
        listdb::log_trace("push %d:%s, %d", arg.db, arg.key.data(), arg.datas.size());
#endif
    }
}

void ListDb::LRange(const ListRangeArg &arg, std::vector<std::string> &result) {
    if (arg.db < mConf.db_count) {
        rocksdb::DB *db = this->mDbs[arg.db];
        std::string val;
        rocksdb::Status s = db->Get(rocksdb::ReadOptions(), arg.key, &val);
        if (s.ok()) {
            // 1. 计算个数
            auto p = val.data(), end = val.data() + val.size();
            uint32_t count = 0;
            while (p < end) {
                uint32_t size = 0;
                p = rocksdb::GetVarint32Ptr(p, p + 5, &size); // TODO corruption?
                count += 1;
                p += size;
            }

            // 2. 计算起始位置, 负数表示从后往前数 [start, last]
            int start = arg.start;
            int last = arg.last;
            while (last < 0) last += (int) count;
            while (start < 0) start += (int) count;

            // 3. 返回结果
            if (start > last) {
#ifndef NDEBUG
                listdb::log_trace("lrange %d:%s, [%d,%d] -> [%d,%d]", arg.db, arg.key.data(), arg.start, arg.last,
                                  start, last);
#endif
                return;
            };
            result.reserve(last - start + 1);
            p = val.data();
            for (int i = 0; p != end; i++) {
                uint32_t size = 0;
                p = rocksdb::GetVarint32Ptr(p, p + 5, &size);

                if (i < start) {
                    p += size;
                    continue;
                }
                if (i > last) break;
                result.emplace_back(p, size); // reuse value's memory
                p += size;
            }
#ifndef NDEBUG
            listdb::log_trace("lrange %d:%s, [%d,%d] -> [%d, %d], get %d", arg.db, arg.key.data(), arg.start, arg.last,
                              start, last, result.size());
#endif

        } else {
#ifndef NDEBUG
            listdb::log_trace("lrange %d:%s, %s", arg.db, arg.key.data(), s.ToString().data());
#endif
        }
    }
}

void ListDb::Delete(const std::string &key, int32_t db_idx) {
    if (db_idx < mConf.db_count) {
        rocksdb::DB *db = this->mDbs[(int) db_idx];
        rocksdb::Status s = db->Delete(rocksdb::WriteOptions(), key);
#ifndef NDEBUG
        listdb::log_trace("delete %d:%s %s", db_idx, key.data(), s.ToString().data());
#endif
    }
}
