//
// Created by Feng Shen on 3/20/17.
//

#include "listdb_server.h"
#include "logger.hpp"

int ListDb::Open() {
    mDbs = new rocksdb::DB *[mConf.db_count];

    rocksdb::Options options;
    auto merger = new ListMergeOperator();
    options.create_if_missing = true;
    options.write_buffer_size = 1024 * 1024 * 256; // 64M buffer
    options.target_file_size_base = 1024 * 1024 * 64; // 32M file size
    options.db_log_dir = "log";
    options.wal_dir = "wal";
    options.compression = rocksdb::kSnappyCompression;
    options.merge_operator.reset(merger);
//        options.row_cache =
//        options.filter_policy = NewBloomFilterPolicy(10);
//        options.block_cache = NewLRUCache(1024 << 20);

    for (int i = 0; i < mConf.db_count; i++) {
        rocksdb::DB *db;
        Watch w;
        auto dir = this->mConf.db_dir + "/" + std::to_string(i);
        rocksdb::Status status = rocksdb::DB::Open(options, dir, &db);
        if (!status.ok())
            return -1;
        listdb::log_info("open db %d, takes %dms", i, w.tic());
        mDbs[i] = db;
    }
    return 0;
}

void ListDb::Push(ListPushArg &arg) {
    if (arg.db < mConf.db_count) {
        rocksdb::DB *db = this->mDbs[arg.db];
        for(const auto &v : arg.datas) {
            db->Merge(rocksdb::WriteOptions(), arg.key, v);
        }
    }
}

void ListDb::LRange(ListRangeArg &arg, std::vector<std::string> &result) {
    if (arg.db < mConf.db_count) {
        rocksdb::DB *db = this->mDbs[arg.db];
        std::string val;
        db->Get(rocksdb::ReadOptions(), arg.key, &val);
    }
}

void ListDb::Delete(std::string &key, int32_t db_idx) {
    if (db_idx < mConf.db_count) {
        rocksdb::DB *db = this->mDbs[(int) db_idx];
        db->Delete(rocksdb::WriteOptions(), key);
    }
}
