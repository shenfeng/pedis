//
// Created by Feng Shen on 3/20/17.
//

#include "listdb_server.h"

int ListDb::Open() {
    mDbs = new rocksdb::DB *[mConf.db_count];

    rocksdb::Options options;
    auto merger = new ListMergeOperator();
    options.create_if_missing = true;
    options.write_buffer_size = 1024 * 1024 * 64; // 64M buffer
    options.target_file_size_base = 1024 * 1024 * 64; // 64M file size
    options.compression = rocksdb::kZlibCompression;
    options.merge_operator.reset(merger);

    if (mConf.cache_size > 0) {
        std::shared_ptr<rocksdb::Cache> cache = rocksdb::NewLRUCache(mConf.cache_size * 1024 * 1024);
        rocksdb::BlockBasedTableOptions table_options;
        table_options.block_cache = cache;
        options.table_factory.reset(new rocksdb::BlockBasedTableFactory(table_options));
    }

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


static void get_key(const std::string &user_key, std::string &result, int type) {
    result.reserve(user_key.size() + 1);
    if (type == kTypeList) {
        result.push_back('l');
    } else if (type == kTypeKey) {
        result.push_back('k');
    }
    result.append(user_key);
}

static int key_type(const rocksdb::Slice &s) {
    if (s.data_[0] == 'l') return kTypeList;
    else if (s.data_[0] == 'k') return kTypeKey;
    return -1;
}

void ListDb::Push(const ListPushArg &arg) {
    this->clear_iterators();

    if (arg.db < mConf.db_count) {
        rocksdb::DB *db = this->mDbs[arg.db];
        std::string key;
        get_key(arg.key, key, kTypeList);
        for (const auto &v : arg.datas) {
            db->Merge(rocksdb::WriteOptions(), key, v);
        }
#ifndef NDEBUG
        listdb::log_trace("push %d:%s, %d", arg.db, arg.key.data(), arg.datas.size());
#endif
    }
}

void ListDb::LRange(const ListRangeArg &arg, std::vector<std::string> &result) {
    this->clear_iterators();
    if (arg.db < mConf.db_count) {
        rocksdb::DB *db = this->mDbs[arg.db];
        std::string val, key;
        get_key(arg.key, key, kTypeList);
        rocksdb::Status s = db->Get(rocksdb::ReadOptions(), key, &val);
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

void ListDb::Delete(const std::string &user_key, int32_t db_idx) {
    this->clear_iterators();
    if (db_idx < mConf.db_count) {
        rocksdb::DB *db = this->mDbs[(int) db_idx];
        std::string key;
        get_key(user_key, key, kTypeList);
        rocksdb::Status s = db->Delete(rocksdb::WriteOptions(), key);
#ifndef NDEBUG
        listdb::log_trace("delete %d:%s %s", db_idx, key.data(), s.ToString().data());
#endif
    }
}

void
ListDb::Scan(const ListScanArg &arg, std::string &cursor_out, std::vector<std::tuple<std::string, int>> &keys_out) {
    this->clear_iterators();
    if (arg.db < mConf.db_count) {
        rocksdb::DB *db = this->mDbs[arg.db];
        rocksdb::Iterator *it = nullptr;
        std::map<std::string, ScanIterator> &map = this->mIterators;
        {
            std::lock_guard<std::mutex> _(this->mMutex);
            auto itit = map.find(arg.cursor);
            if (itit != map.end()) {
                it = itit->second.it;
#ifndef NDEBUG
                listdb::log_debug("cursor %s, use old", arg.cursor.data());
#endif
                this->mIterators.erase(itit);
            }
        }

        if (it == nullptr) {
            it = db->NewIterator(rocksdb::ReadOptions());
            it->SeekToFirst();
#ifndef NDEBUG
            listdb::log_debug("new cursor for %s", arg.cursor.data());
#endif
        }

        int n = 0;
        for (; it->Valid() && n < arg.limit; it->Next()) {
            const rocksdb::Slice slice = it->key();
            keys_out.push_back(std::make_pair(std::string(slice.data_ + 1, slice.size() - 1), key_type(slice)));
            n += 1;
        }

        if (it->Valid()) { // 还有更多
            auto now = system_clock::now();
            std::lock_guard<std::mutex> _(this->mMutex);
            while (1) {
                auto c = std::to_string(std::rand() % 1000000 + 10);
                if (map.find(c) == map.end()) {
                    map.emplace(c, ScanIterator(it, now));
#ifndef NDEBUG
                    listdb::log_debug("save new cursor, %s, total %d", c.data(), map.size());
#endif
                    cursor_out = c;
                    break;
                }
            }
        } else {
            cursor_out = "0";
            delete it;
        }
    }
}
