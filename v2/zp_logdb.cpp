//
// Created by Feng Shen on 8/31/17.
//

#include <iostream>
#include <vector>
#include <csignal>
#include <unistd.h>
#include <unordered_set>
#include "rocksdb/db.h"
#include "rocksdb/filter_policy.h"
#include "table/block_based_table_factory.h"
#include "logger.hpp"
#include "zp_logdb.h"
#include "logdb_api.h"

#include <boost/program_options.hpp>
#include <thrift/concurrency/ThreadManager.h>
#include <thrift/server/TThreadPoolServer.h>
#include <thrift/transport/TServerSocket.h>
#include <concurrency/PlatformThreadFactory.h>
#include <thrift/transport/TBufferTransports.h> // TFramedTransportFactory

#ifndef OS_MACOSX
#include <thrift/server/TNonblockingServer.h>
#endif


using boost::shared_ptr;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace apache::thrift::server;
using namespace apache::thrift::concurrency;
using namespace std::chrono;

class Ticker {
private:
    time_point<system_clock> start;
public:
    Ticker() : start(system_clock::now()) {
    }

    long long int ms() {
        auto t = duration_cast<std::chrono::milliseconds>(system_clock::now() - start);
        return t.count();
    }
};


struct ScanIterator {
    rocksdb::Iterator *it;
    system_clock::time_point last;

    ScanIterator(rocksdb::Iterator *it, system_clock::time_point last) : it(it), last(last) {}
};


class ZpLogDbHandler : virtual public logdb_apiIf {
private:
    const ZpLogDbConf &conf;
    rocksdb::DB **mdbs;

    system_clock::time_point mMinLast;
    std::mutex mMutex;
    std::map<std::string, ScanIterator> mIterators;

    void clear_iterators() {
        std::lock_guard<std::mutex> _(this->mMutex);
        if (mIterators.empty()) return;

        auto now = system_clock::now();
        auto t = duration_cast<std::chrono::milliseconds>(now - mMinLast);
        if (t.count() < 5 * 60 * 1000) return;
        mMinLast = this->mIterators.begin()->second.last;

        for (auto it = this->mIterators.begin(); it != this->mIterators.end();) {
            t = duration_cast<std::chrono::milliseconds>(now - it->second.last);
            if (t.count() > 5 * 60 * 1000) {
                delete it->second.it;
                it = this->mIterators.erase(it);
            } else {
                if (it->second.last < mMinLast) {
                    mMinLast = it->second.last;
                }
                ++it;
            }
        }
    }

public:
    virtual ~ZpLogDbHandler() {}
    ZpLogDbHandler(const ZpLogDbConf &cnf) : conf(cnf) {}

public:
    int Open() {
        rocksdb::Options options;

        auto merger = new ListMergeOperator(conf.max_val);
        options.merge_operator.reset(merger);

        options.create_if_missing = true;
        options.write_buffer_size = 1024 * 1024 * 128; // 128M buffer
        options.target_file_size_base = 1024 * 1024 * 256; // 256M file size
        options.compaction_readahead_size = 1024 * 1024 * 2;
        options.compression = rocksdb::kZlibCompression;
        options.max_open_files = -1;
        options.env->SetBackgroundThreads(2, rocksdb::Env::Priority::HIGH);
        options.env->SetBackgroundThreads(2, rocksdb::Env::Priority::LOW);

        if (conf.cache_size > 0) {
            std::shared_ptr<rocksdb::Cache> cache = rocksdb::NewLRUCache(conf.cache_size * 1024 * 1024, 4);
            rocksdb::BlockBasedTableOptions table_options;
            table_options.block_cache = cache;
            table_options.block_size = 8 * 1024; // 8k
            options.table_factory.reset(new rocksdb::BlockBasedTableFactory(table_options));
        }

        mdbs = new rocksdb::DB *[2];

        for (int i = 0; i < 2; i++) {
            rocksdb::DB *db;
            Ticker ticker;
            auto dir = this->conf.db_dir + "/db" + std::to_string(i);
            rocksdb::Status status = rocksdb::DB::Open(options, dir, &db);
            if (!status.ok()) {
                listdb::log_fatal("open db %s, %s", dir.data(), status.ToString().data());
                return -1;
            }
            listdb::log_info("open db idx %d, %s, takes %dms", i, dir.data(), ticker.ms());
            mdbs[i] = db;
        }

        return 0;
    }

    void Push(const std::vector<PushReq> &reqs) {
        for (auto &r: reqs) {
            if (r.db < 0 || r.db >= 2) continue;
            for (auto &l: r.logs) {
                auto *buf = new TMemoryBuffer(256);
                shared_ptr<TTransport> tran(buf);
                TBinaryProtocol protocol(tran);
                l.write(&protocol);

                uint8_t *p;
                uint32_t size;
                buf->getBuffer(&p, &size);

                std::string val;
                val.reserve(size);
                val.append((char *) p, size);
                mdbs[r.db]->Merge(rocksdb::WriteOptions(), r.key, val);
            }
        }
    }

    void Delete(const std::string& key, const int32_t db) {
        if(db < 0 || db >= 2) return;
        mdbs[db]->Delete(rocksdb::WriteOptions(), key);
    }

    void Range(std::vector<LogItem> & _return, const RangeReq &req) {
        this->clear_iterators();
        if(req.db < 0 || req.db >= 2) return;

        rocksdb::DB *db = this->mdbs[req.db];
        std::string val;
        rocksdb::Status s = db->Get(rocksdb::ReadOptions(), req.key, &val);
        if (s.ok()) {
            // 1. 计算个数
            auto p = val.data(), end = val.data() + val.size();
            int count = 0;
            while (p < end) {
                uint32_t size = 0;
                p = rocksdb::GetVarint32Ptr(p, p + 5, &size); // TODO corruption?
                count += 1;
                p += size;
            }

            // 2. 计算起始位置, 负数表示从后往前数 [start, last]
            int start = req.start;
            int last = req.last;
            if (start <= -count) start = 0;
            if (last <= -count) start = 0;

            while (last < 0) last += count;
            while (start < 0) start += count;

            // 3. 返回结果
            if (start > last) {
#ifndef NDEBUG
                listdb::log_trace("lrange %d:%s, [%d,%d] -> [%d,%d]", req.db, req.key.data(), req.start, req.last,
                                  start, last);
#endif
                return;
            };
            int r_size = last - start + 1;
            _return.reserve(r_size > count ? count : r_size);
            p = val.data();
            auto buf = new TMemoryBuffer((uint8_t *) p, 1);
            shared_ptr<TTransport> tran(buf);

            for (int i = 0; p != end; i++) {
                uint32_t size = 0;
                p = rocksdb::GetVarint32Ptr(p, p + 5, &size);

                if (i < start) {
                    p += size;
                    continue;
                }
                if (i > last) break;
                buf->resetBuffer((uint8_t *) p, size);
                TBinaryProtocol rp(tran);
                LogItem li;
                li.read(&rp);
                _return.push_back(li);
                p += size;
            }
#ifndef NDEBUG
            listdb::log_trace("lrange %d:%s, [%d,%d] -> [%d, %d], get %d", req.db, req.key.data(), req.start, req.last,
                              start, last, _return.size());
#endif

        } else {
#ifndef NDEBUG
            listdb::log_trace("lrange %d:%s, %s", req.db, req.key.data(), s.ToString().data());
#endif
        }

    }

    void Ranges(std::vector<std::vector<LogItem> > & _return, const std::vector<RangeReq> &reqs) {
        _return.resize(reqs.size());
        for(int i = 0; i < reqs.size(); i++) {
            this->Range(_return[i], reqs[i]);
        }
    }


    void Scan(ScanResp &_return, const ScanReq &req) {
        if (req.db < 0 || req.db >= 2) return;

        this->clear_iterators();

        rocksdb::Iterator *it = nullptr;
        std::map<std::string, ScanIterator> &map = this->mIterators;
        if (!req.cursor.empty()) {
            std::lock_guard<std::mutex> _(this->mMutex);
            auto itit = map.find(req.cursor);
            if (itit != map.end()) {
                it = itit->second.it;
                this->mIterators.erase(itit);
            }
        }
        if (it == nullptr) {
            Ticker ticker;
            auto opt = rocksdb::ReadOptions();
            opt.fill_cache = false;
            opt.readahead_size = 2 * 1024 * 1024;
            it = mdbs[req.db]->NewIterator(opt);
            it->SeekToFirst();
            listdb::log_info("scan new it, in %s, out %s, %dms", req.cursor.data(), _return.cursor.data(),
                             ticker.ms());
        }

        int n = 0;
        for (; it->Valid() && n < req.limit; it->Next()) {
            const rocksdb::Slice slice = it->key();
            _return.keys.emplace_back(slice.data(), slice.size());
            n++;
        }

        if (it->Valid()) {
            auto now = system_clock::now();
            std::lock_guard<std::mutex> _(this->mMutex);
            while (1) {
                auto c = std::to_string(std::rand() % 1000000 + 10);
                if (map.find(c) == map.end()) {
                    map.emplace(c, ScanIterator(it, now));
                    _return.__set_cursor(c);
                    break;
                }
            }
        } else {
            _return.__set_cursor("0"); // done
            delete it;
        }
    }
};

void reopen_log(int param) {
    listdb::log_reopen();
}

void parse_args(char **argv, int argc, ZpLogDbConf &conf) {
    namespace po = boost::program_options;
    using boost::program_options::value;

    po::options_description desc("LogDb: save and fetch zp's bg_action with low latency", 200);
    std::string logfile;
    std::string pid;
    desc.add_options()
            ("help,h", "Display a help message and exit.")
            ("loglevel", value<int>(&conf.verbosity)->default_value(2),
             "Can be 0(trace), 1(debug), 2(info), 3(warn), 4(error), 5(fatal)")
            ("logfile", value<std::string>(&logfile)->default_value("stdout"), "Log file, can be stdout")
            ("pidfile", value<std::string>(&pid)->default_value("pid"), "File to store pid")
            ("cache", value<size_t>(&conf.cache_size)->default_value(1024), "Cache size in megabytes")
            ("threads,t", value<size_t>(&conf.threads)->default_value(20), "Threads count")
            ("max_val", value<size_t>(&conf.max_val)->default_value(1048576), "Max val of a key, in bytes")
            ("port,p", value<int>(&conf.port)->default_value(2348), "Port to listen to")
            ("dir", value<std::string>(&conf.db_dir)->default_value("/tmp/data"), "Data dir");

    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);
    po::notify(vm);

    if (vm.count("help")) {
        std::cout << desc << "\n";
        exit(1);
    }

    pid_t p = getpid();
    auto pf = fopen(pid.c_str(), "w");
    fprintf(pf, "%d", p);
    fflush(pf);
    fclose(pf);

    signal(SIGUSR1, reopen_log);

    listdb::log_open(logfile, conf.verbosity);
    listdb::log_info("log to file: %s, verbosity: %d, max val %d", logfile.data(), conf.verbosity, conf.max_val);
}


void start_thrift_server(ZpLogDbHandler &h, ZpLogDbConf &conf) {
    shared_ptr<ZpLogDbHandler> handler(&h);
    shared_ptr<TProcessor> processor(new logdb_apiProcessor(handler));
    shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());

    shared_ptr<ThreadManager> threadManager = ThreadManager::newSimpleThreadManager(conf.threads);
    shared_ptr<PlatformThreadFactory> threadFactory = shared_ptr<PlatformThreadFactory>(new PlatformThreadFactory());
    threadManager->threadFactory(threadFactory);
    threadManager->start();
#ifdef OS_MACOSX
    shared_ptr<TServerTransport> serverTransport(new TServerSocket(conf.port));
    shared_ptr<TTransportFactory> transportFactory(new TFramedTransportFactory());
    TThreadPoolServer s(processor, serverTransport, transportFactory, protocolFactory, threadManager);
#else
    TNonblockingServer s(processor, protocolFactory, conf.port, threadManager);
#endif

    listdb::log_info("thrift server started, port: %d, threads: %d", conf.port, conf.threads);
    s.serve();
}


int main(int argc, char **argv) {
    std::srand(std::time(NULL));
    ZpLogDbConf conf;
    parse_args(argv, argc, conf);

    ZpLogDbHandler handler(conf);
    if (handler.Open() == 0) {
        start_thrift_server(handler, conf);
    }

    return 0;
}
