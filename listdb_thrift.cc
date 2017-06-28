#include <boost/program_options.hpp>
#include <thrift/concurrency/ThreadManager.h>
#include <thrift/server/TThreadPoolServer.h>
#include <thrift/transport/TServerSocket.h>
#include <concurrency/PlatformThreadFactory.h>
#include <thrift/transport/TBufferTransports.h> // TFramedTransportFactory

#ifndef OS_MACOSX
#include <thrift/server/TNonblockingServer.h>
#endif

#include <iostream>
#include "logger.hpp"

#include "Listdb.h"
#include "listdb_server.h"


class ListdbHandler : virtual public ListdbIf {
private:
    ListDb *mDb;
public:
    ListdbHandler(ListDb *db) : mDb(db) {}

    void Push(const PushArg &arg) {
        ListPushArg a(arg.db, arg.key, arg.datas);
        mDb->Push(a);
    }

    void Pushs(const std::vector<PushArg> & arg) {
        for(int i = 0; i< arg.size(); i++) {
            ListPushArg a(arg[i].db, arg[i].key, arg[i].datas);
            mDb->Push(a);
        }
    }

    void Delete(const std::string &key, const int32_t db) {
        mDb->Delete(key, db);
    }

    void Range(std::vector<std::string> &_return, const RangeArg &arg) {
        ListRangeArg a(arg.db, arg.start, arg.last, arg.key);
        mDb->LRange(a, _return);
    }

    void Ranges(std::vector<std::vector<std::string> > & _return, const std::vector<RangeArg> & arg) {
        _return.resize(arg.size());
        for(int i = 0; i < arg.size(); i++) {
            ListRangeArg a(arg[i].db, arg[i].start, arg[i].last, arg[i].key);
            mDb->LRange(a, _return[i]);
        }
    }

    void Scan(ScanResp &_return, const ScanArg &arg) {
        std::vector<std::tuple<std::string, int>> keys_out;
        ListScanArg a(arg.db, arg.limit, arg.cursor);
        mDb->Scan(a, _return.cursor, keys_out);
        _return.keys.reserve(keys_out.size());

        for (auto &k: keys_out) {
            ScanItem item;
            item.key = std::get<0>(k);
            if (std::get<1>(k) == kTypeList) {
                item.type = KeyType::ListType;
            } else if (std::get<1>(k) == kTypeKey) {
                item.type = KeyType::StringType;
            }
            _return.keys.push_back(item);
        }
    }
};

void parse_args(char **argv, int argc, ListServerConf &conf) {
    namespace po = boost::program_options;
    using boost::program_options::value;

    po::options_description desc("Auto complete server, allowed options", 200);
    std::string logfile;
    desc.add_options()
            ("help,h", "Display a help message and exit.")
            ("loglevel", value<int>(&conf.verbosity)->default_value(2), "Can be 0(trace), 1(debug), 2(info), 3(warn), 4(error), 5(fatal)")
            ("logfile", value<std::string>(&logfile)->default_value("stdout"), "Log file, can be stdout")
            ("cache", value<size_t>(&conf.cache_size)->default_value(1024), "Cache size in megabytes")
            ("max_val", value<size_t>(&conf.max_val)->default_value(0), "Max size of value, default 0 unlimited")
            ("threads,t", value<size_t>(&conf.threads)->default_value(8), "Threads count")
            ("port,p", value<int>(&conf.port)->default_value(6571), "Port to listen to")
            ("dir", value<std::string>(&conf.db_dir)->default_value("/tmp/data"), "Data dir")
            ("db", value<int>(&conf.db_count)->default_value(8), "Database count");

    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);
    po::notify(vm);

    if (vm.count("help")) {
        std::cout << desc << "\n";
        exit(1);
    }
    listdb::log_open(logfile, conf.verbosity, 0);
    listdb::log_info("log to file: %s, verbosity: %d, max_val %d bytes", logfile.data(), conf.verbosity, conf.max_val);
}


void start_thrift_server(ListdbHandler &h, ListServerConf &conf) {
    using boost::shared_ptr;
    using namespace apache::thrift;
    using namespace apache::thrift::protocol;
    using namespace apache::thrift::transport;
    using namespace apache::thrift::server;
    using namespace apache::thrift::concurrency;

    shared_ptr<ListdbHandler> handler(&h);
    shared_ptr<TProcessor> processor(new ListdbProcessor(handler));
    shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());

    shared_ptr<ThreadManager> threadManager = ThreadManager::newSimpleThreadManager(conf.threads);
    shared_ptr<PlatformThreadFactory> threadFactory = shared_ptr<PlatformThreadFactory>(new PlatformThreadFactory());
    threadManager->threadFactory(threadFactory);
    threadManager->start();
#ifdef OS_MACOSX
    shared_ptr <TServerTransport> serverTransport(new TServerSocket(conf.port));
    shared_ptr <TTransportFactory> transportFactory(new TFramedTransportFactory());
    TThreadPoolServer s(processor, serverTransport, transportFactory, protocolFactory, threadManager);
#else
    TNonblockingServer s(processor, protocolFactory, conf.port, threadManager);
#endif

    listdb::log_info("thrift server started, port: %d, threads: %d", conf.port, conf.threads);
    s.serve();
}


int main(int argc, char **argv) {
    ListServerConf conf;
    parse_args(argv, argc, conf);

    ListDb db(conf);
    if (db.Open() == 0) {
        ListdbHandler h(&db);
        start_thrift_server(h, conf);
    }

    return 0;
}