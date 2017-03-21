#include <boost/program_options.hpp>
#include <thrift/concurrency/ThreadManager.h>
#include <thrift/server/TThreadPoolServer.h>
#include <thrift/transport/TServerSocket.h>
#include <concurrency/PlatformThreadFactory.h>
#include <thrift/transport/TBufferTransports.h> // TFramedTransportFactory


#include <iostream>
#include "logger.hpp"

#include "Listdb.h"
#include "listdb_server.h"


class ListdbHandler : virtual public ListdbIf {
public:
    ListdbHandler() {
        // Your initialization goes here
    }

    void Push(const PushArg &arg) {
        // Your implementation goes here
        printf("Push\n");
    }

    void Range(std::vector<std::string> &_return, const RangeArg &arg) {
        // Your implementation goes here
        printf("Range\n");
    }
};

void parse_args(const char **argv, int argc, ListServerConf &conf) {
    namespace po = boost::program_options;
    using boost::program_options::value;

    po::options_description desc("Auto complete server, allowed options", 200);
    std::string logfile;
    desc.add_options()
            ("help,h", "Display a help message and exit.")
            ("loglevel", value<int>(&conf.verbosity)->default_value(2), "Can be 0(debug), 1(verbose), 2(notice), 3(warnning)")
            ("logfile", value<std::string>(&logfile)->default_value("stdout"), "Log file, can be stdout")
            ("threads,t", value<size_t>(&conf.threads)->default_value(8), "Threads count")
            ("port,p", value<int>(&conf.port)->default_value(8912), "Port to listen for thrift connection");

    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);
    po::notify(vm);

    if (vm.count("help")) {
        std::cout << desc << "\n";
        exit(1);
    }
    listdb::log_open(logfile, conf.verbosity, 0);
    listdb::log_info("log to file: %s, verbosity: %d", logfile.data(), conf.verbosity);
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
    shared_ptr<TServerTransport> serverTransport(new TServerSocket(conf.port));
    shared_ptr<TTransportFactory> transportFactory(new TFramedTransportFactory());

    shared_ptr<ThreadManager> threadManager = ThreadManager::newSimpleThreadManager(conf.threads);
    shared_ptr<PlatformThreadFactory> threadFactory = shared_ptr<PlatformThreadFactory>(new PlatformThreadFactory());
    threadManager->threadFactory(threadFactory);
    threadManager->start();

    TThreadPoolServer s(processor, serverTransport, transportFactory, protocolFactory, threadManager);
    listdb::log_info("thrift server started, port: %d, threads: %d", conf.port, conf.threads);
    s.serve();
}


int main() {
    std::cout << "Hello, World!" << std::endl;
    return 0;
}