#ifndef _CONFIG_H_
#define _CONFIG_H_

#include <memory>

#define OUTPUT_BUFFER_SIZE (64*1024) /* 64k output buffer */


struct ServerConf {
    int port;
    int dbsize;

    std::shared_ptr<rocksdb::Logger> logger;

    ServerConf() {
        this->port = 7389;
        this->dbsize = 4;
        // logger = new
    }
};


#endif /* _CONFIG_H_ */
