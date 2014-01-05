#ifndef _CONFIG_H_
#define _CONFIG_H_

#include <memory>
#include "ae.h"

#define OUTPUT_BUFFER_SIZE (64*1024) /* 64k output buffer */


struct ServerConf {
    int port;
    int dbsize;
    char neterr[ANET_ERR_LEN];

    std::shared_ptr<rocksdb::Logger> logger;
    aeEventLoop *el;

    ServerConf() {
        this->port = 7389;
        this->dbsize = 4;
        // this->el = aeCreateEventLoop(1024);
    }
};

// http://google-styleguide.googlecode.com/svn/trunk/cppguide.xml
// A macro to disallow the copy constructor and operator= functions
// This should be used in the private: declarations for a class
#define DISALLOW_COPY_AND_ASSIGN(TypeName) \
  TypeName(const TypeName&);               \
  void operator=(const TypeName&)

#endif /* _CONFIG_H_ */
