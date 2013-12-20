#ifndef _LISTDB_H_
#define _LISTDB_H_

#include <stddef.h>
#include <unistd.h>
#include <sys/stat.h>
extern "C" {
#include "anet.h"
#include "ae.h"
}
#include "rocksdb/db.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/cache.h"
#include "rocksdb/env.h"
#include "rocksdb/merge_operator.h"
#include "config.hpp"

class Server {
private:
    int listen_fd;
    aeEventLoop *el;
public:
    Server(int port): el(aeCreateEventLoop(1024)) { }
    int Start();
};

struct Client {
    int fd;
    int reqtype;

    /* */
    int rbufbegin;
    int rbufpos;
    int rbufsize;
    char *rbuf;


    /* Response buffer */
    int wbufpos;
    int wbuflimit;
    char wbuf[OUTPUT_BUFFER_SIZE];
public:
    Client(int fd): fd(fd) {
        reqtype = -1;
        rbufsize = OUTPUT_BUFFER_SIZE;
        rbuf = new char[rbufsize];

        rbufbegin = rbufpos = wbufpos = wbuflimit = 0;
    }

    ~Client() {
        delete []rbuf;
    }
};



#endif /* _LISTDB_H_ */
