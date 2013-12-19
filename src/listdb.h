#ifndef _LISTDB_H_
#define _LISTDB_H_

#include <stddef.h>
#include <sys/stat.h>
extern "C" {
#include "ae/anet.h"
#include "ae/ae.h"
}
#include "rocksdb/db.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/cache.h"
#include "rocksdb/env.h"
#include "rocksdb/merge_operator.h"
#include "config.h"

class Server {
    private:
        int listen_fd;
        aeEventLoop *el;
    public:
        Server(int port): el(aeCreateEventLoop(1024)) { }
        int Start();
};

class Client {
    int fd;

    /* Response buffer */
    int bufpos;
    char buf[OUTPUT_BUFFER_SIZE];
};



#endif /* _LISTDB_H_ */
