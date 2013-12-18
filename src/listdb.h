#ifndef _LISTDB_H_
#define _LISTDB_H_

#include <stddef.h>
#include <sys/stat.h>
extern "C" {
#include "ae/anet.h"
#include "ae/anet.h"
}
#include "rocksdb/db.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/cache.h"
#include "rocksdb/env.h"
#include "rocksdb/merge_operator.h"
#include "config.h"

class Server {
    private:
        int port;
        int listen_fd;
    public:
        Server(int port): port(port) {


        }

        int Start() {
            char err[ANET_ERR_LEN];
            this->listen_fd = anetTcpServer(err, this->port, "0.0.0.0");
            if (this->listen_fd < 0) {

            }
        }
};

class Client {
    int fd;

    /* Response buffer */
    int bufpos;
    char buf[OUTPUT_BUFFER_SIZE];
};



#endif /* _LISTDB_H_ */
