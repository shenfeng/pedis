#ifndef _LISTDB_H_
#define _LISTDB_H_

#include <stddef.h>
#include <unistd.h>
#include <stdio.h>
#include <sys/stat.h>
#include <errno.h>
#include <cstdlib>
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
#include "redis_proto.hpp"
#include <exception>
#include <mutex>
#include <deque>

void readQueryFromClient(aeEventLoop *el, int fd, void *privdata, int mask);
void acceptTcpHandler(aeEventLoop *el, int fd, void *privdata, int mask);
void sendReplyToClient(aeEventLoop *el, int fd, void *privdata, int mask);

class RedisClient;
class ListDbServer {
public:
    ListDbServer() {}
    void HandleRequest(RedisClient *c, std::unique_ptr<RedisRequest> &&req);
};

class RedisClient {
    int fd;
    RedisDecoder decoder;
    ListDbServer *server;

    // response
    size_t bufpos;
    size_t sentlen;
    size_t capacity;
    char *wbuf;
    std::mutex writeLock;

    static ByteBuffer readBuffer; // request. shared, access by IO thread

    inline int _write() { // with writeLock held required
        extern struct ServerConf G_server;

        int nwritten = write(fd, wbuf + sentlen, bufpos - sentlen);
        if (nwritten <= 0) return nwritten;
        sentlen += nwritten;

        if (sentlen == bufpos) bufpos = sentlen = 0;
        return nwritten;
    }

    inline int _tryWrite() { // from worker thread
        extern struct ServerConf G_server;
        int nwritten = _write();
        if (bufpos > 0) { // not all bytes written to the TCP buffer, wait for notify
            aeCreateFileEvent(G_server.el, this->fd, AE_WRITABLE, sendReplyToClient, this);
        }
        return nwritten;
    }

    inline void _reserveSpace(size_t want) { // wirte writeLock help required
        if (capacity - bufpos < want) { //  enlarge output buffer
            capacity = std::max(capacity * 2, bufpos + want);
            char *tmp = (char* )malloc(capacity);
            memcpy(tmp, wbuf, bufpos - sentlen); // copy unsended data
            bufpos = bufpos - sentlen;
            sentlen = 0;
            free(wbuf);
            wbuf = tmp;
        }
    }

public:
    RedisClient(int fd, ListDbServer *server): fd(fd), server(server) {
        bufpos = sentlen = 0;
        capacity = 64 * 1024;
        wbuf = (char*)malloc(capacity);
    }

    ~RedisClient();

    int ReadQuery() {
        readBuffer.Clear(); // clear for reading
        int nread = readBuffer.Read(this->fd);
        readBuffer.Flip();
        if (nread < 0) return nread;
        else if(nread > 0) {
            while(readBuffer.HasRemaining()) {
                auto request = decoder.Decode(readBuffer);
                if (request) {
                    server->HandleRequest(this, std::unique_ptr<RedisRequest>(request));
                    decoder.Reset();
                }
            }
        }
        return nread;
    }

    int Raw(const char* res, size_t size) {
        std::lock_guard<std::mutex> _(writeLock);
        _reserveSpace(size);
        memcpy(wbuf + bufpos, res, size);
        bufpos += size;
        return _tryWrite();
    }

    int Bulk(const std::string &value) {
        std::lock_guard<std::mutex> _(writeLock);
        _reserveSpace(value.size() + 16);
        bufpos += snprintf(wbuf + bufpos, 16, "$%lu\r\n", value.size());
        memcpy(wbuf + bufpos, value.data(), value.size());
        bufpos += value.size();
        wbuf[bufpos++] = '\r';
        wbuf[bufpos++] = '\n';
        return _tryWrite();
    }

    int Write() { // execute in IO thread
        extern struct ServerConf G_server;

        std::lock_guard<std::mutex> _(writeLock);
        int nwritten = _write();
        if (bufpos == 0) {
             aeDeleteFileEvent(G_server.el, this->fd, AE_WRITABLE);
        }
        return nwritten;
    }
};



#endif /* _LISTDB_H_ */
