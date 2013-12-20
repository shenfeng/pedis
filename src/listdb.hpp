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

// Like java's ByteBuffer
class ByteBuffer {
    unsigned int position;
    unsigned int limit;
    unsigned int capacity;
    char *buf;
public:
    ByteBuffer(unsigned int capacity):
        position(0), limit(0), capacity(capacity), buf(new char[capacity]) {}
    ~ByteBuffer() { delete []buf; }

    unsigned int Capacity() const { return capacity; }
    bool HasRemaining() const { return position < limit; }
    unsigned int Remaining() const { return limit - position; }
    char Get() { return buf[++position]; }
    ByteBuffer &Get(char *dest,  int size) {
        memcpy(dest, buf + position, size);
        position += size;
        return *this;
    }

    int Read(int fd) {

    }
};

class RedisArg {
    unsigned int size;
    unsigned int position;
    char *buf_;
    char buffer[16]; // avoid allocate on the stack

public:
    explicit RedisArg(unsigned int size): size(size), position(0), buf_(nullptr) {
        if (size > sizeof(buffer)) {
            buf_ = (char *)malloc(size);
        } else {
            buf_ = buffer;
        }
    }

    bool Read(ByteBuffer &buffer) { // return true iff this arg get all read
        int toRead = std::min(size - position, buffer.Remaining());
        buffer.Get(buf_ + position, toRead);
        position += toRead;
        return position == size;
    }

    RedisArg& operator=(RedisArg&& other) noexcept {
        if(this != &other) {
            if (other.size > sizeof(buffer)) {
                delete buf_;
                buf_ = other.buf_;
                other.buf_ = nullptr;
            } else {
                memcpy(buffer, other.buffer, other.size);
            }
            size = other.size;
        }
        return *this;
    }

    ~RedisArg() {
        if (buf_ && buf_ != buffer) { delete buf_; }
    }
};

class LineReader {
    char buf[16]; // 16 is enough, for reading argument size, byte count
    int lineBufferIdx;
public:
    LineReader(): lineBufferIdx(0) {}

    const char *readLine(ByteBuffer &buffer) {
        while (buffer.HasRemaining()) {
            char c = buffer.Get();
            buf[lineBufferIdx++] = c;
            if (c == '\n')
                return buf;
        }
        return nullptr;
    }

    int LineSize() const { return lineBufferIdx; }

    void reset() { this->lineBufferIdx = 0; }
};

class RedisRequest {
    int argc;
    int argRead;
    std::vector<RedisArg> args;

public:
    RedisRequest(int argc): argc(argc). argRead(0) {}
    bool decode(ByteBuffer &buffer, LineReader *lineReader) {
        while (buffer.HasRemaining()) {

        }
    }
};

class RedisDecoder {
    enum {
        sReadArgCount, sReadArgLen, sReadArg, sAllRead
    };

    int state;
    LineReader linereader;
    std::shared_ptr<RedisRequest> request;

public:
    RedisDecoder(): state(sReadArgCount) { }

    std::shared_ptr<RedisRequest> decode(ByteBuffer &buffer) {
        switch (state) {
        case sReadArgCount:


        }
    }
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
