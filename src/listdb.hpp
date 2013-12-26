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
#include "util/coding.h"
#include "rocksdb/merge_operator.h"
#include "config.hpp"
#include "redis_proto.hpp"
#include <exception>
#include <mutex>
#include <deque>
#include <algorithm>

void readQueryFromClient(aeEventLoop *el, int fd, void *privdata, int mask);
void acceptTcpHandler(aeEventLoop *el, int fd, void *privdata, int mask);
void sendReplyToClient(aeEventLoop *el, int fd, void *privdata, int mask);

static rocksdb::Status to_list(const std::string& value,
                               std::vector<rocksdb::Slice> *elements,
                               int start, int stop) {
    auto p = value.data(), end = value.data() + value.size();
    uint32_t n = 0;
    while( p != end) {
        uint32_t size = 0;
        p = rocksdb::GetVarint32Ptr(p, p + 5, &size); // TODO corruption?
        n += 1;
        p += size;
    }

    while(stop < 0)  stop += (int)n;
    while(start < 0) start += (int)n;

    // uint32_t ustop = uint32_t(stop); // get rid of warning by converting to unsigned
    // while(ustop >= n) ustop -= n;

    //     cout << start << "\t" << ustop << "\t" << n << "\t" << stop << endl;
    if (start > stop) return rocksdb::Status::OK(); // empty

    elements->reserve(stop - start + 1);
    p = value.data();
    end = value.data() + value.size();

    for(int i = 0; p != end; i++) {
        if (i < start) continue;
        if (i > stop) break;

        uint32_t size = 0;
        p = rocksdb::GetVarint32Ptr(p, p + 5, &size);
        elements->emplace_back(p, size); // reuse value's memory
        p += size;
    }

    return rocksdb::Status::OK();
}

class RedisList {
private:
    rocksdb::DB* db;
    const rocksdb::WriteOptions woption;
    const rocksdb::ReadOptions roption;

public:
    RedisList(rocksdb::DB* db): db(db) {
        // woption.disableWAL = true;
    }

    rocksdb::Status Rpush(const rocksdb::Slice& key, const rocksdb::Slice& value) {
        return db->Merge(woption, key, value);
    }

    rocksdb::Status Lrange(const rocksdb::Slice& key,
                           std::string *scratch,  // value
                           std::vector<rocksdb::Slice> *elements,
                           int start,
                           int stop) {
        elements->clear();
        rocksdb::Status s = db->Get(roption, key, scratch);
        if (s.ok()) {
            to_list(*scratch, elements, start, stop);
        }
        return s;
    }
};
class RedisClient;

typedef void redisCommandProc(RedisClient *c, std::unique_ptr<RedisRequest> &&req);
void getCommand(RedisClient *c, std::unique_ptr<RedisRequest> &&req);
void setCommand(RedisClient *c, std::unique_ptr<RedisRequest> &&req);
void rpushCommand(RedisClient *c, std::unique_ptr<RedisRequest> &&req);
void lrangeCommand(RedisClient *c, std::unique_ptr<RedisRequest> &&req);

struct redisCommand {
    std::string name;
    redisCommandProc *proc;
    int arity;
    std::string sflags; /* Flags as string representation, one char per flag. */
    int flags;    /* The actual flags, obtained from the 'sflags' field. */
    /* Use a function to determine keys arguments in a command line.
     * Used for Redis Cluster redirect. */
    /* What keys should be loaded in background when calling this command? */
    int firstkey; /* The first argument that's a key (0 = no keys) */
    int lastkey;  /* The last argument that's a key */
    int keystep;  /* The step between first and last key */
    long long microseconds, calls;
};


class ListDbServer {
private:
    std::unordered_map<std::string, redisCommand*> table_;
public:
    ListDbServer();
    void Handle(RedisClient *c, std::unique_ptr<RedisRequest> &&req);
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
        if (nwritten <= 0) throw IOException("written size < 0");
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

    int ReadAndHandle() {
        readBuffer.Clear(); // clear for reading
        int nread = readBuffer.Read(this->fd);
        if (nread <= 0) return nread;
        else if(nread > 0) {
            readBuffer.Flip();
            while(readBuffer.HasRemaining()) {
                auto request = decoder.Decode(readBuffer);
                if (request) {
                    server->Handle(this, std::unique_ptr<RedisRequest>(request));
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

class ListDBMergeOperator: public rocksdb::MergeOperator {
public:
    virtual ~ListDBMergeOperator(){}

    // Gives the client a way to express the read -> modify -> write semantics
    // key:         (IN) The key that's associated with this merge operation.
    // existing:    (IN) null indicates that the key does not exist before this op
    // operand_list:(IN) the sequence of merge operations to apply, front() first.
    // new_value:  (OUT) Client is responsible for filling the merge result here
    // logger:      (IN) Client could use this to log errors during merge.
    //
    // Return true on success. Return false failure / error / corruption.
    bool FullMerge(const rocksdb::Slice& key,
                   const rocksdb::Slice* existing_value,
                   const std::deque<std::string>& operand_list,
                   std::string* new_value,
                   rocksdb::Logger* logger) const override {

        // Clear the *new_value for writing.
        assert(new_value);
        new_value->clear();

        // Compute the space needed for the final result.
        int numBytes = 0;
        for (auto &s : operand_list) {
            numBytes += s.size() + rocksdb::VarintLength(s.size());
        }

        if (existing_value) {
            new_value->reserve(numBytes + existing_value->size());
            new_value->append(existing_value->data(), existing_value->size());
        } else {
            new_value->reserve(numBytes);
        }

        for (auto &s: operand_list) {
            // encode list item as: size + data
            rocksdb::PutVarint32(new_value, s.size()); // put size
            new_value->append(s);
        }

        return true;
    }

    // This function performs merge(left_op, right_op)
    // when both the operands are themselves merge operation types.
    // Save the result in *new_value and return true. If it is impossible
    // or infeasible to combine the two operations, return false instead.
    bool PartialMerge(const rocksdb::Slice& key,
                      const rocksdb::Slice& left_operand,
                      const rocksdb::Slice& right_operand,
                      std::string* new_value,
                      rocksdb::Logger* logger) const override {
        return false;
    }

    // The name of the MergeOperator. Used to check for MergeOperator
    // mismatches (i.e., a DB created with one MergeOperator is accessed
    // using a different MergeOperator)
    const char* Name() const {
        return "ListMergeOperator";
    }
};


#endif /* _LISTDB_H_ */
