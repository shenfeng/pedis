#ifndef _REDIS_PROTO_H_
#define _REDIS_PROTO_H_

class ProtocolException :public std::exception {
private:
    const std::string msg;
public:
    ProtocolException(const std::string &msg): msg(msg) {
    }
    virtual const char* what() const throw() {
        return msg.data();
    }
};

class IOException :public std::exception {
private:
    const std::string msg;
public:
    IOException(const std::string &msg): msg(msg) {
    }
    virtual const char* what() const throw() {
        return msg.data();
    }
};

// Like java's ByteBuffer
class ByteBuffer {
    size_t position;
    size_t limit;
    size_t capacity;
    char *buf;
public:
    ByteBuffer(unsigned int capacity): position(0), limit(capacity), capacity(capacity) {
        buf = (char *)malloc(capacity);
    }
    ~ByteBuffer() { free(buf); }

    bool HasRemaining() const { return position < limit; }
    size_t Remaining() const { return limit - position; }

    char Get() { return buf[position++]; }
    void Get(std::string *dest,  int size) {
        dest->append(buf + position, size);
        position += size;
    }

    void Clear() { position = 0; limit = capacity; }
    void Flip() { limit = position; position = 0; }

    int Read(int fd) { // like java's SocketChannel.read()
        int nread = read(fd, buf + position, Remaining());
        if (nread < 0) {
            if (errno == EAGAIN) {
                nread = 0;
            } else {
                throw IOException("read error");
            }
        } else if (nread == 0) {
            nread = -1; // normal close
        }
        if (nread > 0) {
            position += nread;
        }
        return nread;
    }
};

struct RedisArg {
    unsigned int Size;
    std::string Value;
    enum { sReadData, sReadCR, sReadLF, sArgDone };
    int state;

public:
    explicit RedisArg(unsigned int size): Size(size), state(sReadData) {
        Value.reserve(size);
    }

    bool Read(ByteBuffer &buffer) { // return true iff this arg get all read
        int toRead;
        while(buffer.HasRemaining() && state != sArgDone) {
            switch(state) {
                case sReadData:
                    toRead = std::min(Size - Value.size(), buffer.Remaining());
                    buffer.Get(&Value, toRead);
                    if (Value.size() == Size)
                        state = sReadCR;
                    break;
                case sReadCR: // read \r
                    buffer.Get();
                    state = sReadLF;
                    break;
                case sReadLF: // read \n
                    buffer.Get();
                    state = sArgDone;
                    break;
            }
        }
        return state == sArgDone;
    }

};

class LineReader {
    char buf[16]; // 16 is enough, for reading argument count, byte count
    int lineBufferIdx;
public:
    LineReader(): lineBufferIdx(0) {}

    const char *ReadLine(ByteBuffer &buffer) {
        while (buffer.HasRemaining()) {
            char c = buffer.Get();
            if (lineBufferIdx >= 14) {
                throw ProtocolException("too large size");
            }
            buf[lineBufferIdx++] = c;
            if (c == '\n') {
                buf[lineBufferIdx + 1] = '\0';
                lineBufferIdx = 0; // reset
                return buf;
            }
        }
        return nullptr;
    }
};

struct RedisRequest {
    public:
        std::vector<RedisArg> Args;
        RedisRequest(int argc): argc(argc), argReading(0) {}

        void Add(int size) { Args.emplace_back(size); ++argReading; }
        // return true iff the whole requet decoded
        bool Done() { return argReading == argc; }
        bool ReadArg(ByteBuffer &buffer) { return Args[argReading - 1].Read(buffer); }
    private:
        const int argc;
        int argReading;
};

class RedisDecoder {
    enum { sReadArgCount, sReadArgLen, sReadArg, sAllRead };
    int state;  // state machine

    LineReader lineReader; // a complete line may not receied yet, do the buffering needed
    RedisRequest *request;

public:
    RedisDecoder(): state(sReadArgCount) { }

    // return the request iff a complete request is the buffer, else return null
    RedisRequest* Decode(ByteBuffer &buffer) {
        const char *line;
        while(buffer.HasRemaining() && state != sAllRead) {
            switch (state) {
            case sReadArgCount:
                if (buffer.Get() != '*') {
                    throw ProtocolException("* expected");
                }

                if ((line = lineReader.ReadLine(buffer)) != nullptr) {
                    int argc = std::atoi(line);
                    if (argc >= 1) {
                        request = new RedisRequest(argc);
                        state = sReadArgLen;
                    } else {
                        throw ProtocolException("wrong argc");
                    }
                }
                break;
            case sReadArgLen:
                if (buffer.Get() != '$')
                    throw ProtocolException("$ expected");

                if ((line = lineReader.ReadLine(buffer)) != nullptr) {
                    int size = std::atoi(line);
                    if (size > 0 && size < 8 * 1024 * 1024) { // TODO max arg 8M configurable
                        request->Add(size);
                        state = sReadArg;
                    } else {
                        throw ProtocolException("size out of range");
                    }
                }
                break;
            case sReadArg:
                if (request->ReadArg(buffer)) {
                    state = request->Done() ? sAllRead : sReadArgLen;
                }
                break;
            }
        }
        return state == sAllRead ? request : nullptr;
    }

    void Reset() { state = sReadArgCount; }
};

#endif /* _REDIS_PROTO_H_ */
