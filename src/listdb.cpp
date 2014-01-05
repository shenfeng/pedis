#include "listdb.hpp"

void readQueryFromClient(aeEventLoop *el, int fd, void *privdata, int mask) {
    PedisClient *c = (PedisClient*)privdata;
    try {
        if(c->ReadAndHandle() < 0) {
            delete c; // normal clode
        }
    } catch (ProtocolException &e) {
        printf("error: %s\n", e.what());
        delete c;
    } catch (IOException &e) {
        printf("error: %s\n", e.what());
        delete c;
    }
}

void acceptTcpHandler(aeEventLoop *el, int fd, void *privdata, int mask) {
    int cport, cfd;
    char cip[128];
    char neterr[ANET_ERR_LEN];
    cfd = anetTcpAccept(neterr, fd, cip, sizeof(cip), &cport);
    if (cfd == AE_ERR) {
        return;
    }

    PedisClient *c = new PedisClient(cfd, (PedisServer*)privdata);
    anetEnableTcpNoDelay(NULL, cfd);
    if (aeCreateFileEvent(el, cfd, AE_READABLE,
                          readQueryFromClient, c) == AE_ERR) {
        delete c;
    }
}

void sendReplyToClient(aeEventLoop *el, int fd, void *privdata, int mask) {
    PedisClient *c = (PedisClient*) privdata;
    try {
        c->Write();
    } catch (IOException &e) {
        printf("error: %s\n", e.what());
        delete c;
    }
}

// read buffer, shared across all socket
ByteBuffer PedisClient::readBuffer(1024 * 64);

void PedisClient::Get(rocksdb::DB* db, std::unique_ptr<RedisRequest> &&req) {

}

void PedisClient::Set(rocksdb::DB* db, std::unique_ptr<RedisRequest> &&req) {

}

void PedisClient::Rpush(rocksdb::DB* db, std::unique_ptr<RedisRequest> &&req) {

}

void PedisClient::Lrange(rocksdb::DB* db, std::unique_ptr<RedisRequest> &&req) {

}

void PedisClient::Select(rocksdb::DB* db, std::unique_ptr<RedisRequest> &&req) {

}

PedisClient::~PedisClient() {
    aeDeleteFileEvent(el, this->fd, AE_READABLE | AE_WRITABLE);
    close(this->fd);
    free(this->wbuf);
}

static RedisCommand redisCommandTable[] = {
    {"GET",&PedisClient::Get,2,"r",0,1,1,1,0,0},
    {"SET",&PedisClient::Set,-3,"wm",0,1,1,1,0,0},
    {"RPUSH",&PedisClient::Rpush,-3,"wm",0,1,1,1,0,0},
    {"SELECT",&PedisClient::Select,2,"rl",0,0,0,0,0,0},
    {"LRANGE",&PedisClient::Lrange,4,"r",0,1,1,1,0,0}
};

PedisServer::PedisServer() {
    int numcommands = sizeof(redisCommandTable)/sizeof(RedisCommand);
    for (int i = 0; i < numcommands; i++) {
        RedisCommand *c = redisCommandTable + i;
        const char *f = c->sflags.data();

        while(*f != '\0') {
            switch(*f) {
            case 'w': c->flags |= REDIS_CMD_WRITE; break;
            case 'r': c->flags |= REDIS_CMD_READONLY; break;
            case 'm': c->flags |= REDIS_CMD_DENYOOM; break;
            case 'n': c->flags |= REDIS_CMD_NETWORK_THREAD; break;
            case 'a': c->flags |= REDIS_CMD_ADMIN; break;
            case 'p': c->flags |= REDIS_CMD_PUBSUB; break;
            case 's': c->flags |= REDIS_CMD_NOSCRIPT; break;
            case 'R': c->flags |= REDIS_CMD_RANDOM; break;
            case 'S': c->flags |= REDIS_CMD_SORT_FOR_SCRIPT; break;
            case 'l': c->flags |= REDIS_CMD_LOADING; break;
            case 't': c->flags |= REDIS_CMD_STALE; break;
            case 'M': c->flags |= REDIS_CMD_SKIP_MONITOR; break;
            case 'k': c->flags |= REDIS_CMD_ASKING; break;
            default: throw "error";
            }
            f++;
        }
        // convert to upper case
        std::transform(c->name.begin(), c->name.end(), c->name.begin(), ::toupper);
        this->_table[c->name] = c;
    }

    this->el = aeCreateEventLoop(1024);
}

void PedisServer::Handle(PedisClient *c, std::unique_ptr<RedisRequest> &&req) {
    auto &name = req->Args[0].Value;
    // convert to upper case
    std::transform(name.begin(), name.end(), name.begin(), ::toupper);
    char buf[100];

    auto it = _table.find(name);
    if (it != _table.end()) {
        RedisCommand *cmd = it->second;
        if ((cmd->arity > 0 && cmd->arity != req->Args.size()) ||
            (-cmd->arity > -req->Args.size())) {
            int n = snprintf(buf, sizeof(buf),
                             "-ERR wrong number of arguments for '%s' command\r\n",
                             name.data());
            c->Raw(buf, n);
        }
        auto db = this->GetDb(c->db_idx); // fetch in the network thread
        if (cmd->flags & REDIS_CMD_NETWORK_THREAD) {
            (c->*(it->second->proc))(db, std::move(req));
        } else if (cmd->flags & REDIS_CMD_WRITE) {
            // TODO, move to dedicated thread
            (c->*(it->second->proc))(db, std::move(req));
        } else {
            // TODO, move to threadpool
            (c->*(it->second->proc))(db, std::move(req));
        }
    } else {
        int n = snprintf(buf, sizeof(buf), "-ERR unknow command: %s\r\n", name.data());
        c->Raw(buf, n);
    }
}
