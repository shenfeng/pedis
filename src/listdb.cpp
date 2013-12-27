#include "listdb.hpp"

struct ServerConf G_server; // /* server global state */

void readQueryFromClient(aeEventLoop *el, int fd, void *privdata, int mask) {
    RedisClient *c = (RedisClient*)privdata;
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
    cfd = anetTcpAccept(G_server.neterr, fd, cip, sizeof(cip), &cport);
    if (cfd == AE_ERR) {
        return;
    }

    RedisClient *c = new RedisClient(cfd, (ListDbServer*)privdata);
    anetEnableTcpNoDelay(NULL, cfd);
    if (aeCreateFileEvent(G_server.el, cfd, AE_READABLE,
                          readQueryFromClient, c) == AE_ERR) {
        delete c;
    }
}

void sendReplyToClient(aeEventLoop *el, int fd, void *privdata, int mask) {
    RedisClient *c = (RedisClient*) privdata;
    try {
        c->Write();
    } catch (IOException &e) {
        printf("error: %s\n", e.what());
        delete c;
    }
}

// read buffer, shared across all socket
ByteBuffer RedisClient::readBuffer(1024 * 64);

void RedisClient::Get(std::unique_ptr<RedisRequest> &&req) {

}

void RedisClient::Set(std::unique_ptr<RedisRequest> &&req) {

}

void RedisClient::Rpush(std::unique_ptr<RedisRequest> &&req) {

}

void RedisClient::Lrange(std::unique_ptr<RedisRequest> &&req) {

}

void RedisClient::Select(std::unique_ptr<RedisRequest> &&req) {

}

RedisClient::~RedisClient() {
    aeDeleteFileEvent(G_server.el, this->fd, AE_READABLE | AE_WRITABLE);
    close(this->fd);
    free(this->wbuf);
}

RedisCommand redisCommandTable[] = {
    {"GET",&RedisClient::Get,2,"r",0,1,1,1,0,0},
    {"SET",&RedisClient::Set,-3,"wm",0,1,1,1,0,0},
    {"RPUSH",&RedisClient::Rpush,-3,"wm",0,1,1,1,0,0},
    {"SELECT",&RedisClient::Select,2,"rl",0,0,0,0,0,0},
    {"LRANGE",&RedisClient::Lrange,4,"r",0,1,1,1,0,0}
};

ListDbServer::ListDbServer() {
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
        this->table_[c->name] = c;
    }
}

void ListDbServer::Handle(RedisClient *c, std::unique_ptr<RedisRequest> &&req) {
    auto &name = req->Args[0].Value;
    // convert to upper case
    std::transform(name.begin(), name.end(), name.begin(), ::toupper);
    char buf[100];

    auto it = table_.find(name);
    if (it != table_.end()) {
        RedisCommand *cmd = it->second;
        if ((cmd->arity > 0 && cmd->arity != req->Args.size()) ||
            (-cmd->arity > -req->Args.size())) {
            int n = snprintf(buf, sizeof(buf),
                             "-ERR wrong number of arguments for '%s' command\r\n",
                             name.data());
            c->Raw(buf, n);
        }
        if (cmd->flags & REDIS_CMD_NETWORK_THREAD) {
            (c->*(it->second->proc))(std::move(req));
        } else if (cmd->flags & REDIS_CMD_WRITE) {
            // TODO, move to dedicated thread
            (c->*(it->second->proc))(std::move(req));
        } else {
            // TODO, move to threadpool
            (c->*(it->second->proc))(std::move(req));
        }
    } else {
        int n = snprintf(buf, sizeof(buf), "-ERR unknow command: %s\r\n", name.data());
        c->Raw(buf, n);
    }
}

int main() {
    char addr[] = "0.0.0.0";
    G_server.el = aeCreateEventLoop(1024);
    ListDbServer server;

    int listen_fd = anetTcpServer(G_server.neterr, G_server.port, addr);
    if (listen_fd < 0) {
        perror("open listening socket");
    } else {
        aeCreateFileEvent(G_server.el, listen_fd, AE_READABLE, acceptTcpHandler , &server);
        aeMain(G_server.el);
    }
    return listen_fd;
}
