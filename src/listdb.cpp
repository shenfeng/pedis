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

ByteBuffer RedisClient::readBuffer(1024 * 64);

RedisClient::~RedisClient() {
    aeDeleteFileEvent(G_server.el, fd, AE_READABLE | AE_WRITABLE);
    close(fd);
    free(wbuf);
}

void getCommand(RedisClient *c, std::unique_ptr<RedisRequest> &&req) {}
void setCommand(RedisClient *c, std::unique_ptr<RedisRequest> &&req) {}
void rpushCommand(RedisClient *c, std::unique_ptr<RedisRequest> &&req) {}
void lrangeCommand(RedisClient *c, std::unique_ptr<RedisRequest> &&req) {}


// void ListDbServer::HandleRequest(RedisClient *c,  std::unique_ptr<RedisRequest> &&req) {
//     const auto &cmd = req->args[0].Value;
//     if (cmd == "GET") {
//         c->Bulk("footbar");
//     } else if (cmd == "SET") {
//         c->Raw("+OK\r\n", 5);
//     } else {
//         char buf[100];
//         int n = snprintf(buf, sizeof(buf), "-ERR unknow command: %s\r\n", cmd.data());
//         c->Raw(buf, n);
//     }
// }

struct redisCommand redisCommandTable[] = {
    {"GET",getCommand,2,"r",0,1,1,1,0,0},
    {"SET",setCommand,-3,"wm",0,1,1,1,0,0},
    {"RPUSH",rpushCommand,-3,"wm",0,1,1,1,0,0},
    {"LRANGE",lrangeCommand,4,"r",0,1,1,1,0,0}
};

ListDbServer::ListDbServer() {
    int numcommands = sizeof(redisCommandTable)/sizeof(struct redisCommand);
    for (int i = 0; i < numcommands; i++) {
        redisCommand *c = redisCommandTable + i;
        this->table_[c->name] = c;
    }
}

void ListDbServer::Handle(RedisClient *c, std::unique_ptr<RedisRequest> &&req) {
    auto &cmd = req->args[0].Value;
    // convert to upper case
    std::transform(cmd.begin(), cmd.end(), cmd.begin(), ::toupper);

    auto it = table_.find(cmd);
    if (it != table_.end()) {
        it->second->proc(c, std::move(req));
    } else {
        char buf[100];
        int n = snprintf(buf, sizeof(buf), "-ERR unknow command: %s\r\n", cmd.data());
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
