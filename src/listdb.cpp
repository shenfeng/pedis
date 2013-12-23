#include "listdb.hpp"

struct ServerConf G_server; // /* server global state */

void readQueryFromClient(aeEventLoop *el, int fd, void *privdata, int mask) {
    RedisClient *c = (RedisClient*)privdata;
    try {
        if(c->ReadQuery() < 0) {
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

void ListDbServer::HandleRequest(RedisClient *c,  std::unique_ptr<RedisRequest> &&req) {
    const auto &cmd = req->args[0].Value;
    if (cmd == "GET") {
        c->Bulk("footbar");
    } else if (cmd == "SET") {
        c->Raw("+OK\r\n", 5);
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
