#include "listdb.hpp"

struct ServerConf G_server; // /* server global state */

void readQueryFromClient(aeEventLoop *el, int fd, void *privdata, int mask) {
    RedisClient *c = (RedisClient*)privdata;
    try {
        if (c->ReadQuery() <= 0) { // TODO handle errror
            delete c;
        }
    } catch (ProtocolException &e) {
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
    if (aeCreateFileEvent(G_server.el, cfd, AE_READABLE, readQueryFromClient, c) == AE_ERR) {
        delete c;
    }
}

void sendReplyToClient(aeEventLoop *el, int fd, void *privdata, int mask) {
    RedisClient *c = (RedisClient*) privdata;
    if (c->Write() < 0) {
        delete c;
    }
}

ByteBuffer RedisClient::readBuffer(1024 * 64);

void ListDbServer::HandleRequest(RedisClient *c,  std::unique_ptr<RedisRequest> &&req) {
    if (req->args[0].Value == "GET") {
        c->TryWrite("$6\r\nfoobar\r\n");
    } else if (req->args[0].Value == "SET") {
        c->TryWrite("+OK\r\n");
    } else {
        c->TryWrite("-ERR unknown command: \r\n");
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
