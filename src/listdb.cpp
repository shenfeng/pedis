#include "listdb.hpp"

struct ServerConf G_server; // /* server global state */

void readQueryFromClient(aeEventLoop *el, int fd, void *privdata, int mask) {
    Client *c = (Client*)privdata;

    int buflen = sizeof(c->rbuf - c->rbufpos);
    int nread = read(fd, c->rbuf + c->rbufpos, buflen);
    if (nread <= 0) { // < 0 error, = 0 client by client
        if (errno == EAGAIN) {
            nread = 0;
        } else {
            if (nread == 0) {
                Log(G_server.logger, "Client closed connection");
            } else {
                Log(G_server.logger, "Read %d bytes from client: %s", nread, strerror(errno));
            }
            close(fd);
            delete c;
            return;
        }
    } else {
        c->rbufpos += nread;
    }
}

void acceptTcpHandler(aeEventLoop *el, int fd, void *privdata, int mask) {
    int cport, cfd;
    char cip[128];
    cfd = anetTcpAccept(G_server.neterr, fd, cip, sizeof(cip), &cport);
    if (cfd == AE_ERR) {
        return;
    }

    Client *c = new Client(cfd);
    anetEnableTcpNoDelay(NULL, cfd);
    if (aeCreateFileEvent(G_server.el, cfd, AE_READABLE, readQueryFromClient, c) == AE_ERR) {
        close(fd);
        delete c;
    }
}

int Server::Start() {
    return 1;
}

int main() {
    char addr[] = "0.0.0.0";
    int listen_fd = anetTcpServer(G_server.neterr, G_server.port, addr);
    if (listen_fd < 0) {
        Log(G_server.logger,
            "Creating Server TCP listening socket on port %d: %s", G_server.port, G_server.neterr);
    } else {
        aeCreateFileEvent(G_server.el, listen_fd, AE_READABLE, acceptTcpHandler , NULL);
    }
    return listen_fd;
}
