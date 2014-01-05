#include "listdb.hpp"

int main() {
    char addr[] = "0.0.0.0";
    PedisServer server;

    ServerConf conf;

    int listen_fd = anetTcpServer(conf.neterr, conf.port, addr);
    if (listen_fd < 0) {
        perror("open listening socket");
    } else {
        aeCreateFileEvent(server.el, listen_fd, AE_READABLE, acceptTcpHandler , &server);
        aeMain(server.el);
    }
    return listen_fd;
}
