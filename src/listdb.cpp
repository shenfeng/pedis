#include "listdb.h"

struct ServerConf G_conf; // /* server global state */

int Server::Start() {
    char err[ANET_ERR_LEN];
    char addr[] = "0.0.0.0";
    this->listen_fd = anetTcpServer(err, G_conf.port, addr);
    if (this->listen_fd < 0) {
        Log(G_conf.logger, "Creating Server TCP listening socket on port %d: %s",
                            G_conf.port, err);
        aeCreateFileEvent(this->el, this->listen_fd, AE_READABLE, acceptTcpHandler, NULL);
    } else {

    }
    return this->listen_fd;
}

int main() {

}
