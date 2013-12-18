#ifndef _LISTDB_H_
#define _LISTDB_H_


class Client {
    int fd;

    /* Response buffer */
    int bufpos;
    char buf[1024 * 64];
}



#endif /* _LISTDB_H_ */
