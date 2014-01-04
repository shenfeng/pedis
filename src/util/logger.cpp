#include "util/logger.h"

using namespace std;

namespace pedis {

    __thread char* Logger::_thread_name(NULL);
    static Logger logger; // static global logger

    void Logger::Logv(int level, const char *fmt, va_list ap) {
        assert(level <= L_FATAL && level >= L_TRACE && "logger level out of range");
        if (_level > level) return;

        char msg[4096]; // maxline 4k
        vsnprintf(msg, sizeof(msg), fmt, ap);

        char buf[64];
        struct timeval tv;
        gettimeofday(&tv,NULL);
        this->try_rotate(&tv);

        int off = strftime(buf,sizeof(buf),"%F %H:%M:%S.",localtime(&tv.tv_sec));
        snprintf(buf+off,sizeof(buf)-off,"%03d",(int)tv.tv_usec/1000);

        const char levels[] = "TDIWEF"; // trace, debug, info, warn, error, fatal
        {
            std::lock_guard<std::mutex> _(_mutex);
            if (_thread_name) {
                fprintf(_logfile, "%s %c [%s] %s\n", buf, levels[level], _thread_name, msg);
            } else {
                auto id = std::this_thread::get_id();
                std::stringstream s;
                s << id;
                auto n = s.str();
                fprintf(_logfile, "%s %c [%s] %s\n", buf, levels[level], n.data(), msg);
            }
            fflush(_logfile);
        }
    }

    bool Logger::Open(const std::string &filename, int level, int rotateHour) {
        assert(level <= L_FATAL && level >= L_TRACE && "logger level out of range");
        assert((_logfile == NULL || _logfile == stdout) && "already opened");

        _level = level;
        _next_rotate_mill = 0;
        _rotateHour = rotateHour;
        _filename = filename;

        {
            std::lock_guard<std::mutex> _(_mutex); // need to held lock before call _close
            _close();
        }

        if (filename == "stdout") {
            _logfile = stdout;
        } else if (rotateHour > 0) {
            struct timeval now;
            gettimeofday(&now, NULL);
            _next_rotate_mill = 1;
            this->try_rotate(&now);
        } else {
            _logfile = fopen(filename.c_str(), "a");
        }

        return _logfile != NULL;
    }

    bool log_open(const std::string &filename, int level, int rotateHour) {
        return logger.Open(filename, level, rotateHour);
    }

    int set_log_level(int level) { return logger.SetLevel(level); }
    void set_thread_name(const std::string &name) { logger.SetThreadName(name); }

    void log_write(int level, const char *fmt, ...) {
        if (!logger.Enabled(level)) return;
        va_list ap;
        va_start(ap, fmt);
        logger.Logv(level, fmt, ap);
        va_end(ap);
    }
}


#if 0

#include <iostream>
using namespace std;
using namespace pedis;

int main() {
    Logger l;
    auto r = l.Open("stdout", Logger::L_DEBUG, 1);
    assert(r== true && "open failed");

    l.SetThreadName("main");


    l.Logv(Logger::L_TRACE, "hello %s", "world");
    l.Logv(Logger::L_DEBUG, "hello %s", "world");

    std::thread t([&](){
            l.Logv(Logger::L_WARN, "hello %s", "world");
            l.SetThreadName("thread-1");
            l.Logv(Logger::L_WARN, "hello with name: %s", "world");
        });

    t.join();

    log_open("stdout", Logger::L_DEBUG, 1);
    log_info("hello %s", "world");
}

#endif
