#include "logger.hpp"

using namespace std;

namespace listdb {

__thread int Logger::thread_id_(0);
static Logger g_logger; // static global logger

void Logger::Logv(int level, const char *fmt, va_list ap) {
    assert(level <= kLevelFatal && level >= kLevelTrace && "logger level out of range");
    if (level_ > level) return;

    char msg[4096]; // maxline 4k
    vsnprintf(msg, sizeof(msg), fmt, ap);

    char buf[64];
    struct timeval tv;
    gettimeofday(&tv,NULL);
    this->TryRotate(&tv);

    int off = strftime(buf,sizeof(buf),"%F %H:%M:%S.",localtime(&tv.tv_sec));
    snprintf(buf+off,sizeof(buf)-off,"%03d",(int)tv.tv_usec/1000);

    const char levels[] = "TDIWEF"; // trace, debug, info, warn, error, fatal
    {
        std::lock_guard<std::mutex> _(mutex_);
        if(!thread_id_) {
            thread_id_ = ++this->current_max_id_;
        }
        fprintf(logfile_, "%s %c [thread-%d] %s\n", buf, levels[level], thread_id_, msg);
        fflush(logfile_);
    }
}

bool Logger::Open(const std::string &filename, int level, int rotateHour) {
    assert(level <= kLevelFatal && level >= kLevelTrace && "logger level out of range");
    assert((logfile_ == NULL || logfile_ == stdout) && "already opened");

    level_ = level;
    next_rotate_mill_ = 0; // do not rotate for stdout
    rotate_hour_ = rotateHour;
    filename_ = filename;

    {
        std::lock_guard<std::mutex> _(mutex_); // need to held lock before call _close
        this->Close();
    }

    if (filename == "stdout") {
        logfile_ = stdout;
    } else if (rotateHour > 0) {
        struct timeval now;
        gettimeofday(&now, NULL);
        next_rotate_mill_ = 1;
        this->TryRotate(&now);
    } else {
        logfile_ = fopen(filename.c_str(), "a");
    }

    return logfile_ != NULL;
}

bool log_open(const std::string &filename, int level, int rotateHour) {
    return g_logger.Open(filename, level, rotateHour);
}

int set_log_level(int level) { return g_logger.SetLevel(level); }
//void set_thread_name(const std::string &name) { g_logger.SetThreadName(name); }

void log_write(int level, const char *fmt, ...) {
    if (!g_logger.Enabled(level)) return;
    va_list ap;
    va_start(ap, fmt);
    g_logger.Logv(level, fmt, ap);
    va_end(ap);
}
} // namespace pedis


#if 0

#include <iostream>
using namespace std;
using namespace pedis;

int main() {
    Logger l;
    auto r = l.Open("stdout", Logger::kLevelDebug, 1);
    assert(r== true && "open failed");

    l.SetThreadName("main");


    l.Logv(Logger::kLevelTrace, "hello %s", "world");
    l.Logv(Logger::kLevelDebug, "hello %s", "world");

    std::thread t([&](){
            l.Logv(Logger::kLevelWarn, "hello %s", "world");
            l.SetThreadName("thread-1");
            l.Logv(Logger::kLevelWarn, "hello with name: %s", "world");
        });

    t.join();

    log_open("stdout", Logger::kLevelDebug, 1);
    log_info("hello %s", "world");
}

#endif
