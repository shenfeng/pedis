#ifndef _PEDIS_UTIL_LOG_H
#define _PEDIS_UTIL_LOG_H

#include <assert.h>
#include <thread>
#include <mutex>
#include <cstdarg>
#include <string>
#include <time.h>
#include <sys/time.h>
#include <stdio.h>
#include <sstream>
#include <string.h>

namespace listdb {
class Logger {
public:
    enum { kLevelTrace, kLevelDebug, kLevelInfo, kLevelWarn, kLevelError, kLevelFatal };

    // default log to stdout
    Logger(): level_(kLevelInfo), next_rotate_mill_(0), logfile_(stdout), current_max_id_(0) {}
    ~Logger() { std::lock_guard<std::mutex> _(mutex_); this->Close(); }

    // Open a logger, filename can be "stdout". rotate every rotateHour
    bool Open(const std::string &filename, int level=kLevelInfo, int rotatehour=0);

    // set thread name, like log4j, can logout thead name
    // if not set for current thread, use std::this_thread::get_id()
//    void SetThreadName(const std::string &name) {
//        char* _name = (char *)malloc(name.size() + 1);
//        strcpy(_name, name.data());
//        thread_name_ = _name;
//    }

    bool Enabled(int level) { return level >= level_; }
    void Logv (int level, const char* fmt, va_list ap);
    void Logv (int level, const char* fmt, ...) {
        va_list ap;
        va_start(ap, fmt);
        this->Logv(level, fmt, ap);
        va_end(ap);
    }

    int SetLevel(int level) {
        assert(level <= kLevelFatal && level >= kLevelTrace && "logger level out of range");
        int old = level_;
        level_ = level;
        return old;
    }

private:
    int level_;
    int next_rotate_mill_;
    int rotate_hour_;
    FILE *logfile_;
    std::mutex mutex_;
    std::string filename_;

    int current_max_id_;
    static __thread int thread_id_;

    // manage the memory. No free get called
//    static __thread char* thread_name_; // thread local. take ownership of the memory

    inline long MilliTime(struct timeval const *now) {
        return now->tv_sec * 1000 + now->tv_usec/1000;
    }

    // prerequisite: mutex held
    inline void Close() {
        if (logfile_ && logfile_ != stdout) { fclose(logfile_); }
    }

    inline void TryRotate(struct timeval const *now) {
        // First go a fast path by checking next_rotate_mill_, then grab the lock.
        // The cost is the log file may get closed then reopened a few times.
        // which should happen rarely
        if (next_rotate_mill_ > 0 && this->MilliTime(now) > next_rotate_mill_) {
            std::lock_guard<std::mutex> _(mutex_);
            this->Close();
            char buf[64];
            strftime(buf,sizeof(buf),"_%F",localtime(&(now->tv_sec)));
            auto filename = filename_ + buf;
            logfile_ = fopen(filename.data(), "a");
            next_rotate_mill_ = this->MilliTime(now) + rotate_hour_ * 60 * 60 * 1000;
        }
    }

    Logger(const Logger&);         // No copying allowed
    void operator=(const Logger&);
};

// convenient methods, operate on a global Logger(static)
void log_write(int level, const char *fmt, ...);
bool log_open(const std::string &filename, int level=Logger::kLevelInfo, int rotateHour=0);
int set_log_level(int level);
void set_thread_name(const std::string &name);

#define __FILE_NAME (strrchr(__FILE__, '/') ? strrchr(__FILE__, '/') + 1 : __FILE__)

/*
  sample:
  2014-01-04 19:43:49.888 E [main] util/log.cpp(81): hello world

  no thread name
  2014-01-04 19:44:32.437 E [0x7fff7c80a310] util/log.cpp(74): hello world
*/

#define log_trace(fmt, ...)                                             \
    log_write(listdb::Logger::kLevelTrace, "%s(%d): " fmt, __FILE_NAME, __LINE__, __VA_ARGS__)
#define log_debug(fmt, ...)                                             \
    log_write(listdb::Logger::kLevelDebug, "%s(%d): " fmt, __FILE_NAME, __LINE__, __VA_ARGS__)
#define log_info(fmt, ...)                                              \
    log_write(listdb::Logger::kLevelInfo,  "%s(%d): " fmt, __FILE_NAME, __LINE__, __VA_ARGS__)
#define log_warn(fmt, ...)                                              \
    log_write(listdb::Logger::kLevelWarn,  "%s(%d): " fmt, __FILE_NAME, __LINE__, __VA_ARGS__)
#define log_error(fmt, ...)                                             \
    log_write(listdb::Logger::kLevelError, "%s(%d): " fmt, __FILE_NAME, __LINE__, __VA_ARGS__)
#define log_fatal(fmt, ...)                                             \
    log_write(listdb::Logger::kLevelFatal, "%s(%d): " fmt, __FILE_NAME, __LINE__, __VA_ARGS__)

} // namespace pedis

#endif /* _PEDIS_UTIL_LOG_H */
