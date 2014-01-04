#ifndef _UTIL_LOG_H
#define _UTIL_LOG_H

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

namespace pedis {
    class Logger {
    private:
        int _level;
        int _next_rotate_mill;
        int _rotateHour;
        FILE *_logfile;
        std::mutex _mutex;
        std::string _filename;
        // manage the memory. No free get called
        static __thread char* _thread_name; // thread local

    public:
        enum { L_TRACE, L_DEBUG, L_INFO, L_WARN, L_ERROR, L_FATAL };

        // default log to stdout
        Logger(): _logfile(stdout), _level(L_INFO), _next_rotate_mill(0) {}
        ~Logger() { std::lock_guard<std::mutex> _(_mutex); _close(); }

        // Open a logger, filename can be "stdout". rotate every rotateHour
        bool Open(const std::string &filename, int level=L_INFO, int rotateHour=0);

        // set thread name, like log4j, can logout thead name
        // if not set for current thread, use std::this_thread::get_id()
        void SetThreadName(const std::string &name) {
            char* _name = (char *)malloc(name.size() + 1);
            strcpy(_name, name.data());
            _thread_name = _name;
        }

        bool Enabled(int level) { return level >= _level; }
        void Logv (int level, const char* fmt, va_list ap);
        void Logv (int level, const char* fmt, ...) {
            va_list ap;
            va_start(ap, fmt);
            this->Logv(level, fmt, ap);
            va_end(ap);
        }

        int SetLevel(int level) {
            assert(level <= L_FATAL && level >= L_TRACE && "logger level out of range");
            int old = _level;
            _level = level;
            return old;
        }

    private:
        Logger(const Logger&);         // No copying allowed
        void operator=(const Logger&);

        inline long millitime(struct timeval const *now) {
            return  now->tv_sec + now->tv_usec/1000.0/1000;
        }

        // prerequisite: mutex held
        inline void _close() {
            if (_logfile && _logfile != stdout) { fclose(_logfile); }
        }

        inline void try_rotate(struct timeval const *now) {
            /*
              first go a fast path by checking _next_rotate_mill, then held the lock.
              the cost is the log file may get closed then reopened a few times, which is fine
             */
            if (_next_rotate_mill > 0 && millitime(now) > _next_rotate_mill) {
                std::lock_guard<std::mutex> _(_mutex);
                _close();
                char buf[64];
                strftime(buf,sizeof(buf),"_%F_%H",localtime(&(now->tv_sec)));
                auto filename = _filename + buf;
                _logfile = fopen(filename.data(), "a");
                _next_rotate_mill = millitime(now) + _rotateHour * 60 * 60 * 1000;
            }
        }
    };

    // convenient methods, operate on a global Logger(static)
    void log_write(int level, const char *fmt, ...);
    bool log_open(const std::string &filename, int level=Logger::L_INFO, int rotateHour=0);
    int set_log_level(int level);
    void set_thread_name(const std::string &name);

    /*
      sample:
      2014-01-04 19:43:49.888 E [main] util/log.cpp(81): hello world

      no thread name
      2014-01-04 19:44:32.437 E [0x7fff7c80a310] util/log.cpp(74): hello world
    */

#define log_trace(fmt, args...)                                         \
    log_write(Logger::L_TRACE, "%s(%d): " fmt, __FILE__, __LINE__, ##args)
#define log_debug(fmt, args...)                                         \
    log_write(Logger::L_DEBUG, "%s(%d): " fmt, __FILE__, __LINE__, ##args)
#define log_info(fmt, args...)                                          \
    log_write(Logger::L_INFO,  "%s(%d): " fmt, __FILE__, __LINE__, ##args)
#define log_warn(fmt, args...)	\
    log_write(Logger::L_WARN,  "%s(%d): " fmt, __FILE__, __LINE__, ##args)
#define log_error(fmt, args...)	\
    log_write(Logger::L_ERROR, "%s(%d): " fmt, __FILE__, __LINE__, ##args)
#define log_fatal(fmt, args...)	\
    log_write(Logger::L_FATAL, "%s(%d): " fmt, __FILE__, __LINE__, ##args)
}

#endif /* _UTIL_LOG_H */
