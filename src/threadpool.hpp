#ifndef _THREAD_POOL_HPP
#define _THREAD_POOL_HPP
/*
 * Feng Shen <shenedu@gmail.com>  2013/12/5
 * http://shenfeng.me
 * g++  -Wall -pedantic -O3 -std=c++11 -o threadpool threadpool.cpp -lpthread
 * using gcc (Debian 4.8.1-7) 4.8.1
 */

#include <atomic>
#include <thread>
#include <vector>
#include <condition_variable>
#include <mutex>
#include <future>
#include <chrono>
#include <tuple>

class thread_pool {
    struct impl_base {
        virtual void call() = 0;
        virtual ~impl_base() {}
    };

    // 0 个参数
    template<typename F, typename... Args> struct impl_type: impl_base {
        F f;
        std::tuple<Args...> args;

        impl_type(F &&f, Args&& ...args):
            f(std::move(f)), args(std::make_tuple(args...)) {}
        void call() { f(); }
    };


    struct function_wrapper {
        std::unique_ptr<impl_base> impl;

        template<typename F, typename... Args> explicit
        function_wrapper(F &&f, Args&& ...args):
            impl(new impl_type<F, Args...>(std::move(f), args...)) { }

        function_wrapper() = default;
        function_wrapper& operator= (function_wrapper &&other) = default;
        function_wrapper(function_wrapper &&other) = default;
        void operator() () { impl->call(); }
    };

    template<typename T> class array_block_queue {
    private:
        int size; // maxsize of the queue
        T *items;
        int take_index;
        int put_index;
        int count; // element count in the queue

        mutable std::mutex lock;
        std::condition_variable not_empty; // waiting takes;
        std::condition_variable not_full; // waiting put;

        inline int inc(int i) { return (++i == size) ? 0: i; }
        inline int dec(int i) { return ((i == 0) ? size: i) - 1; }

        inline T& extract() {
            int idx = take_index;
            take_index = inc(take_index);
            -- count;
            return items[idx];
        }

    public:
        explicit array_block_queue(int size):
            size(size), items(new T[size]), take_index(0), put_index(0), count(0) { }

        ~array_block_queue() { delete []items; }

        array_block_queue(array_block_queue<T> &other) = delete;
        array_block_queue(array_block_queue<T> &&other) = delete;
        array_block_queue<T> & operator=(array_block_queue<T> &r) = delete;
        array_block_queue<T> & operator=(array_block_queue<T> &&r) = delete;

        // inserts the specified element at the tail of this queue,
        // waiting for space to become available if the queue is full.
        void put(T &&new_value) {
            {
                std::unique_lock<std::mutex> lk(lock);
                not_full.wait(lk, [this] { return count != size; });
                items[put_index] = std::move(new_value); // copy
                put_index = inc(put_index);
                ++count;
            }
            not_empty.notify_one(); // 不需要lock
        }

        // Inserts the specified element at the tail of this queue if it is
        // possible to do so immediately without exceeding the queue's capacity,
        // returning true upon success and false if this queue is full
        bool offer(const T&new_value) {
            std::lock_guard<std::mutex> _(lock);
            if (count == size) {
                return false;
            }
            insert(new_value);
        }

        bool empty() const {
            std::lock_guard<std::mutex> _(lock);
            return count == 0;
        }

        // Retrieves and removes the head of this queue, or returns null if this queue is empty.
        T poll() {
            std::lock_guard<std::mutex> _(lock);
            return count == 0 ? std::shared_ptr<T>() : extract();
        }

        // take with a timeout
        template<typename Rep, typename Period>
        bool take(T &result, const std::chrono::duration<Rep, Period>& rel_time) {
            std::unique_lock<std::mutex> lk(lock);
            if (count == 0) not_empty.wait_for(lk, rel_time);
            if (count > 0) {
                result = std::move(extract());
                lk.unlock();
                not_full.notify_one();
                return true;
            }
            return false;
        }

        // Retrieves and removes the head of this queue, waiting if necessary until an element becomes
        bool take(T &result) {
            std::unique_lock<std::mutex> lk(lock);
            if (count == 0) not_empty.wait(lk);
            if (count > 0) {
                result = std::move(extract());
                lk.unlock();
                not_full.notify_one();
                return true;
            }
            return false;
        }
    };

private:
    std::atomic<bool> done;
    array_block_queue<function_wrapper> work_queue;
    std::vector<std::thread> threads;

    void worker_thread() {
        function_wrapper task;
        while(true) {
            if (done.load(std::memory_order_relaxed) && work_queue.empty()) break;
            // if (done && work_queue.empty()) break;
            // C++的 blocking wait 不像java那样，有InterruptedException
            if (work_queue.take(task, std::chrono::milliseconds(100))) {
                task();
            }
        }
    }

public:
    thread_pool(int thread, int queue): done(false), work_queue(queue) {
        for(int i = 0; i < thread; i++) {
            threads.emplace_back(&thread_pool::worker_thread, this);
        }
    }

    template<typename F, typename... Args>
    std::future<typename std::result_of<F(Args...)>::type> submit(F &&f, Args&& ...args) {
        typedef typename std::result_of<F(Args...)>::type result_type;
        std::packaged_task<result_type(Args...)> task(f);
        auto fu = task.get_future();
        work_queue.put(function_wrapper(std::move(task), args...));
        return fu;
    }

    void shutdown() { done = true; }

    ~thread_pool() {
        done = true;
        for(auto &t: threads) {
            if (t.joinable()) t.join();
        }
    }
};

#endif /* _THREAD_POOL_HPP */
