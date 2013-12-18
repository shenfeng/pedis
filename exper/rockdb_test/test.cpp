#include "rocksdb/db.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/cache.h"
#include "rocksdb/merge_operator.h"
#include "util/coding.h"
#include "util/random.h"
#include <iostream>
#include <thread>
#include <ratio>
#include <chrono>
#include <stdio.h>
#include <algorithm>

using namespace std;
using namespace rocksdb;
using namespace std::chrono;


class Watch {
private:
    system_clock::time_point last;

public:
    Watch(): last(system_clock::now()) {}
    void tap() { last = system_clock::now(); }
    double tic() {
        auto now = system_clock::now();
        auto t = duration_cast<duration<double>>(now - last).count();
        last = now;
        return t * 1000; // in ms
    }
};

// Helper for quickly generating random data.
class RandomGenerator {
private:
    std::string data_;
    unsigned int pos_;

public:
    RandomGenerator() {
        // We use a limited amount of data over and over again and ensure
        // that it is larger than the compression window (32KB), and also
        // large enough to serve all typical value sizes we want to write.
        //Random rnd(system_clock::to_time_t(system_clock::now()));
        auto seed = system_clock::to_time_t(system_clock::now());
        Random rnd(seed);

        const unsigned int size = 1024 * 1024 * 4;

        data_.resize(size);
        for (int i = 0; i < 1024 * 1024 * 4; i++) {
            data_[i] = static_cast<char>(' ' + rnd.Uniform(95));   // ' ' .. '~'
        }
        pos_ = 0;

    }

    Slice Generate(unsigned int len) {
        if (pos_ + len > data_.size()) {
            pos_ = 0;
            assert(len < data_.size());
        }
        pos_ += len;
        return Slice(data_.data() + pos_ - len, len);
    }
};


Status to_list(const string& value,
               std::vector<Slice> *elements,
               int start,
               int stop) { // [start, stop]
    auto p = value.data(), end = value.data() + value.size();
    uint32_t n = 0;
    while( p != end) {
        uint32_t size = 0;
        p = GetVarint32Ptr(p, p + 5, &size); // TODO corruption?
        n += 1;
        p += size;
    }

    while(stop < 0)  stop += (int)n;
    while(start < 0) start += (int)n;

    // uint32_t ustop = uint32_t(stop); // get rid of warning by converting to unsigned
    // while(ustop >= n) ustop -= n;

    //     cout << start << "\t" << ustop << "\t" << n << "\t" << stop << endl;
    if (start > stop) return Status::OK(); // empty

    elements->reserve(stop - start + 1);
    p = value.data();
    end = value.data() + value.size();

    for(int i = 0; p != end; i++) {
        if (i < start) continue;
        if (i > stop) break;

        uint32_t size = 0;
        p = GetVarint32Ptr(p, p + 5, &size);
        elements->emplace_back(p, size); // reuse value's memory
        p += size;
    }

    return Status::OK();
}

class ListMergeOperator: public MergeOperator {
public:
    virtual ~ListMergeOperator(){}

    // Gives the client a way to express the read -> modify -> write semantics
    // key:         (IN) The key that's associated with this merge operation.
    // existing:    (IN) null indicates that the key does not exist before this op
    // operand_list:(IN) the sequence of merge operations to apply, front() first.
    // new_value:  (OUT) Client is responsible for filling the merge result here
    // logger:      (IN) Client could use this to log errors during merge.
    //
    // Return true on success. Return false failure / error / corruption.
    bool FullMerge(const Slice& key,
                   const Slice* existing_value,
                   const std::deque<std::string>& operand_list,
                   std::string* new_value,
                   Logger* logger) const override {

        // Clear the *new_value for writing.
        assert(new_value);
        new_value->clear();

        // Compute the space needed for the final result.
        int numBytes = 0;
        for (auto &s : operand_list) {
            numBytes += s.size() + VarintLength(s.size());
        }

        if (existing_value) {
            new_value->reserve(numBytes + existing_value->size());
            new_value->append(existing_value->data(), existing_value->size());
        } else {
            new_value->reserve(numBytes);
        }

        for (auto &s: operand_list) {
            // encode list item as: size + data
            PutVarint32(new_value, s.size()); // put size
            new_value->append(s);
        }

        return true;
    }

    // This function performs merge(left_op, right_op)
    // when both the operands are themselves merge operation types.
    // Save the result in *new_value and return true. If it is impossible
    // or infeasible to combine the two operations, return false instead.
    bool PartialMerge(const Slice& key,
                      const Slice& left_operand,
                      const Slice& right_operand,
                      std::string* new_value,
                      Logger* logger) const override {
        return false;
    }

    // The name of the MergeOperator. Used to check for MergeOperator
    // mismatches (i.e., a DB created with one MergeOperator is accessed
    // using a different MergeOperator)
    const char* Name() const {
        return "ListMergeOperator";
    }
};

class RedisList {
private:
    rocksdb::DB* db;
    const WriteOptions woption;
    const ReadOptions roption;

public:
    RedisList(rocksdb::DB* db): db(db) {
        // woption.disableWAL = true;
    }

    Status Rpush(const Slice& key, const Slice& value) {
        return db->Merge(woption, key, value);
    }

    Status Lrange(const Slice& key,
                  std::string *scratch,  // value
                  std::vector<Slice> *elements,
                  int start,
                  int stop) {
        elements->clear();
        Status s = db->Get(roption, key, scratch);
        if (s.ok()) {
            to_list(*scratch, elements, start, stop);
        }
        return s;
    }
};


std::string get_stats(std::vector<double> &times) {
    std::sort(times.begin(), times.end());
    auto n = times.size();
    char buf[1024];

    if (times.empty()) return "";

    snprintf(buf,
             sizeof(buf),
             "n=%lu, 50%%: %.4fms, 90%%: %.4fms, 95%%: %.4fms, 99%%: %.4fms",
             n,
             times[int(n * 0.5)],
             times[int(n * 0.9)],
             times[int(n * 0.95)],
             times[int(0.99 * n)]);
    return std::string(buf);
}

void bench_read_and_write(RedisList *list) {

    RandomGenerator gen;

    const unsigned int N = 1 << 31; // 2 billion items
    std::vector<Slice> elements;

    auto start = high_resolution_clock::now(),
        last = high_resolution_clock::now();

    Random rnd(31);
    std::vector<double> times;

    for(unsigned int i = 0; i < N; i++) {
        int userid = rnd.Uniform(1024 * 1024 * 100); // 0.1 billion
        string key = std::to_string(userid);
        if (rnd.Next() % 2) { // 50% write
            list->Rpush(key, gen.Generate(rnd.Uniform(7) + 13));
        } else { // 50% read
            Watch w;
            std::string scratch;
            auto s = list->Lrange(key, &scratch, &elements, 0, 200);
            //    cout << key << "\t" << s.ToString() << endl;
            if (s.ok())
                times.push_back(w.tic());
        }

        if (i && i % 200000 == 0) {
            auto now = high_resolution_clock::now();
            auto time_span = duration_cast<duration<double>>(now - start).count() * 1000; // mill seconds
            auto last_span = duration_cast<duration<double>>(now - last).count() * 1000; // mill seconds

            last = now;

            printf("i = %d, time: %.4fms, ops/ms: %.4f, last 100k: %.4f, read stats: %s\n", i, time_span, i / time_span,
                   200000 / last_span, get_stats(times).data());
            fflush(stdout);
            times.clear();
        }
    }
}

void start_read_write_thread() {
    thread t([](){


        });
    t.detach();
}

int main() {
    srand(time(NULL));
    rocksdb::DB* db;
    rocksdb::Options options;
    options.create_if_missing = true;
    options.write_buffer_size = 1024 * 1024 * 256; // 64M buffer
    options.target_file_size_base = 1024 * 1024 * 64; // 32M file size
    options.filter_policy = NewBloomFilterPolicy(10);
    options.block_cache = NewLRUCache(1024 << 20);
    options.merge_operator.reset(new ListMergeOperator);

    Watch w;
    rocksdb::Status status = rocksdb::DB::Open(options, "./tmp_data", &db);
    assert(status.ok());
    cout << "db opened in: " << w.tic() << " ms" << endl;

    RedisList list(db);

    Slice key = "1212121"; // userid
    list.Rpush(key, "x:1212121:123123");
    list.Rpush(key, "x:1212121:123124");

    std::vector<Slice> elements;
    string scratch;
    list.Lrange(key, &scratch, &elements, 0, -1);
    for(auto s: elements) {
        cout<<s.ToString()<<endl;
    }

    bench_read_and_write(&list);
    delete db;
    // db->Put(WriteOptions(), "user1212");
}
