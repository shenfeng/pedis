#include "rocksdb/db.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/merge_operator.h"
#include "util/coding.h"
#include <iostream>

using namespace std;
using namespace rocksdb;

Status to_list(const string& value,
               std::vector<Slice> *elements,
               uint32_t start, int stop) { // [start, stop]
    auto p = value.data(), end = value.data() + value.size();
    uint32_t n = 0;
    while( p != end) {
        uint32_t size = 0;
        p = GetVarint32Ptr(p, p + 5, &size); // TODO corruption?
        n += 1;
        p += size;
    }

    while(stop < 0) stop += (int)n;
    uint32_t ustop = uint32_t(stop); // get rid of warning by converting to unsigned
    while(ustop >= n) ustop -= n;

    if (start > ustop) return Status::OK(); // empty

    elements->reserve(ustop - start + 1);
    p = value.data();
    end = value.data() + value.size();

    for(uint32_t i = 0; p != end; i++) {
        if (i < start) continue;
        if (i > ustop) break;

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
    RedisList(rocksdb::DB* db): db(db) { }

    Status Rpush(const Slice& key, const Slice& value) {
        return db->Merge(woption, key, value);
    }

    Status Lrange(const Slice& key, std::vector<Slice> *elements,
                  uint32_t start, int stop ) {
        std::string value;
        Status s = db->Get(roption, key, &value);
        if (s.ok()) {
            to_list(value, elements, start, stop);
        }
        return s;
    }
};

int main() {
    rocksdb::DB* db;
    rocksdb::Options options;
    options.create_if_missing = true;
    options.filter_policy = NewBloomFilterPolicy(10);
    options.merge_operator.reset(new ListMergeOperator);

    rocksdb::Status status = rocksdb::DB::Open(options, "/tmp/rockdb_test", &db);
    assert(status.ok());

    RedisList list(db);
    Slice key = "1212121"; // userid
    list.Rpush(key, "x:1212121:123123");
    list.Rpush(key, "x:1212121:123124");

    std::vector<Slice> elements;
    list.Lrange(key, &elements, 0, -1);
    for(auto s: elements) {
        cout<<s.ToString()<<endl;
    }

    delete db;
    // db->Put(WriteOptions(), "user1212");
}
