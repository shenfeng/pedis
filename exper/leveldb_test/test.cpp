#include <stdio.h>
#include <stdlib.h>
#include "leveldb/db.h"
#include "leveldb/filter_policy.h"
#include <iostream>

#define N 400000
using namespace std;

int main(int argc, char** argv) {

    leveldb::DB* db;
    leveldb::Options options;
    options.create_if_missing = true;
    options.filter_policy = leveldb::NewBloomFilterPolicy(10);
    leveldb::Status status = leveldb::DB::Open(options, "/tmp/testdb", &db);
    assert(status.ok());
    leveldb::Slice key = "test-key";

    {
        std::string s;
        db->Get(leveldb::ReadOptions(), key, &s);
        // return 1;
        cout << s.length() << endl;
        s.clear();

        for(int i = 0; i < 10000; i++) {
            db->Put(leveldb::WriteOptions(), "kkk-" + std::to_string(i), "*vvvvvvvvvv*" + std::to_string(i));
        }

        // return 1;

        s.append(1024 * 1024 * 4, 'c');
        // db->Put(leveldb::WriteOptions(), "aaaaaaaaaa", "111111");
        // db->Put(leveldb::WriteOptions(), "bbbb", "222222");
        db->Put(leveldb::WriteOptions(), key, s);
        s.clear();

        db->Get(leveldb::ReadOptions(), key, &s);
        cout << s.length() << endl;
        db->CompactRange(nullptr, nullptr);
        delete db;
        return 1;
    }

    std::string value;
    leveldb::ReadOptions roption;
    db->Get(roption, key, &value);
    cout << value << endl;


    db->Put(leveldb::WriteOptions(), key, "version-one");

    {
        leveldb::ReadOptions options;
        options.snapshot = db->GetSnapshot();
        leveldb::Iterator* iter = db->NewIterator(options);
        db->Put(leveldb::WriteOptions(), key, "version-two");

        for (iter->Seek(key); iter->Valid();) {
            cout<< iter->value().ToString() << endl;
            break;
        }

        db->Get(options, key, &value);
        cout << " get " << value << endl;


        delete iter;
        db->ReleaseSnapshot(options.snapshot);

    }

    for(int i = 0; i < N; i++) {
        db->Put(leveldb::WriteOptions(), "key-" + std::to_string(i),
                "value----value-0000000000" + std::to_string(i));
    }

    for(int i = 0; i < 100; i++) {
        db->Get(leveldb::ReadOptions(), "key-" + std::to_string(i), &value);
        //        std::cout << value << std::endl;
    }
    for (int k = 0; k < 2; k++) {

        if (k == 1)
            db->CompactRange(nullptr, nullptr);
        const char* properties[] = {"leveldb.sstables", "leveldb.stats"};
        for (unsigned int i = 0; i < sizeof(properties) / sizeof(char *); i++) {
            std::string value;
            db->GetProperty(properties[i], &value);
            cout << properties[i] << "\n----------------\n" << value << endl;
        }
        cout << "\n\n" << endl;
    }
    //    cout << sizeof (properties ) / sizeof (char*) << endl;
    //    db->GetProperty("", <#std::string *value#>)

    delete db;

    return 0;
}
