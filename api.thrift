struct PushArg {
       1:  string key,
       2:  list<string> datas,
       3:  optional i32 db = 0
}

struct RangeArg {
       1: string key,
       2: i32 start
       3: i32 last
       4: optional i32 db = 0
}


enum KeyType {
    ListType, StringType
}

struct ScanItem {
    1: required string key,
    2: required KeyType type,
}

struct ScanResp {
    1: string cursor  // "0" is the end
    2: list<ScanItem> keys
}

struct ScanArg {
    1: string cursor
    2: i32 limit,
    3: optional i32 db
}

service Listdb {
       void Push(1: PushArg arg)
       void Pushs(1: list<PushArg> arg)
       void Delete(1: string key, 2: i32 db)
       list<string> Range(1: RangeArg arg)
       list<list<string>> Ranges(1: list<RangeArg> arg)
       ScanResp Scan(1: ScanArg arg) // "0" is the start
}