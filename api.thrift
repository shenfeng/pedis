struct PushArg {
       1:  string key,
       2:  list<string> datas,
       3:  optional i32 db = 0
}

struct RangeArg {
       1: string key,
       2: i32 start
       3: i32 limit
       4: optional i32 db = 0
}


service Listdb {
       void Push(1: PushArg arg)
       void Delete(1: string key, 2: i32 db)
       list<string> Range(1: RangeArg arg)
}