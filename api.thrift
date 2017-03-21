struct PushArg {
       1:  string key,
       2:  list<string> datas,
}

struct RangeArg {
       1: string key,
       2: i32 start
       3: i32 limit
}

service Listdb {
       void Push(1: PushArg arg)
       list<string> Range(1: RangeArg arg)
}