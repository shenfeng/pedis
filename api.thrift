
struct PushArg {
       1:  string key,
       2:  list<string> datas,
}


service pedis {
        string get(1: string key)
        void put(1: string key, 2: string expire)

        void remove(1: string key)
        bool exits(1: string key)
        string typeof(1: string key)

        // return how many element in the list
        i32 rpush(1: PushArg arg)
}