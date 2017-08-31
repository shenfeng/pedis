namespace java logdb.thrift

enum Action {
CHAT
CHAT_AUDIO
CHAT_FACETIME
CHAT_INTERVIEW_ACCEPT
CHAT_INTERVIEW_CANCEL
CHAT_INTERVIEW_INVITE
CHAT_INTERVIEW_REFUSE
CHAT_MOBILE
CHAT_MOBILE_ACCEPT
CHAT_MOBILE_REJECT
CHAT_PIC
CHAT_RESUME_GET
CHAT_RESUME_GET_ACCEPT
CHAT_RESUME_GET_REJECT
CHAT_RESUME_SEND
CHAT_RESUME_SEND_ACCEPT
CHAT_WEIXIN
CHAT_WEIXIN_ACCEPT
CHAT_WEIXIN_REJECT
DETAIL_BOSS
DETAIL_BRAND
DETAIL_F_BOSS
DETAIL_F_GEEK
DETAIL_GEEK
DETAIL_GEEK_ADDFRIEND
GEEK_CALL
GEEK_CALL_USED
LIST_BOSS
LIST_BOSS_SEARCH
LIST_GEEK
LIST_GEEK_SEARCH
LIST_NOTIFY
OPEN_RESUME
SEARCH_CLICK
SEARCH_LIST
}

struct LogItem {
    1: required Action action
    2: required i32 ts

    3: optional i32 bossId,
    4: optional i32 jobId,

    5: optional i32 geekId,
    6: optional i32 expectId

    7: optional string msg     // 聊天内容
    8: optional string algo    // detail_geek 等的算法标记
    9: optional string keyword // 搜索关键字
    10: optional string filter // 搜索的筛选条件
    11: optional string extra
    12: optional i16 page      // list-geek-search, list-boss, list-geek, list-boss-search等
    13: optional i32 brandId
}


struct ScanReq {
    1: required i32 limit
    2: string cursor = "0"
    3: optional i32 db = 0
}

struct ScanResp {
    1: required list<string> keys
    2: required string cursor
}


struct PushReq {
    1: required string key,
    2: required i32 db,
    3: required list<LogItem> logs
}

struct RangeReq {
       1: string key,
       2: i32 start
       3: i32 last
       4: i32 db = 0
}

service logdb_api {
    void Push(1: list<PushReq> reqs)
    void Delete(1: string key, 2: i32 db)
    ScanResp Scan(1: ScanReq req)
    list<LogItem> Range(1: RangeReq req)
    list<list<LogItem>> Ranges(1: list<RangeReq> reqs)
}