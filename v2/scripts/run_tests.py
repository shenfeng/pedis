# encoding: utf8

import os
import sys

reload(sys)
sys.setdefaultencoding('utf-8')

DIR = os.path.abspath(os.path.dirname(__file__))
if '%s/gen-py' % DIR not in sys.path: sys.path = ['%s/gen-py' % DIR] + sys.path
if DIR not in sys.path:  sys.path.append(DIR)


def get_client(server):
    from thrift.transport import TSocket, TTransport
    from thrift.protocol import TBinaryProtocol
    from logdb.logdb_api import Client

    server, port = server.split(':')
    transport = TSocket.TSocket(server, int(port))
    transport.open()

    trans = TTransport.TFramedTransport(transport)
    protocol = TBinaryProtocol.TBinaryProtocol(trans)
    return Client(protocol)


def run_tests():
    client = get_client('localhost:2111')

    from logdb.ttypes import LogItem, ScanReq, ScanResp, PushReq, RangeReq, Action

    geek_offset, repeat, key, db = 1000, 10, "10101", 1
    msg = "this is a test"

    client.Delete(key, db)
    p = PushReq(key=key, db=db, logs=[])
    for i in range(repeat):
        li = LogItem(action=Action.CHAT, ts=i, msg=msg, geekId=i + geek_offset)
        p.logs.append(li)

    client.Push([p])
    rr = RangeReq(key=key, start=-1000, last=-1, db=db)
    resp = client.Range(rr)
    assert len(resp) == repeat
    assert resp[0].msg == msg
    assert resp[0].geekId == geek_offset

    resps = client.Ranges([rr, RangeReq(key="not exits", start=0, last=-1, db=db)])
    assert len(resps[0]) == repeat
    assert len(resps[1]) == 0

    # test max_val
    repeat, db, key2 = 1000, 0, "12102102"
    client.Delete(key, db)
    for i in range(10):
        li = LogItem(action=Action.CHAT, ts=i, msg=msg, geekId=i + geek_offset)
        client.Push([PushReq(key=key, db=db, logs=[li] * (repeat / 100))])

    rr = RangeReq(key=key, start=-1000, last=-1, db=db)
    resp = client.Range(rr)
    assert len(resp) < repeat
    assert resp[0].msg == msg

    print "all tests pass"


if __name__ == '__main__':
    run_tests()
