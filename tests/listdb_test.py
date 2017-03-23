# encoding: utf8
__author__ = 'feng'
import sys

reload(sys)
sys.setdefaultencoding('utf-8')
import os
import logging

DIR = os.path.abspath(os.path.dirname(__file__))
if '%s/gen-py' % DIR not in sys.path:
    sys.path.append('%s/gen-py' % DIR)
if DIR not in sys.path:
    sys.path.append(DIR)
FORMAT = '%(asctime)-15s  %(message)s'
logging.basicConfig(format=FORMAT, level=logging.INFO)

from api.ttypes import PushArg, RangeArg, ScanArg
from api.Listdb import Client


def get_client(server):
    from thrift.transport import TSocket, TTransport
    from thrift.protocol import TBinaryProtocol

    server, port = server.split(':')
    transport = TSocket.TSocket(server, int(port))
    transport.open()

    trans = TTransport.TFramedTransport(transport)
    protocol = TBinaryProtocol.TBinaryProtocol(trans)
    return Client(protocol)


def run_tests():
    # 1 . test get set

    client = get_client("localhost:6571")
    key = "my-key"

    for i in range(1000):
        run_test_short(client, key)
        run_tests_long(client, key)
        test_scan(client)
        # import time
        # time.sleep(1)
        logging.info("all tests pass")


def test_scan(client):
    prefx = 'key-xxxxx'
    db = 2
    total = 1000 - 1
    for i in range(total):
        key = '%s-%d' % (prefx, i)
        client.Delete(key, db)
        client.Push(PushArg(key=key, db=db, datas=["value-%s-%d" % (prefx, i) for i in range(3)]))

    for i in range(5):
        # pass
        resp = client.Scan(ScanArg(db=db, limit=100, cursor='0'))

    keys = []
    cursor = '-1'
    while cursor != '0':
        resp = client.Scan(ScanArg(db=db, limit=100, cursor=cursor))
        cursor = resp.cursor
        for k in resp.keys:
            keys.append(k.key)

    assert len(keys) == total
    assert len(set(keys)) == total


def run_test_short(client, key):
    client.Delete(key, 0)
    resp = client.Range(RangeArg(key=key, start=0, last=100, db=0))
    assert len(resp) == 0
    client.Push(PushArg(key=key, db=0, datas=["恒泰value %d" % i for i in range(3)]))
    resp = client.Range(RangeArg(key=key, start=0, last=10, db=0))
    assert len(resp) == 3
    assert resp[2] == "恒泰value 2"
    resp = client.Range(RangeArg(key=key, start=0, last=-1, db=0))
    assert len(resp) == 3
    assert resp[2] == "恒泰value 2"
    resp = client.Range(RangeArg(key=key, start=1, last=-1, db=0))
    assert len(resp) == 2
    assert resp[1] == "恒泰value 2"


def run_tests_long(client, key):
    client.Delete(key, 0)
    resp = client.Range(RangeArg(key=key, start=-2, last=-1, db=0))
    assert len(resp) == 0

    val = "a" * 100
    # add more
    client.Push(PushArg(key=key, db=0, datas=[val for i in range(3)]))
    resp = client.Range(RangeArg(key=key, start=-2, last=-1, db=0))
    assert len(resp) == 2
    assert resp[1] == val == resp[0]

    client.Delete(key, 0)
    resp = client.Range(RangeArg(key=key, start=-2, last=-1, db=0))
    assert len(resp) == 0


if __name__ == '__main__':
    run_tests()
