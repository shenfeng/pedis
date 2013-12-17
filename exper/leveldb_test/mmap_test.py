import mmap
import argparse
import logging
import os
from os import path
import random
import string
import time

FORMAT = '%(asctime)-15s  %(message)s'
logging.basicConfig(format=FORMAT, level=logging.INFO)

parser = argparse.ArgumentParser(description="Parse Search query log")
parser.add_argument('-n', type=int, default=1, help='file count')
parser.add_argument('-s', type=int, default=100, help='file size in mb')
parser.add_argument('--loop', type=int, default=1000, help='loop count')
parser.add_argument('--dir', type=str, default="/home/feng/mmap_tmp", help='test dir')

args = parser.parse_args()


def rand_chars(n):
    r = int(random.random() * len(string.ascii_letters))
    return string.ascii_letters[r] * n


def get_stats(times):
    times = sorted(times)
    pos_50 = times[int(len(times) * 0.50)]
    pos_80 = times[int(len(times) * 0.80)]
    pos_95 = times[int(len(times) * 0.95)]
    pos_99 = times[int(len(times) * 0.99)]
    return "50%%=%.4fms, avg=%.4fms 80%%=%.4fms, 95%%=%.4fms, 99%%=%.4fms, max=%.4fms" % (
        pos_50, sum(times) / len(times), pos_80, pos_95, pos_99, max(times))


def main():
    if not path.exists(args.dir):
        logging.info('create dir: %s', args.dir)
        os.makedirs(args.dir)

    read_size = 1024 * 16 # read/write 16k a time
    times, mmaps, size = [], [], args.s * 1024 * 1024 # file size in M

    for i in range(args.n): # make sure file is the right size
        f = open('%s/file-%s' % (args.dir, i), 'a+b')
        os.ftruncate(f.fileno(), size)

    for i in range(args.n):  # mmap, record the time
        start = time.time()
        f = open('%s/file-%s' % (args.dir, i), 'a+b')
        mmaps.append(mmap.mmap(f.fileno(), 0))
        times.append((time.time() - start) * 1000)  # in ms

    logging.info('mmap %d files, each %dM: %s', args.n, args.s, get_stats(times))

    for i in xrange(args.loop):
        for m in mmaps:
            loc = int(random.random() * size) - read_size
            if loc < 0:
                loc = 0
            if loc % 20:  # ~95% read
                c = len(m[loc:loc + read_size])
            else:  # ~5% write
                m[loc:loc + read_size] = rand_chars(read_size)


if __name__ == "__main__":
    main()
