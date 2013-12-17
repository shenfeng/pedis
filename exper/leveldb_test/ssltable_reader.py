__author__ = 'feng'

# https://github.com/dowski/misc/blob/master/varints.py
def encode_varint(value):
    """Encodes a single Python integer to a VARINT."""
    return "".join(encode_varint_stream([value]))


def decode_varint(value):
    """Decodes a single Python integer from a VARINT.

    Note that `value` may be a stream containing more than a single
    encoded VARINT. Only the first VARINT will be decoded and returned. If
    you expect to be handling multiple VARINTs in a stream you might want to
    use the `decode_varint_stream` function directly.

    """
    return decode_varint_stream(value).next()


def encode_varint_stream(values):
    """Lazily encodes an iterable of Python integers to a VARINT stream."""
    for value in values:
        while True:
            if value > 127:
                # Yield a byte with the most-significant-bit (MSB) set plus 7
                # bits of data from the value.
                yield chr((1 << 7) | (value & 0x7f))

                # Shift to the right 7 bits to drop the data we've already
                # encoded. If we've encoded all the data for this value, set the
                # None flag.
                value >>= 7
            else:
                # This is either the last byte or only byte for the value, so
                # we don't set the MSB.
                yield chr(value)
                break


def decode_varint_stream(stream):
    """Lazily decodes a stream of VARINTs to Python integers."""
    value = 0
    base = 1
    for raw_byte in stream:
        val_byte = ord(raw_byte)
        value += (val_byte & 0x7f) * base
        if (val_byte & 0x80):
            # The MSB was set; increase the base and iterate again, continuing
            # to calculate the value.
            base *= 128
        else:
            # The MSB was not set; this was the last byte in the value.
            yield value
            value = 0
            base = 1


def varintSize(value):
    """Compute the size of a varint value."""
    if value <= 0x7f: return 1
    if value <= 0x3fff: return 2
    if value <= 0x1fffff: return 3
    if value <= 0xfffffff: return 4
    if value <= 0x7ffffffff: return 5
    if value <= 0x3ffffffffff: return 6
    if value <= 0x1ffffffffffff: return 7
    if value <= 0xffffffffffffff: return 8
    if value <= 0x7fffffffffffffff: return 9
    return 10


def varintsSize(*values):
    return sum(varintSize(v) for v in values)


buffer = open('/tmp/testdb/000005.ldb').read()
print len(buffer)


def hex_dump(buf):
    print ':'.join(x.encode('hex') for x in buf)


footer = buffer[-48:]
print len(footer)
print ':'.join(x.encode('hex') for x in footer)

print '-----------------------'

meta_off, meta_size, index_off, index_size = list(decode_varint_stream(footer))[:4]


# print meta_off, meta_size, index_off, index_size
# print buffer[meta_off:meta_off + meta_size]
# print
# print
index_block = buffer[index_off: index_off + index_size]

idx_buffer = buffer[index_off: index_off + index_size]
# print hex_dump(idx_buffer[:50])
sizes = list(decode_varint_stream(idx_buffer))[:3]

print index_block
# print varintsSize(*sizes)

# print '----------------------------------@@@@@@@@@@@@@'

def dump_entry(buf):
    first_sizes = list(decode_varint_stream(buf[:20]))[:3]

    size = sum(first_sizes[1:]) + varintsSize(*first_sizes)

    print first_sizes, size
    print buf[:size]


    # print buf[:size], size
    return size, first_sizes


print '-' * 100

idx = 5
while idx:
    size, sizes = dump_entry(index_block)
    off = varintsSize(*sizes) + sizes[1]
    print '~' * 10
    print list(decode_varint_stream(index_block[off: off + 10]))

    index_block = index_block[size:]
    idx -= 1


# print '=================='




print meta_off, meta_size, '=================='
meta_idx_buffer = buffer[meta_off:meta_off + meta_size]
size, sizes = dump_entry(meta_idx_buffer)
print size, sizes
off = varintsSize(*sizes) + sizes[1]
print list(decode_varint_stream(meta_idx_buffer[off: off + 10]))

