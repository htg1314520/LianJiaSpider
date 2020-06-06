from redis import StrictRedis


class HashMap:
    def __init__(self, m, seed):
        self.m = m
        self.seed = seed

    def hash(self, value):
        ret = 0
        for i in range(len(value)):
            ret += self.seed * ret + ord(value[i])
        return (self.m - 1) & ret


BLOOMFILTER_HASH_NUMBER = 6
BLOOMFILTER_BIT = 30


class BloomFilter:
    def __init__(self, server, key, bit=BLOOMFILTER_BIT, hash_number=BLOOMFILTER_HASH_NUMBER):
        self.m = 1 << bit
        self.seeds = range(hash_number)

        self.maps = [HashMap(self.m, seed) for seed in self.seeds]

        self.server = server

        self.key = key

    def exists(self, value):
        if not value:
            return False

        exist = 1
        for map in self.maps:
            offset = map.hash(value)
            exist = exist & self.server.getbit(self.key, offset)

        return exist

    def insert(self, value):
        for f in self.maps:
            offset = f.hash(value)
            self.server.setbit(self.key, offset, 1)


if __name__ == '__main__':
    conn = StrictRedis(host="localhost", port=6379)

    bf = BloomFilter(conn, 'testbf', 5, 6)
    if bf.exists('http://www.baidu.com'):  # 判断字符串是否存在
        print('exists!')
    else:
        print('not exists!')
        bf.insert('http://www.baidu.com')
