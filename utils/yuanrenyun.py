import redis
import requests
from apscheduler.schedulers.blocking import BlockingScheduler


class YuanRen:
    pool = redis.ConnectionPool(db=1, host='localhost', port=6379,  decode_responses=True)
    r = redis.Redis(connection_pool=pool)

    def __init__(self, yid, ysecret):
        self.yid = yid
        self.ysecret = ysecret
        self.count = 5
        self.score = 5
        self.ttl = 1000

    def check_ip(self):
        """
        监控 IP 分数、个数，对其进行增删
        """
        # 检查分数
        nodes = self.r.zrevrange('YRYProxy', 0, -1, withscores=True)
        print(f"nodes:{nodes}")
        for i in nodes:
            node = list(i)
            score = int(node[1])

            if score <= 0:
                print('\033[1;33m分数过低剔除\033[0m')
                self.r.zrem('YRYProxy', node[0])

        # 检查个数
        _sum = self.r.zcard('YRYProxy')
        print("sum:", _sum)

        if _sum < self.count:
            self.add_ip()

    def add_ip(self, count=3):
        """
        提取IP
        """
        get_url = f"http://tunnel-api.apeyun.com/q?id={self.yid}&secret={self.ysecret}&limit={count}&format=json&auth_mode=auto"
        # 返回的文本进行解析
        response = requests.get(get_url)

        if response.status_code == 200:
            ret = response.json()
            if ret.get('code') == 200:
                self.parse(ret)
            elif ret.get('code') == '11020012':
                print('十秒内可提取IP数已用完...')
            elif ret.get('code') == "11020001":
                print("订单已过期...")
        else:
            print('提取失败')

    def parse(self, data):
        """
        解析返回数据,存储IP和分数到redis
        """
        proxy_list = data.get('data')
        for node in proxy_list:
            proxy = f"{node.get('ip')}:{node.get('port')}"
            self.save_to_redis(proxy, self.score)

        print("save success")

    def save_to_redis(self, proxy, score):
        """
        推送到redis集合中
        """
        print('代理 %s 推入redis集合' % proxy)
        # 此写法仅支持python-redis 3.5.2以上版本
        self.r.zadd('YRYProxy', {proxy: score})


def aps_run(proxy):
    """
    定时检测IP
    """
    proxy.check_ip()


if __name__ == '__main__':
    print("代理池启动。。。。")
    ID = "2120060100028401846"
    SECRET = "xxx"
    yry = YuanRen(ID, SECRET)
    # 循环监控
    scheduler = BlockingScheduler()
    scheduler.add_job(aps_run, 'cron', second='*/2', args=[yry], max_instances=10)  # 设置检测，推荐2s一次(默认)
    scheduler.start()
