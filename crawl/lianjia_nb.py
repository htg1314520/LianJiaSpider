import logging
import pathlib
import random
import time
import copy

import re
import yaml
import requests
import concurrent.futures
import logging.config

from config.header_config import UA, target_house, log_config_file_path, spider_cofig_file_path, filename
from mongoBase.mongoBase import DBBase
from utils.bloomBase import BloomFilter
from utils.notice import send_mail
from utils.redisBase import RedisClient
from scrapy import Selector
from functools import wraps
from concurrent.futures import as_completed
from urllib.parse import urljoin


def decorator_time(func):
    """
    时间统计装饰器
    :param func:
    :return:
    """

    @wraps(func)
    def inner(*args, **kwargs):
        local_time = time.time()
        result_url = func(*args, **kwargs)
        print('current Function [%s] run time is %.2f' % (func.__name__, time.time() - local_time))
        return result_url

    return inner


class LianJia:
    def __init__(self):
        # 初始化日志配置
        try:
            with open(log_config_file_path, 'r') as f:
                # 将字符串转化为字典或列表
                log_config = yaml.load(f)
                # 字典配置信息和dictConfig()函数实现日志配置
                logging.config.dictConfig(log_config)

            self.lianjia_spider_log = logging.getLogger('spider')
            self.lianjia_spider_log.info('Logger init success')
        except Exception as err:
            self.lianjia_spider_log.error('Logger初始化失败' + str(err))

        # 初始化数据库配置
        try:
            with open(spider_cofig_file_path, 'r') as f:
                spider_config = yaml.load(f)

                self.lianjia_spider_log.info('Config init success')
        except Exception as err:
            self.lianjia_spider_log.error('Config初始化失败' + str(err))
            return

        self.pattern_position = re.compile(r"resblockPosition:'(.*?),(.*?)',")
        # 请求过期时间
        self.timeout = spider_config['timeout']
        self.bflianjia = spider_config['bloomFilter']
        self.lianjia = spider_config['houseurl']
        self.headers = spider_config['Headers']

        # 获取redis链接
        self.redis_client = RedisClient(spider_config['redis'])
        self.conn = self.redis_client.get_conn

        self.db = DBBase(spider_config['Mongo']['db'], spider_config['Mongo']['collection'])

        # 初始化爬取列表页
        path = pathlib.Path(filename)

        if path.exists():
            self.areaUrl = self.read_url_from_txt(filename)
        else:
            self.areaUrl = self.get_area_url(filename)

        self.lianjia_spider_log.info(f"init url:{len(self.areaUrl)}")

    @decorator_time
    def get_area_url(self, filename):
        """
        获取细分到每个区域的房屋链接 146 + N
        :return:
        """
        result_url = list()
        # 宁波和杭州的二手房列表首页
        start_url = target_house
        headers = copy.deepcopy(self.headers)
        for url in start_url:
            headers['User-Agent'] = random.choice(UA)
            resp = requests.get(url, headers=headers)
            response = Selector(resp)
            DistrictList = response.xpath("//div[@data-role='ershoufang']/div/a/@href").extract()
            for district in DistrictList:
                headers['User-Agent'] = random.choice(UA)
                districtResp = requests.get(urljoin(url, district), headers=headers)
                districtResponse = Selector(districtResp)
                # 获取最小化的区域
                streetList = districtResponse.xpath("//div[@data-role='ershoufang']/div[2]/a/@href").extract()
                streetList = [urljoin(url, streeturl) for streeturl in streetList]
                result_url.extend(streetList)

        # 存到文件中方便日后调试省去获取所有小区域的步骤
        with open(filename, "w", encoding="utf8") as f:
            for url in result_url:
                f.write(url + '\n')

        return result_url

    def read_url_from_txt(self, filename):
        self.lianjia_spider_log.info("read url from txt...")
        result_url = list()

        with open(filename, "r") as f:
            while True:
                line = f.readline()  # 逐行读取
                if not line:  # 到 EOF，返回空字符串，则终止循环
                    break
                result_url.append(line.strip())

        return result_url

    @decorator_time
    def get_detail_url(self):
        """
        获取所有详情页链接
        :return:
        """
        headers = copy.deepcopy(self.headers)
        page_parttern = r"page-data='{\"totalPage\":(\d+),\"curPage\":1}'"
        # 遍历所有区块
        for url in self.areaUrl:
            house_url = url
            page, total_page = 1, 1
            # 某个区块的全部详情页
            while True:
                headers['User-Agent'] = random.choice(UA)
                resp = requests.get(house_url, headers=headers)
                response = Selector(resp)

                # 正则提取总页数与模板链接
                if page == 1:
                    try:
                        total_page = int(re.search(page_parttern, resp.text, re.S | re.I).group(1))
                        pageurl = re.search("page-url=\"(.*?)\"", resp.text, re.S).group(1)
                    except Exception as err:
                        print(f"the current error:{err}")
                        break

                detail_urls = response.xpath("//div[@class='item']/a[@class='img']/@href").extract()

                if detail_urls:
                    # 用bloom_filter进行url去重
                    bf = BloomFilter(self.conn, self.bflianjia, 30, 6)
                    for detail_url in detail_urls:
                        # 判断字符串是否存在
                        if bf.exists(detail_url):
                            print('url exists!')
                        else:
                            bf.insert(detail_url)
                            print("add url to redis success...")
                            self.redis_client.lpush(self.lianjia, detail_url)

                    # 如果下一页有链接则翻页
                    if page < total_page:
                        page = page + 1
                        house_url = urljoin(house_url, pageurl.format(page=page))
                    else:
                        break

                time.sleep(self._set_random_sleep_time())

            self.lianjia_spider_log.info(f"单个区块详情爬取完毕：{url}")
        self.lianjia_spider_log.info("All Urls Finished!!")

    def get_house_info(self):
        # detail_url = self.redis_client.lpop(self.lianjia)[0]
        detail_url = "https://nb.lianjia.com/ershoufang/103109030954.html"
        if not detail_url:
            return

        proxy_ip, proxies, score = self.get_ip_from_pool()
        while True:
            # 出现异常或访问失败则进行无限重试
            headers = copy.deepcopy(self.headers)
            headers['User-Agent'] = random.choice(UA)
            try:
                resp = requests.get(detail_url, headers=headers, timeout=10, proxies=proxies)
                break

            except requests.exceptions.ConnectionError as err:
                print(f'{err}:Timeout代理{proxy_ip}当前分数{score}减3')
                self.redis_client.connection_client.zincrby("YRYProxy", -3, proxy_ip)
                proxy_ip, proxies, score = self.get_ip_from_pool()

        if resp.history and resp.history[0].status_code == 302:
            # 采集失败把链接再放回去
            print("发生重定向，可能是跳转到验证码页面...")
            self.redis_client.lpush(self.lianjia, detail_url)
        elif resp.status_code == 200:
            self.lianjia_spider_log.info(f"开始解析并保存{detail_url}")
            self.parse_house_info(resp)

        return detail_url

    def parse_house_info(self, resp):
        """
        解析二手房信息
        :return:
        """
        item = dict()
        response = Selector(resp)
        generalXpath = "//span[text()='{}']/../text()"
        # 链家编号
        item['houseCode'] = response.xpath("//div[@class='houseRecord']/span[2]/text()").extract_first("").strip()
        # 小区名
        item['houseName'] = response.xpath("//div[@class='communityName']/a[1]/text()").extract_first("").strip()
        # 朝向
        item['houseDirection'] = response.xpath(generalXpath.format("房屋朝向")).extract_first("").strip()
        # 户型
        item['houseType'] = response.xpath(generalXpath.format("房屋户型")).extract_first("").strip()
        # 电梯
        item['houseElevator'] = response.xpath(generalXpath.format("配备电梯")).extract_first("").strip()
        # 区域
        item['houseAddress'] = response.xpath("//div[@class='areaName']/a/text()").extract_first("").strip()
        item['houseDistrict'] = response.xpath(
            "//div[@class='areaName']/span[@class='info']/a[2]/text()").extract_first("").strip()
        item['houseRegion'] = response.xpath("//div[@class='areaName']/span[@class='info']/a[1]/text()").extract_first(
            "").strip()
        # 楼层
        item['houseFloor'] = response.xpath(generalXpath.format("所在楼层")).extract_first("").strip()
        # 建筑面积
        item['houseSize'] = response.xpath(generalXpath.format("建筑面积")).extract_first("").strip()
        # 装修情况
        item['houseStatus'] = response.xpath(generalXpath.format("装修情况")).extract_first("").strip()
        # 每平米价格
        item['houseUnitPrice'] = response.xpath("//span[@class='unitPriceValue']/text()").extract_first("").strip()
        # 总价
        item['houseAllPrice'] = response.xpath(
            "//div[@class='price ']/span[@class='total']/text()").extract_first("").strip()
        # 建设时间
        item['houseYear'] = response.xpath("//div[@class='area']/div[@class='subInfo']/text()").re_first(r"(\d+)")

        # 原文链接
        item['url'] = resp.url

        # 经纬度
        postions = self.pattern_position.search(resp.text)
        # 获取坐标
        item['Longitude'] = postions.group(1)
        item['Latitude'] = postions.group(2)
        self.db.update_set('houseCode', item)
        self.lianjia_spider_log.info(f'parse item success:{resp.url}')

    def _set_random_sleep_time(self):
        """
        设置随机睡眠时间
        :return:
        """
        return random.randint(1, 3)

    def get_ip_from_pool(self):
        """
        获取代理
        :return:
        """
        while True:
            data = self.redis_client.score_range('YRYProxy', -1, 5)
            if data:
                break

        proxyip, score = random.choice(data)
        proxies = {"http": f"http://{proxyip}", "https": f"https://{proxyip}"}

        return proxyip, proxies, score

    def run(self):
        """
        启动函数
        :return:
        """
        # self.get_detail_url()
        total = self.redis_client.llen(self.lianjia)
        self.lianjia_spider_log.debug(f"total:{total}")
        if total > 0:
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                all_task = [executor.submit(self.get_house_info) for _ in range(1)]
                for future in as_completed(all_task):
                    data = future.result()
                    print("in main: get page {} success".format(data))

            self.lianjia_spider_log.info("Crawl Finished!!")
            send_mail("Crawl Finished!!")


if __name__ == '__main__':
    lj = LianJia()
    lj.run()
