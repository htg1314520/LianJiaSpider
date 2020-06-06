"""
@file:lianjiaPosition.py
@time:2019/11/19-10:42
@info:协程补充数据经纬度采集
"""
import random
import re

import aiohttp
import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
import async_timeout

from config.header_config import UA
from mongoBase.mongoBase import DBBase

semaphore = asyncio.Semaphore(20)
Headers = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3",
    "accept-language": "zh-CN,zh;q=0.9"
}
PATTENPOSITION = re.compile(r"resblockPosition:'(.*?),(.*?)',")


class MotorBase:
    _db = {}
    _collection = {}

    def __init__(self, loop=None):
        self.motor_uri = ''
        self.loop = loop or asyncio.get_event_loop()

    def client(self, db):
        self.motor_uri = f"mongodb://localhost:27017/{db}"
        return AsyncIOMotorClient(self.motor_uri, io_loop=self.loop)

    def get_db(self, db='BiliBiliData'):
        if db not in self._db:
            self._db[db] = self.client(db)[db]

        return self._db[db]


async def update_data(item):
    mb = MotorBase().get_db('House')
    try:
        await mb.lianjiaNew.update_one({
            'houseCode': item.get("houseCode")},
            {'$set': {'Longitude': item.get('Longitude'), 'Latitude': item.get('Latitude')}},
            upsert=True)
        print(f"{item['houseCode']} update ok!!")
    except Exception as e:
        print("数据插入出错", e.args, "此时的item是", item)


# resblockPosition:'120.100555,30.272743',
async def fetch(item):
    async with semaphore:
        async with aiohttp.ClientSession() as session:
            with async_timeout.timeout(20):
                try:
                    Headers['User-Agent'] = random.choice(UA)
                    async with session.get(item['url'], headers=Headers) as res:
                        if res.status in [200, 201]:
                            text = await res.text()
                            postions = PATTENPOSITION.search(text)
                            # 获取坐标
                            item['Longitude'] = postions.group(1)
                            item['Latitude'] = postions.group(2)
                            await asyncio.sleep(2)
                            await update_data(item)
                        else:
                            raise "spider had found!!"
                except Exception as err:
                    print(err)


if __name__ == '__main__':
    db = DBBase('House', 'lianjiaNew')
    items = db.find_exists("Latitude", False)
    all_tasks = [fetch(item) for item in items]
    loop = asyncio.get_event_loop()
    loop.run_until_complete(asyncio.gather(*all_tasks))
    print("update Finished!!")
