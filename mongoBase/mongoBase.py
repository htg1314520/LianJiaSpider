from pymongo import MongoClient
from mongoBase.mongoOperation import BaseHandle

HoST = "127.0.0.1"
PORT = 27017


class MonGo:
    def __init__(self, dbname):
        self.client = MongoClient(HoST, PORT)
        self.db = self.client[dbname]


class DBBase(MonGo):
    def __init__(self, dbname, collection):
        super(DBBase, self).__init__(dbname)
        self.collection = self.db[collection]

    def insert_one(self, data):
        resId = BaseHandle.insert_one(self.collection, data)
        return resId

    def insert_many(self, data_list):
        resIds = BaseHandle.insert_many(self.collection, data_list)
        return resIds

    def find_one(self, data, data_field={}):
        resId = BaseHandle.find_one(self.collection, data, data_field)
        return resId

    def find_many(self, data, data_field={}):
        """ 有多个键值的话就是 AND 的关系"""
        resIds = BaseHandle.find_many(self.collection, data, data_field)
        # 返回结果是Cursor类型相当于一个生成器
        return resIds

    def find_all(self, data={}, data_field={}):
        """select * from table"""
        res = BaseHandle.find_many(self.collection, data, data_field)
        return res

    def find_in(self, field, item_list, data_field={}):
        """SELECT * FROM inventory WHERE status in ("A", "D")"""
        data = dict()
        data[field] = {"$in": item_list}
        res = BaseHandle.find_many(self.collection, data, data_field)
        return res

    def find_or(self, data_list, data_field={}):
        """db.inventory.find( {"$or": [{"status": "A"}, {"qty": {"$lt": 30}}]} )
        SELECT * FROM inventory WHERE status = "A" OR qty < 30
        """
        data = dict()
        data["$or"] = data_list
        res = BaseHandle.find_many(self.collection, data, data_field)
        return res

    def find_between(self, field, value1, value2, data_field={}):
        """获取俩个值中间的数据"""
        data = dict()
        data[field] = {"$gt": value1, "$lt": value2}
        res = BaseHandle.find_many(self.collection, data, data_field)
        return res

    # db.users.find({name: {$exists: false}});{"Latitude":null}
    def find_exists(self, field, value, data_field={}):
        """db.inventory.find( {"$or": [{"status": "A"}, {"qty": {"$lt": 30}}]} )
        SELECT * FROM inventory WHERE status = "A" OR qty < 30
        """
        data = dict()
        data[field] = {"$exists": value}
        res = BaseHandle.find_many(self.collection, data, data_field)
        return res

    def find_more(self, field, value, data_field={}):
        data = dict()
        data[field] = {"$gt": value}
        res = BaseHandle.find_many(self.collection, data, data_field)
        return res

    def find_less(self, field, value, data_field={}):
        data = dict()
        data[field] = {"$lt": value}
        res = BaseHandle.find_many(self.collection, data, data_field)
        return res

    def find_like(self, field, value, data_field={}):
        """ where key like "%audio% """
        data = dict()
        data[field] = {'$regex': '.*' + value + '.*'}
        res = BaseHandle.find_many(self.collection, data, data_field)
        return res

    def query_limit(self, query, num):
        """
        db.collection.find(<query>).limit(<number>)
        获取指定数据
        """
        res = query.limit(num)
        return res

    def query_count(self, query):
        res = query.count()
        return res

    def query_skip(self, query, num):
        res = query.skip(num)
        return res

    def query_sort(self, query, data):
        """
        db.orders.find().sort( { amount: -1 } ) 根据amount 降序排列
        """
        res = query.sort(data)
        return res

    def delete_one(self, data):
        """
        删除单行数据 如果有多个 则删除第一个
        """
        res = BaseHandle.delete_one(self.collection, data)
        return res

    def delete_many(self, data):
        """
        删除查到的多个数据 data 是一个字典
        """
        res = BaseHandle.delete_many(self.collection, data)
        return res

    def update_setOnInsert(self, data, key):
        """
        不存在则插入，存在则不操作
        :return:

        """
        res = BaseHandle.update_setOnInsert(self.collection, data, key)
        return res

    def update_set(self, data, key):
        """
        根据条件判断有无记录，有的话就更新记录，没有的话就插入一条记录
        :return:

        """
        res = BaseHandle.update_set(self.collection, data, key)
        return res


if __name__ == '__main__':
    # data = [{"name": "lxb", "age": 23}, {"name": "zzz", "age": 24}]
    data = {'name': "AAA", "age": 30}
    db = DBBase('module', 'ceshi')
    # print(db.insert_many(data))
    # print(db.update_True(data, "name").modified_count)
