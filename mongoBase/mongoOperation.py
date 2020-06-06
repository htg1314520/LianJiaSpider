class BaseHandle:
    @staticmethod
    def insert_one(collection, data):
        res = collection.insert_one(data)
        return res.inserted_id

    @staticmethod
    def insert_many(collection, data_list):
        res = collection.insert_many(data_list)
        return res.inserted_ids

    # find_one()查询得到是单个结果，find()则返回Cursor生成器，可得多个结果。
    @staticmethod
    def find_one(collection, data, data_field={}):
        if len(data_field):
            res = collection.find_one(data, data_field)
        else:
            res = collection.find_one(data)
        return res

    @staticmethod
    def find_many(collection, data, data_field={}):
        """ data_field 是指输出操作者需要的字段{"filed1":1,"field2":0}"""
        if len(data_field):
            res = collection.find(data, data_field)
        else:
            res = collection.find(data)
        return res

    @staticmethod
    def update_one(collection, data_condition, data_set):
        """修改一条数据"""
        res = collection.update_one(data_condition, data_set)
        return res

    @staticmethod
    def update_many(collection, data_condition, data_set):
        """ 修改多条数据 """
        res = collection.update_many(data_condition, data_set)
        return res

    @staticmethod
    def replace_one(collection, data_condition, data_set):
        """ 完全替换掉 这一条数据， 只是 _id 不变"""
        res = collection.replace_one(data_condition, data_set)
        return res

    @staticmethod
    def delete_many(collection, data):
        res = collection.delete_many(data)
        return res

    @staticmethod
    def delete_one(collection, data):
        res = collection.delete_one(data)
        return res

    @staticmethod
    def update_setOnInsert(collection, results, key):
        res = collection.update_one({key: results[key]}, {'$setOnInsert': results}, upsert=True)
        return res

    @staticmethod
    def update_set(collection, key, results):
        res = collection.update_one({key: results[key]}, {'$set': results}, upsert=True)
        return res
