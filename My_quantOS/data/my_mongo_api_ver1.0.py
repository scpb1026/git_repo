# encoding: utf-8

import pandas as pd
from pymongo import MongoClient
import pymongo


class MyMongoApi():
    '''
    根据给定参数, 查询mongoDB的相应数据
    查询主方法:   query
    -----------------------------------------------------------------------------
    参数                      说明
    view         查询内容的类别, 即mongoDb的db名字，类型str    
    start_date   开始日期, 类型int, 例如20170101
    end_date     结束日期, 类型int, 例如20170101
    symbol       查询股票代码, 类型str, 逗号隔开, 不能有空格, 例如'600000.SH,000001.SZ'
    fields       查询字段, 类型str, 逗号隔开, 不能有空格, 默认有trade_date和symbol, 例如'open,close,pe,pb'
    '''

    def __init__(self, host='localhost', port=27017):
        self.host = host
        self.port = port
        self.client = None
        self.db = None
        self.col = None
        self.start_date = None
        self.end_date = None
        self.symbol = None
        self.fields = None


    def login(self, db_name, col_name):
        "登录到mongoDB, 判断db和collection是否存在, 同时存在则返回True, 否则返回False"
        self.client = MongoClient(self.host, self.port)
        if db_name not in self.client.list_database_names():
            print("Database name is not exist.")
            return False
        else:
            self.db = self.client[db_name]
            if col_name not in self.db.collection_names():
                print("Collection name is not exist.   Symbol: {}".format(col_name))
                return False
            else:
                self.col = self.db[col_name]
                print("Login success.")
                print("Query data of symbol: {}".format(col_name))
                return True


    def logout(self):
        "登出mongoDB"
        self.client.close()
        print("Logout.")


    def _cursor2df(self, cursor_):
        '''
        将数据库取出的cursor文件, 转换成DataFrame
        转换思路:
        1. 用line历遍cursor_
        2. line转换为Series
        3. 形成Series组成的列表
        4. 最后将Series的列表转换成DataFrame
        '''
        # 判断cursor_是否pymongo.cursor.Cursor类型
        if isinstance(cursor_, pymongo.cursor.Cursor):
            series_list = []
            for line in cursor_:
                series_list.append(pd.Series(line))
            df = pd.DataFrame(series_list)
            return df
        else:
            return pd.DataFrame()


    def _str2list(self, str_):
        "str转换为list, 分隔符为',' "
        if isinstance(str_, str):
            res = str_.split(',')
            return res


    def _list2str(self, list_):
        "list转换为str, 分隔符为',' "
        if isinstance(list_, list):
            res = ','.join(list_)
            return res


    def _query_symbol(self):
        "构造symbol的查询条件，返回list"
        query_symbol = self._str2list(self.symbol)
        return query_symbol


    def _query_date(self):
        '''
        构造pymongo.find方法的trade_date的查询条件, 返回dict
        例如:
        collection.find({'trade_date': {'$gte': start_date, '$lte': end_date}})
        代表查询trade_date列中大于等于start_date, 并小于等于end_date的部分
        '''
        query_date = {'trade_date': {
            '$gte': self.start_date, '$lte': self.end_date}}
        return query_date


    def _query_fields(self):
        '''
        构造pymongo.find方法的fields的查询条件, 返回dict
        例如:
        collection.find({}, {'trade_date': 1, '_id': 0, 'fields': 1})
        代表查询trade_date和fields两个列的所有数据，并且不显示_id列(默认是显示的)
        '''
        fields = self._str2list(self.fields)
        keys = ['trade_date', '_id'] + [i for i in fields]
        values = [1, 0] + [1] * len(fields)
        query_fields = dict(zip(keys, values))
        return query_fields


    def query(self, view, start_date, end_date, symbol, fields):
        "根据字段查询数据的主函数"
        self.start_date = start_date
        self.end_date = end_date
        self.symbol = symbol
        self.fields = fields

        df = pd.DataFrame()
        for code in self._query_symbol():
            self.db = view
            self.col = code
            # 按照字段要求, 取出数据
            cursor = self._get_data()
            # cursor转换为dataframe
            df_temp = self._cursor2df(cursor)
            # 增加symbol列, 并拼接不同symbol的dataframe
            df_temp['symbol'] = code
            df = pd.concat([df, df_temp])
            df.index = range(len(df))
        return df


    def _get_data(self):
        "根据字段从数据库取出数据的内部实现方法"
        try:
            # login方法, 判断db和collection同时存在于数据库, 返回True
            if self.login(self.db, self.col):
                # 使用pymongo.client.db.collection.find()方法
                res = self.col.find(self._query_date(), self._query_fields())
                return res
        except Exception as e:
            print(e)
        finally:
            self.logout()



if __name__ == '__main__':
    api = MyMongoApi()
    x = api.query('reference_daily_data_of_stock', 20041001,
                  20041101, symbol='600000.SH,600016.SH,600300.SH', fields='pe,pb')
    print(x)
