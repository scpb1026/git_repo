# encoding: utf-8
# Author:   Scpb1026
# Github:   https://github.com/scpb1026
# Version:  0.1.0

import sys
import time
from datetime import datetime, timedelta
import pymongo
import pandas as pd
from multiprocessing.dummy import Pool as ThreadPool

from jaqs.data.dataapi import DataApi
from jaqs.data.dataservice import RemoteDataService
from jaqs.data.dataview import DataView


class MyTushareApi():
    def __init__(self):
        self.universe = []
        self.symbol = ""
        self.fields = ""
        self.start_date = 0
        self.end_date = 0
        self._data_status = 0
        self._data_init_config = {}
        self._client = None
        self._db = None
        self._col = None
        self._db_list = []
        self._col_list = []
        self._folder_path = './log/'
        self.reference = {
            "lb.secDailyIndicator": "pe, pe_ttm, pb, ps, ps_ttm, net_assets",
            # 可扩展view和fileds
            }


    def _login(self, dataapi=False, mongodb=False):
        self._data_init_config = {
            "remote.data.address": "tcp://data.tushare.org:8910",
            "remote.data.username": "13916302001",
            "remote.data.password": "eyJhbGciOiJIUzI1NiJ9.eyJjcmVhdGVfdGltZSI6IjE1MTU4MDU5MzA0ODciLCJpc3MiOiJhdXRoMCIsImlkIjoiMTM5MTYzMDIwMDEifQ.0U0UO6m_0IiELOA-YVZr1Dpdw-d8bekfK7vUPLyIXoQ"
        }

        if dataapi:
            self._dataapi = DataApi(
                addr=self._data_init_config.get('remote.data.address'))
            self._dataapi.login(self._data_init_config.get(
                'remote.data.username'), self._data_init_config.get('remote.data.password'))

        if mongodb:
            # 连接mongoDB
            self._client = pymongo.MongoClient(host='localhost', port=27017)

    
    def _logout(self):
        self._client.close()


    def _set_data_status(self):
        '''
        判断是首次下载，或增量更新数据
        首次下载，则self._data_status = 1
        增量更新，则self._data_status = 2
        同时，返回去掉默认数据库后的db列表和collection列表
        '''
        self._db_list = self._client.list_database_names()
        # 去掉默认的数据库，admin和local
        self._db_list.remove('admin')
        self._db_list.remove('local')
        if self._db_list == []:
            self._data_status = 1
        else:
            self._col_list = self._db.collection_names()
            self._data_status = 2


    def _get_instrumentInfo(self):
        '''
        获取沪深A股基本资料
        inst_type=1  证券类别：股票
        status=1     上市状态：上市
        返回: pd.DataFrame
        '''
        df, msg = self._dataapi.query(
            view="jz.instrumentInfo",
            fields="status,list_date,delist_date,name,market,symbol",
            filter="inst_type=1&status=1",
            data_format='pandas')

        # 股票市场为沪市和深市，原数据包括港股
        df = df[(df['market'] == 'SH') | (df['market'] == 'SZ')]
        return df


    def _set_universe(self):
        "获取当前日期，所有上市状态的A股代码集合"
        self.universe = set(self._get_instrumentInfo()['symbol'])


    def _set_start_end_date_first_time(self):
        "确定首次下载数据时的起止日期"
        df = self._get_instrumentInfo()
        list_date = df[df['symbol'] == self.symbol]['list_date'].iloc[0]
        delist_date = df[df['symbol'] == self.symbol]['delist_date'].iloc[0]

        # 判定上市、退市日期和today的关系
        today = datetime.today().strftime('%Y%m%d')
        self.start_date = list_date
        self.end_date = min(today, delist_date)


    def _set_start_end_date_update(self):
        "确定增量更新数据时的起止日期"
        # 从数据库中获取已存数据的交易日期, 类型pd.DataFrame"
        trade_date = self._col.find({}, {'trade_date': 1, '_id': 0})
        trade_date = [i for i in trade_date]
        trade_date = pd.DataFrame(trade_date)

        # 获取已存数据的最新交易日期，类型str"
        latest_date = trade_date['trade_date'].max()
        latest_date = str(latest_date)

        # 增量更新时的起止日期"
        today = datetime.today().strftime('%Y%m%d')
        latest_date = datetime.strptime(latest_date, '%Y%m%d')
        latest_date = latest_date + timedelta(days=1)
        latest_date = latest_date.strftime('%Y%m%d')

        if today >= latest_date:
            self.start_date = latest_date
            self.end_date = today
        else:
            print("Trade date in Dbase is latest.")


    def _download_data(self):
        df = self._dataapi.query(
            view=self.db,
            fields=self.fields,
            filter=self.symbol + '&' + self.start_date + '&' + self.end_date
        )
        return df


    def _save_data(self):
        for db_name, self.fields in self.reference.items():
            df = self._download_data()
            props = df.to_dict(orient='records')

            self._db = self._client[db_name]
            self._col = self._db[self.symbol]

            try:
                self._col.insert_many(props)
                print("\n\Save data to DBase...\nSymbol: {}\n\n".format(self.symbol))
            except:
                # 将错误股票代码列表写入文件
                with open(self._folder_path + 'error_list.csv', 'a') as f:
                    f.write(symbol + ',')
            finally:
                self._logout()


    def main(self, symbol):
        self.symbol = symbol
        self._login(dataapi=True, mongodb=True)
        self._set_data_status()
        self._set_universe()

        if self._data_status == 1:
            self._set_start_end_date_first_time()
            self._save_data()


        if self._data_status == 2:
            self._set_start_end_date_update()
            self._save_data()




if __name__ == '__main__':
    api = MyTushareApi()
    api.main('000001.SZ')

            


