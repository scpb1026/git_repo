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


class MyQuantosDataApi():
    def __init__(self):
        self.universe = []
        self.symbol = ""
        self.fields = ""
        self.start_date = 0
        self.end_date = 0
        self._client = None
        self._db = None
        self._col = None
        self._db_name = ""
        self._col_name = ""
        self._db_list = []
        self._col_list =[]
        self._data_init_config = {}
        self._dataapi = None
        self._remote_data_service = None
        self._instrument_info = pd.DataFrame()
        self._symbol_set_all_A = set()
        

    def _login(self, dataapi=False, remote=False, mongodb=False):
        self._data_init_config = {
            "remote.data.address": "tcp://data.tushare.org:8910",
            "remote.data.username": "13916302001",
            "remote.data.password": "eyJhbGciOiJIUzI1NiJ9.eyJjcmVhdGVfdGltZSI6IjE1MTU4MDU5MzA0ODciLCJpc3MiOiJhdXRoMCIsImlkIjoiMTM5MTYzMDIwMDEifQ.0U0UO6m_0IiELOA-YVZr1Dpdw-d8bekfK7vUPLyIXoQ"
        }

        if dataapi:
            self._dataapi = DataApi(addr=self._data_init_config.get('remote.data.address'))
            self._dataapi.login(self._data_init_config.get('remote.data.username'), self._data_init_config.get('remote.data.password'))

        if remote:
            self._remote_data_service = RemoteDataService()
            self._remote_data_service.init_from_config(self._data_init_config)

        if mongodb:
            # 连接mongoDB
            self._client = pymongo.MongoClient(host='localhost', port=27017)
            # 连接db和collection
            self._db = self._client[self._db_name]
            self._col_name = self.symbol
            self._col = self._db[self._col_name]
            # 获得db和collection的名称列表
            self._db_list = self._client.list_database_names()
            self._col_list = self._db.collection_names()


    def _prepare_data(self, first=False, update=False):
        self._login(dataapi=True, mongodb=True)
        self._get_instrumentInfo()
        self._get_symbol_set_all_A()

        if first:
            self._get_start_end_date_first_time()
        if update:
            self._get_start_end_date_update()
        pass




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
        self._instrument_info = df


    def _get_symbol_set_all_A(self):
        "获取当前日期，所有上市状态的A股代码集合"
        self._symbol_set_all_A = set(self._instrument_info['symbol'])


    def _get_start_end_date_first_time(self):
        "确定首次下载数据时的起止日期"
        df = self._instrument_info
        list_date = df[df['symbol'] == self.symbol]['list_date'].iloc[0]
        delist_date = df[df['symbol'] == self.symbol]['delist_date'].iloc[0]

        # 判定上市、退市日期和today的关系
        today = datetime.today().strftime('%Y%m%d')
        self.start_date = int(list_date)
        self.end_date = int(min(today, delist_date))


    def _get_start_end_date_update(self):
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
            self.start_date = int(latest_date)
            self.end_date = int(today)
        else:
            print("Trade date in Dbase is latest.")




if __name__ == '__main__':
    r = MyQuantosDataApi()
    r.symbol = '600000.SH'
    r._prepare_data()
    # x = r._instrumentInfo()
    # x = r._symbol_set_all_A()
    # print(r._symbol_set_all_A)
    print(r.start_date, r.end_date)



