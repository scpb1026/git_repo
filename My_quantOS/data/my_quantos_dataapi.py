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
        self._dataview_data = pd.DataFrame()
        self._dbase_props = {}
        self._db_init_config = ['reference_daily_fields']
        self._input_init_config = []
        

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


    def prepare_data(self, first=False, update=False):
        self._get_fields()
        self._login(dataapi=True, remote=True)
        self._get_instrumentInfo()
        self._get_symbol_set_all_A()

        if first:
            self._db_name = self._fields_init_config
            self._col_name = self.symbol
            self._get_start_end_date_first_time()

        if update:
            self._login(mongodb=True)
            self._get_start_end_date_update()


    def _get_input_init_config(self):
        "确定最终多线程map函数的输入列表，返回[(db_name, col_name)]"
        for db_name in self._db_init_config:
            for col_name in self._symbol_set_all_A:
                self._input_init_config.append(db_name, col_name)


    def _get_fields(self):
        "确定查询字段，同时也确定mongoDB的db_name"
        dv = DataView()
        fields_init_config = {
            'reference_daily_fields': dv.reference_daily_fields
            # 此处可能增加新的字段，只要是qunatos的dataview支持的字段
            }


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




    def _download_data(self):
        "使用quantos的dataview，下载单个股票的给定字段的数据，返回pd.DataFrame"
        dv = DataView()
        # fields = ','.join(list(dv.reference_daily_fields))
        props = {
            'symbol': self.symbol,
            'fields': self.fields,
            'start_date': self.start_date,
            'end_date': self.end_date,
            'freq': 1
            }

        dv.init_from_config(props=props, data_api=self._remote_data_service)
        dv.prepare_data()
        self._dataview_data = dv.data_d


    def _dataframe_to_dbase_props(self):
        "将dataframe转换为mongoDB中的props，以备写入数据库"
        df = self._dataview_date
        df.columns = df.columns.droplevel()
        df.reset_index(inplace=True)

        # dataframe的范围限制在更新时期之内，因为dataview取数据时会将日期的范围前后各放宽几天
        df = df[(df['trade_date'] >= self.start_date) & (df['trade_date'] <= self.end_date)]

        # 判断drop掉trade_date列，并dropna后，dataframe是否为空，空则说明更新日期已经是最新的
        is_df = df.drop('trade_date', axis=1).dropna(how='all')

        if is_df.empty:
            print("\n\nTrade date is latest.\n\n")
            return None
        else:
            props = df.to_dict(orient='records')
            self._dbase_props = props


    def _write_data_to_dbase(self):
        pass




if __name__ == '__main__':
    r = MyQuantosDataApi()
    r.symbol = '600000.SH'
    r._prepare_data()
    # x = r._instrumentInfo()
    # x = r._symbol_set_all_A()
    # print(r._symbol_set_all_A)
    print(r.start_date, r.end_date)



