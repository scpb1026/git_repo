# encoding: utf-8
"""
取得指数成分股的pe和pb，计算指数的pe和pb，并判断估值区间
"""

from __future__ import print_function
from __future__ import absolute_import
from pandas import Series, DataFrame
import pandas as pd
from datetime import datetime as dt

from jaqs.data.dataservice import RemoteDataService
from jaqs.data.dataview import DataView
from jaqs.data.dataapi import DataApi
import jaqs.util as jutil

from config_path import DATA_CONFIG_PATH, TRADE_CONFIG_PATH

data_config = jutil.read_json(DATA_CONFIG_PATH)
# trade_config = jutil.read_json(TRADE_CONFIG_PATH)

index = '000300.SH'
fields = 'pe_ttm,pb'

dataview_dir_path = '../../output/index_valuation/' + index + '/dataview'
result_dir_path = '../../output/index_valuation/' + index + '/result'
# backtest_result_dir_path = '../../output/index_vluation' + index + '/result'


# 获取指数基本信息
def get_index_basic_information():
    api = DataApi(addr=data_config.get('remote.data.address'))
    api.login(data_config.get('remote.data.username'), data_config.get('remote.data.password'))
    df, msg = api.query(
        view="lb.indexInfo",
        fields="symbol,name,listdate,expire_date",
        filter="symbol=" + index,
        data_format='pandas')

    # 判断今天的日期是否超出了指数的终止发布日期, 如果超出，以终止发布日期为准
    # 开始日期等于指数发布日期
    today = dt.today().strftime('%Y%m%d')
    if df['expire_date'][0]:
        end_date = min(today, df['expire_date'][0])
    else:
        end_date = today

    symbol, name, start_date = df['symbol'][0], df['name'][0], df['listdate'][0]
    # print(symbol, name, start_date, end_date)
    return (symbol, name, start_date, end_date)


# 读取所需数据并保存至本地
def save_dataview():
    ds = RemoteDataService()
    ds.init_from_config(data_config)
    dv = DataView()
    start_date = get_index_basic_information()[2]
    end_date = get_index_basic_information()[3]
    props = {
        'universe': index,
        'start_date': start_date,
        'end_date': end_date,
        'fields': fields,
        'freq': 1
    }
    dv.init_from_config(props, data_api=ds)
    dv.prepare_data()
    dv.save_dataview(folder_path=dataview_dir_path)


# 计算某一天的指数pe和pb
def calculate_pe_pb_of_index_single_day(date):
    dv = DataView()
    dv.load_dataview(folder_path=dataview_dir_path)

    # 计算指数pe和pb的中位数、等权数
    data = dv.get_snapshot(date, symbol='', fields='pe_ttm,pb')

    # 判断数据质量，如果非nan数据占比超过2%，则抛出异常
    if len(data.dropna(how='any')) / len(data) <= 0.98:
        raise Exception('Nan of Data is too much.')
    else:
        data.dropna(how='any', inplace=True)

    # 计算成分股个数
    N = len(data)
    # 计算中位数，以倒数排序可以去掉负数的影响
    pe_median = 1 / ((1 / data['pe_ttm']).median())
    pb_median = 1 / ((1 / data['pb']).quantile(0.5))
    # 计算等权，即调和平均数
    pe_equal = N / (1 / data['pe_ttm']).sum()
    pb_equal = N / (1 / data['pb']).sum()
    print(data)
    print(date, pe_median, pe_equal, pb_median, pb_equal)
    return (date, pe_median, pe_equal, pb_median, pb_equal)


# 判断估值区间的函数
def calculate_state(data):
    if data < 10.0:
        return 'extreme undervalue'
    elif 10 <= data and data < 20:
        return 'undervalue'
    elif 20 <= data and data < 40:
        return 'below normal'
    elif 40 <= data and data < 60:
        return 'normal'
    elif 60 <= data and data < 80:
        return 'high normal'
    elif 80 <= data and data < 90:
        return 'overvalue'
    elif 90 <= data:
        return 'extreme overvalue'


# 判断数据类型是否float，是则保留2位小数
def convert_float(x):
    if isinstance(x, float):
        return round(x, 2)


# 计算指数pe、pb的中位数和等权的估值区间
def calculate_value_range_of_index():






    pass


if __name__ == '__main__':
    calculate_pe_pb_of_index_single_day(20171214)
    # get_index_basic_information(index)