import time
from datetime import datetime, timedelta
from multiprocessing.dummy import Pool as ThreadPool
import pymongo
import pandas as pd
import sys

from jaqs.data.dataapi import DataApi
from jaqs.data.dataservice import RemoteDataService
from jaqs.data.dataview import DataView


data_config = {
    "remote.data.address": "tcp://data.tushare.org:8910",
    "remote.data.username": "13916302001",
    "remote.data.password": "eyJhbGciOiJIUzI1NiJ9.eyJjcmVhdGVfdGltZSI6IjE1MTU4MDU5MzA0ODciLCJpc3MiOiJhdXRoMCIsImlkIjoiMTM5MTYzMDIwMDEifQ.0U0UO6m_0IiELOA-YVZr1Dpdw-d8bekfK7vUPLyIXoQ"
}

save_data_folder = './output/get_data_quantOS/dataview/'

ds = RemoteDataService()
ds.init_from_config(data_config)

api = DataApi(addr=data_config.get('remote.data.address'))
api.login(data_config.get('remote.data.username'),
          data_config.get('remote.data.password'))


def get_instrumentInfo():
    '''
    获取沪深A股基本资料
    inst_type=1  证券类别：股票
    status=1     上市状态：上市
    '''
    df, msg = api.query(
        view="jz.instrumentInfo",
        fields="status,list_date,delist_date,name,market",
        filter="inst_type=1&status=1",
        data_format='pandas')

    # 股票市场为沪市和深市，原数据包括港股
    df = df[(df['market'] == 'SH') | (df['market'] == 'SZ')]
    return df


def get_code_and_date(symbol):
    "获取股票代码、上市日期、退市日期"
    if not isinstance(symbol, str):
        raise TypeError('Type of symbol must be str.')
    df = get_instrumentInfo()
    df = df[['symbol', 'name', 'list_date', 'delist_date']]
    df = df[df['symbol'] == symbol]
    return df


def get_trade_date_first_time(symbol):
    '''
    获取首次存储时股票的start_date和end_date

    返回：
    symbol -> str
    start_date -> int
    end_date -> int
    '''
    df = get_code_and_date(symbol)

    list_date = df['list_date'].iloc[0]
    delist_date = df['delist_date'].iloc[0]

    # 判定上市、退市日期和today的关系
    today = datetime.today().strftime('%Y%m%d')
    start_date = int(list_date)
    end_date = int(min(today, delist_date))

    res = {
        'symbol': symbol,
        'start_date': start_date,
        'end_date': end_date}
    return res


def save_reference_daily_data(symbol, start_date, end_date):
    """
    保存单个股票的基本财务数据
    :param symbol: str
    :return: DataView
    """
    dv = DataView()
    fields = ','.join(list(dv.reference_daily_fields))
    props = {
        'symbol': symbol,
        'fields': fields,
        'start_date': start_date,
        'end_date': end_date,
        'freq': 1
    }

    dv.init_from_config(props=props, data_api=ds)
    dv.prepare_data()
    return dv.data_d


# def load_data():
#     "从已保存的dataview中读取数据"
#     dv = DataView()
#     dv.load_dataview(save_data_folder)
#     df = dv.data_d
#     return df


def get_props_first_time(symbol, start_date, end_date):
    "首次存储数据时，将读取的dataframe转换为props, 以备写入数据库"
    df = save_reference_daily_data(symbol, start_date, end_date)
    df.columns = df.columns.droplevel()
    df.reset_index(inplace=True)
    props = df.to_dict(orient='records')
    return props


def get_props_update(symbol, start_date, end_date):
    "增量更新数据时，将读取的dataframe转换为props，以备写入数据库"
    df = save_reference_daily_data(symbol, start_date, end_date)
    df.columns = df.columns.droplevel()
    df.reset_index(inplace=True)

    # dataframe的范围限制在更新时期之内，因为dataview取数据时会将日期的范围前后各放宽几天
    df = df[(df['trade_date'] >= start_date) & (df['trade_date'] <= end_date)]

    # 判断drop掉trade_date列，并dropna后，dataframe是否为空，空则说明更新日期已经是最新的
    df_if = df.drop('trade_date', axis=1).dropna(how='all')

    if df_if.empty:
        print("\n\nTrade date is latest.\n\n")
        return False
    else:
        props = df.to_dict(orient='records')
        return props


def get_symbol_list():
    "获取所有A股的代码列表"
    symbol_list = get_instrumentInfo()['symbol'].tolist()
    return symbol_list


def write_data_into_dbase(symbol, start_date, end_date, props_func):
    "将单只股票数据写入数据库"
    client = pymongo.MongoClient('localhost', 27017)
    db = client.reference_daily_data_of_stock

    try:
        props = props_func(symbol, start_date, end_date)
        if props:
            collection = db[symbol]
            collection.insert_many(props)
            print("\n\nWrite data into DBase...\nSymbol: {}\n\n".format(symbol))

    except:
        # 将错误股票代码列表写入文件
        with open(save_data_folder + 'error_list.csv', 'a') as f:
            f.write(symbol + ',')

    finally:
        client.close()


# def threading_do():
#     "多线程"
#     # 获取股票列表
#     symbol_list = get_symbol_list()

#     start_time = time.time()
#     pool = ThreadPool(4)
#     pool.map(write_data_into_dbase, symbol_list)
#     pool.close()
#     pool.join()
#     end_time = time.time()
#     print("\nMission Completed in {:.2f} second.".format(
#         end_time - start_time))


def get_symbol_list_already_in_dbase():
    "从数据库中获取已经存在的股票列表，存在则返回代码列表，不存在则返回空列表"
    try:
        client = pymongo.MongoClient('localhost', 27017)
        db = client.reference_daily_data_of_stock
        already_symbol_list = db.collection_names()
        client.close()
        return already_symbol_list
    except:
        return []


def get_trade_date_already_in_dbase(symbol):
    "从数据库中获取已存数据的交易日期，返回DataFrame"
    client = pymongo.MongoClient('localhost', 27017)
    db = client.reference_daily_data_of_stock
    collection = db[symbol]
    trade_date = collection.find({}, {'trade_date': 1, '_id': 0})
    trade_date = [i for i in trade_date]
    trade_date = pd.DataFrame(trade_date)
    return trade_date


def get_latest_trade_date_in_dbase(symbol):
    "获取已存数据的最新交易日期，返回str"
    latest_date = get_trade_date_already_in_dbase(symbol)
    latest_date = latest_date['trade_date'].max()
    latest_date = str(latest_date)
    return latest_date


def get_trade_date_update(symbol):
    "增量更新时的开始和结束日期，返回int"
    today = datetime.today().strftime('%Y%m%d')
    latest_trade_date = get_latest_trade_date_in_dbase(symbol)
    latest_trade_date = datetime.strptime(latest_trade_date, '%Y%m%d')
    latest_trade_date = latest_trade_date + timedelta(days=1)
    latest_trade_date = latest_trade_date.strftime('%Y%m%d')

    if today >= latest_trade_date:
        start_date = int(latest_trade_date)
        end_date = int(today)
    else:
        print("Trade date in Dbase is latest.")

    res = {
        'start_date': start_date,
        'end_date': end_date}
    return res


# def save_data_to_dbase_first_time():
#     "首次存储数据"
#     # 判断股票代码是否已经存在于数据库中
#     symbol_list = get_symbol_list()
#     already_symbol_list = get_symbol_list_already_in_dbase()

#     start_time = time.time()

#     for symbol in symbol_list:
#         if symbol not in already_symbol_list:
#             # 获取开始和结束日期
#             date = get_trade_date_first_time(symbol)
#             start_date = date['start_date']
#             end_date = date['end_date']

#             write_data_into_dbase(symbol, start_date,
#                                   end_date, get_props_first_time)

#     end_time = time.time()
#     print("\nMission Completed in {:.2f} second.".format(
#         end_time - start_time))


def update_date_to_dbase():
    "增量更新数据"
    # 获取股票代码列表
    symbol_list = get_symbol_list()[:13]
    # already_symbol_list = get_symbol_list_already_in_dbase()

    start_time = time.time()

    for symbol in symbol_list:
        if not first_or_update_bool(symbol):
            # 获取增量更新的开始和结束日期
            date = get_trade_date_update(symbol)
            start_date = date['start_date']
            end_date = date['end_date']

            write_data_into_dbase(symbol, start_date,
                                  end_date, get_props_update)

        else:
            # 获取开始和结束日期
            date = get_trade_date_first_time(symbol)
            start_date = date['start_date']
            end_date = date['end_date']

            write_data_into_dbase(symbol, start_date,
                                  end_date, get_props_first_time)

    end_time = time.time()
    print("\nMission Completed in {:.2f} second.".format(
        end_time - start_time))


def first_or_update_bool(symbol):
    "判断symbol状态，首次存储则返回True，增量更新则返回False"
    # symbol_list = get_symbol_list()
    already_symbol_list = get_symbol_list_already_in_dbase()

    if symbol not in already_symbol_list:
        return True
    else:
        latest_trade_date = get_latest_trade_date_in_dbase(symbol)
        today = datetime.today().strftime('%Y%m%d')

        if today <= latest_trade_date:
            print("The data of symbol {} is latest.".format(symbol))
            sys.exit()
        else:
            return False



if __name__ == '__main__':
    # save_data_to_dbase_first_time()
    update_date_to_dbase()

