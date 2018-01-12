from datetime import datetime as dt
import pymongo
from multiprocessing.dummy import Pool
import time

from jaqs.data.dataapi import DataApi
from jaqs.data.dataservice import RemoteDataService
from jaqs.data.dataview import DataView

data_config = {
    "remote.data.address": "tcp://data.tushare.org:8910",
    "remote.data.username": "13916302001",
    "remote.data.password": "eyJhbGciOiJIUzI1NiJ9.eyJjcmVhdGVfdGltZSI6IjE1MTMwODM0NTYyOTEiLCJpc3MiOiJhdXRoMCIsImlkIjoiMTM5MTYzMDIwMDEifQ.ElZ4FrNrU33BemD2BRsY50JEsueckomSXyr2dx_2c-A"
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


def get_date(symbol):
    ''' 
    获取股票的start_date和end_date
    
    返回：
    symbol -> str
    start_date -> int
    end_date -> int
    '''
    df = get_code_and_date(symbol)

    list_date = df['list_date'].iloc[0]
    delist_date = df['delist_date'].iloc[0]

    # 判定上市、退市日期和today的关系
    today = dt.today().strftime('%Y%m%d')
    start_date = int(list_date)
    end_date = int(min(today, delist_date))

    res = {
        'symbol': symbol,
        'start_date': start_date,
        'end_date': end_date}
    return res


def save_reference_daily_data(symbol):
    """
    保存单个股票的基本财务数据
    :param symbol: str
    :return: DataView
    """
    date = get_date(symbol)
    start_date = date['start_date']
    end_date = date['end_date']

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
    dv.save_dataview(save_data_folder)


def load_data():
    "从已保存的dataview中读取数据"
    dv = DataView()
    dv.load_dataview(save_data_folder)
    df = dv.data_d
    return df


def get_props():
    "将读取的dataframe转换为props, 以备写入数据库"
    df = load_data()
    df.columns = df.columns.droplevel()
    df.reset_index(inplace=True)
    props = df.to_dict(orient='records')
    return props


def get_symbol_list():
    "获取所有A股的代码列表"
    symbol_list = get_instrumentInfo()['symbol'].tolist()
    return symbol_list


def write_data_into_dbase():
    "按照代码列表，将数据写入数据库"
    symbol_list = get_symbol_list()
    
    client = pymongo.MongoClient()
    db = client.reference_daily_data_of_stock

    # 错误股票代码列表
    error_list = []

    for symbol in symbol_list:
        try:
            save_reference_daily_data(symbol)

            collection = db[symbol]
            collection.insert_many(get_props())
            print("\n\nWrite data into DBase...\nSymbol: {}\n\n".format(symbol))
        except:
            error_list.append(symbol)
    
    client.close()

    # 将错误股票代码列表写入文件
    with open(save_data_folder + 'error_list.csv', 'w') as f:
        for symbol in error_list:
            f.write(symbol)
    

def threading_do():
    "多进程"
    start_time = time.time()
    pool = Pool(4)
    pool.apply(write_data_into_dbase)
    pool.close()
    pool.join()
    end_time = time.time()
    print("\nMission Completed in {:.2f} second.".format(end_time - start_time))



if __name__ == '__main__':
    threading_do()
