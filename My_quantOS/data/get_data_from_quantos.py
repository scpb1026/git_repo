from datetime import datetime as dt
import pymongo
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
    dv = DataView()
    dv.load_dataview(save_data_folder)
    df = dv.data_d
    return df


def get_props():
    df = load_data()
    df.columns = df.columns.droplevel()
    df.reset_index(inplace=True)
    props = df.to_dict(orient='records')
    return props


def get_symbol_list():
    symbol_list = get_instrumentInfo()['symbol'].tolist()
    return symbol_list


def write_data_into_dbase():
    # symbol_list = get_symbol_list()
    symbol_list = ['000001.SZ', '600300.SH']
    
    client = pymongo.MongoClient()
    db = client.financial_data_of_stock_test

    for symbol in symbol_list:
        save_reference_daily_data(symbol)

        collection = db[symbol]
        collection.insert_many(get_props())
        print("\n\nWrite data into DBase...\nSymbol: {}\n\n".format(symbol))
    
    client.close()



if __name__ == '__main__':
    write_data_into_dbase()
