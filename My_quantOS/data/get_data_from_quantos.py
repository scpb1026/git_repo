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


def get_code_and_listdate(symbol):
    "获取股票代码、上市日期、退市日期"
    if not isinstance(symbol, str):
        raise TypeError('Type of symbol should be str.')
    df = get_instrumentInfo()
    df = df[['symbol', 'name', 'list_date', 'delist_date']]
    df = df[df['symbol'] == symbol]
    return df


def get_date(symbol):
    "获取股票的start_date和end_date"
    df = get_code_and_listdate(symbol)

    list_date = df['list_date'].astype(int).iloc[0]
    delist_date = df['delist_date'].astype(int).iloc[0]

    # 判定上市、退市日期和today的关系
    today = dt.today().strftime('%Y%m%d')
    today = int(today)
    start_date = list_date
    end_date = min(today, delist_date)

    res = {
        'symbol': symbol,
        'start_date': start_date,
        'end_date': end_date}
    return res


def save_financial_data(symbol):
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
    # dv.save_dataview(save_data_folder)
    # print(dv.data_d)


if __name__ == '__main__':
    save_financial_data('600300.SH')

    # print(dv.data_d)
    # print(dv.data_inst)
    # dv.data_d = dv.data_d.astype(int)
