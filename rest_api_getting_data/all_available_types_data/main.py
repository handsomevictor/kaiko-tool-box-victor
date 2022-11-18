import datetime

from get_data import get_data_combined_exch_concurrent, get_data_single_exch
from rest_api_getting_data.all_available_types_data.get_lending_borrowing_protocols_data import \
    get_multiple_exches_concurrent


"""
If trade data is needed, change the parameter: data_type to trades.
if OHLCV VWAP data needed, change it to count_ohlcv_vwap.
"""


def get_ohlcv():
    start = '2022-04-01T00:00:00.000Z'
    end = '2022-10-05T00:00:00.000Z'
    list_pairs = ['weth-crv',
                  'weth-usdt',
                  'weth-tribe',
                  'weth-renbtc',
                  'weth-rari',
                  'weth-amp',
                  'weth-raini']

    list_pairs = ['btc-usd', 'eth-usd']
    # list_exchanges = ['curv', 'sush', 'usp2', 'usp3', 'blc2']
    list_exchanges = ['bnus', 'cbse', 'gmni']
    interval = '1h'

    new_list_exchanges = [list_exchanges for pair in list_pairs]

    start_dates = [start for i in range(len(list_pairs))]
    end_dates = [end for i in range(len(list_pairs))]

    b = get_data_combined_exch_concurrent(exches=new_list_exchanges, pairs=list_pairs, start_dates=start,
                                          end_dates=end, intervals=interval, data_type='count_ohlcv_vwap',
                                          max_workers_process=20,
                                          max_workers_thread=20)
    print(b)


def get_assetprice():
    start = '2022-05-01T00:00:00.000Z'
    end = '2022-10-02T00:00:00.000Z'

    list_pairs = ['btc-usd']
    interval = '1h'

    b = get_data_combined_exch_concurrent(pairs=list_pairs, start_dates=start,
                                          end_dates=end, intervals=interval, data_type='assetprice',
                                          max_workers_process=20,
                                          max_workers_thread=20)
    print(b)


def get_lending_borrowing_protocols():
    start = datetime.datetime(2022, 9, 1)
    end = datetime.datetime(2022, 10, 1)

    exches = ['cmpd', 'aav2']
    start_list = [start for exch in exches]
    end_list = [end for exch in exches]

    res = get_multiple_exches_concurrent(exches=exches, start_dates=start_list, end_dates=end_list,
                                         max_workers_process=50,
                                         max_workers_thread=5)
    print(res.head())


if __name__ == '__main__':
    # get_ohlcv()
    # get_assetprice()
    # get_lending_borrowing_protocols()

    get_lending_borrowing_protocols()
