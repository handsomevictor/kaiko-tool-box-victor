import pandas as pd
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from itertools import repeat
import os
import requests
from tqdm import tqdm
import warnings
from api_key_and_constants import API_KEY

warnings.filterwarnings("ignore")
database_combined_file = os.path.join("database", "combined_file")

if not os.path.exists(database_combined_file):
    print("It's strange! The result folder should have been created! Check!")
    os.makedirs(database_combined_file)

YOUR_API_KEY = API_KEY
headers = {"X-Api-Key": YOUR_API_KEY, "Accept": "application/json", "Accept-Encoding": "gzip"}


def get_data_single_exch(exch=None, pair=None, start_date=None, end_date=None, data_type=None, interval='1h'):
    """
    For assetprice, the return result will not include any exchanges
    :param exch:
    :param pair:
    :param start_date:
    :param end_date:
    :param data_type:
    :param interval:
    :return:
    """
    if data_type == "count_ohlcv_vwap":
        URL = f'https://us.market-api.kaiko.io/v2/data/trades.v1/exchanges/{exch}/spot/{pair}/aggregations/'\
              f'count_ohlcv_vwap?interval={interval}&start_time={start_date}&end_time={end_date}&page_size=10000'

    elif data_type == "trades":
        URL = f'https://us.market-api.kaiko.io/v2/data/trades.v1/exchanges/{exch}/spot/{pair}/trades?start_time='\
              f'{start_date}&end_time={end_date}&page_size=10000'

    elif data_type == "assetprice":
        if '-' in pair:
            base = pair.split('-')[0]
            quote = pair.split('-')[1]
        if pair == 'usd':
            raise ValueError("Please input Base! Not quote")
        URL = f'https://us.market-api.kaiko.io/v2/data/trades.v1/spot_direct_exchange_rate/{base}/{quote}?'\
              f'start_time={start_date}&end_time={end_date}&interval={interval}&page_size=1000'

    try:
        res = requests.get(URL, headers=headers, stream=True).json()
        temp_df2 = pd.DataFrame(res["data"])
        temp_df2['pair'] = pair
        if data_type != "assetprice":
            temp_df2['exchange'] = exch

        while "next_url" in res.keys():
            res = requests.get(res["next_url"], headers=headers, stream=True).json()
            temp_df2 = pd.DataFrame(res["data"])
            temp_df2['pair'] = pair

            if data_type != "assetprice":
                temp_df2['exchange'] = exch
    except:
        temp_df2 = pd.DataFrame()
        print('no data for this instrument ' + str(pair) + str(exch))
    return temp_df2


def get_data_single_exch_concurrent(exches=None, pair=None, start_date=None, end_date=None, interval=None,
                                    data_type=None, max_workers=20):
    temp = pd.DataFrame()
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        if data_type == 'assetprice':
            # Since no exchange list is needed, just return get_data_single_exch is fine.
            res = get_data_single_exch(exch=None, pair=pair, start_date=start_date, end_date=end_date,
                                       data_type=data_type, interval='1h')
            return res
        else:
            res = list(pool.map(get_data_single_exch, exches, repeat(pair), repeat(start_date),
                                repeat(end_date),
                                repeat(data_type), repeat(interval)))
            for i in res:
                temp = pd.concat([temp, i], axis=0)
    return temp


def get_data_combined_exch_concurrent(exches=None, pairs=None, start_dates=None, end_dates=None, intervals=None,
                                      data_type=None, max_workers_process=None, max_workers_thread=None):
    """
    exches should be a list of the same lengths as pairs, each item in exches should correspond to the same
    elements in pairs, for example:

    exches = [['blc2', 'curv'], ['blc2', 'sush'], ['usp2']]
    pairs = ['weth-acmusd', 'weth-amp', 'weth-atona']

    Here, we will download the data from ['blc2', 'curv'] two exchanges of pair weth-acmusd.

    Final result will be a big csv file stored in database.
    """
    temp = pd.DataFrame()
    with ProcessPoolExecutor(max_workers=max_workers_process) as pool:
        if data_type == 'assetprice':
            res = list(tqdm(pool.map(get_data_single_exch_concurrent, repeat('Stupid_stuff'), pairs,
                                     repeat(start_dates),
                                     repeat(end_dates), repeat(intervals), repeat(data_type),
                                     repeat(max_workers_thread)), total=len(pairs)))
        else:
            res = list(tqdm(pool.map(get_data_single_exch_concurrent, exches, pairs, repeat(start_dates),
                                     repeat(end_dates), repeat(intervals), repeat(data_type),
                                     repeat(max_workers_thread)), total=len(pairs)))

        for i in res:
            temp = pd.concat([temp, i], axis=0)

    save_dir = os.path.join(database_combined_file)

    temp['dates'] = pd.to_datetime(temp['timestamp'], unit='ms')
    temp.to_csv(os.path.join(save_dir, f"result_{data_type}.csv"))
    return temp


if __name__ == '__main__':
    pass
