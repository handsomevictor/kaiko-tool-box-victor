import pandas as pd
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from itertools import repeat
import os
import requests
from tqdm import tqdm
import warnings
import datetime

from api_key_and_constants import API_KEY

warnings.filterwarnings("ignore")
database_combined_file = os.path.join("database", "combined_file")

if not os.path.exists(database_combined_file):
    print("It's strange! The result folder should have been created! Check!")
    os.makedirs(database_combined_file)

# YOUR_API_KEY = API_KEY
YOUR_API_KEY = '2u8d86u372f3r2a1rme8l0463saycq4h'
headers = {"X-Api-Key": YOUR_API_KEY, "Accept": "application/json", "Accept-Encoding": "gzip"}


def get_lending_borrowing_data_single_exch(exch=None, start_date=None, end_date=None):
    """
    For lending_borrowing_protocols, the return result will be the result of one single exchange

    start_date and end_date should not be in datetime!

    Reminder: pair is not a parameter!
    """
    # change the start and end date from datetime to str!
    if isinstance(start_date, datetime.datetime):
        start_date = start_date.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        end_date = end_date.strftime("%Y-%m-%dT%H:%M:%S.000Z")

    URL = 'https://eu.market-api.kaiko.io/v2/data/lending.v1/events'
    params = {'start_time': start_date, 'end_time': end_date, 'exchange': exch, 'page_size': 1000}
    # print(f"Getting data from {exch} from {start_date} to {end_date}")

    # ------------------------------------------------------------------------------------------------------------------

    try:
        res = requests.get(URL, headers=headers, stream=True, params=params).json()
        temp_df2 = pd.DataFrame(res["data"])

        while "next_url" in res.keys():
            res = requests.get(res["next_url"], headers=headers, stream=True).json()
            temp_df3 = pd.DataFrame(res["data"])
            temp_df2 = pd.concat([temp_df2, temp_df3], ignore_index=True)
    except:
        temp_df2 = pd.DataFrame()
        print(f'no data for this {exch} from {start_date} to {end_date}!')
    temp_df2['exchange'] = exch

    # Now handle the complex metadata column
    # extract all possible keys and values and add columes to the df
    try:
        all_keys = list(set([item for sublist in [i.keys() for i in temp_df2.metadata] for item in sublist]))

        metadata_df = pd.DataFrame()
        for i in all_keys:
            metadata_df[i] = temp_df2.metadata.apply(lambda x: x.get(i, None))

        temp_df2 = pd.concat([temp_df2, metadata_df], axis=1)
        temp_df2.drop(columns=['metadata'], inplace=True)

        # rename: add metadata in prefix
        rename_dict = {i: f'metadata_{i}' for i in all_keys}
        temp_df2.rename(columns=rename_dict, inplace=True)
    except:
        pass
    return temp_df2


def get_single_exches_concurrent(exch=None, start_date=None, end_date=None, max_workers=60):
    """
    This one is using concurrent for getting data from one exchange but split the time period into multiple parts.

    start_date and end_date should be in datetime!
    """

    # extract each hour
    start_date_list = []
    end_date_list = []
    while start_date < end_date:
        start_date_list.append(start_date)
        start_date = start_date + datetime.timedelta(days=1)
        end_date_list.append(start_date)
    if start_date_list[-1] != end_date:
        start_date_list.append(start_date)
        end_date_list.append(end_date)

    start_date_list = [i.strftime("%Y-%m-%dT%H:%M:%S.000Z") for i in start_date_list]
    end_date_list = [i.strftime("%Y-%m-%dT%H:%M:%S.000Z") for i in end_date_list]

    temp = pd.DataFrame()
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        res = list(pool.map(get_lending_borrowing_data_single_exch, repeat(exch), start_date_list, end_date_list))
        for i in res:
            temp = pd.concat([temp, i], axis=0)
    return temp


def get_multiple_exches_concurrent(exches: list=None, start_dates=None, end_dates=None, max_workers_process=None,
                                   max_workers_thread=None):
    """
    exches should be a list of the same lengths as pairs, each item in exches should correspond to the same
    elements in pairs, for example:

    exches = [['blc2', 'curv'], ['blc2', 'sush'], ['usp2']]
    pairs = ['weth-acmusd', 'weth-amp', 'weth-atona']

    start_dates and end_dates should be in datetime list!

    Here, we will download the data from ['blc2', 'curv'] two exchanges of pair weth-acmusd.

    Final result will be a big csv file stored in database.
    """
    temp = pd.DataFrame()
    with ProcessPoolExecutor(max_workers=max_workers_process) as pool:
        res = list(tqdm(pool.map(get_single_exches_concurrent, exches, start_dates, end_dates,
                                 repeat(max_workers_thread)), total=len(exches)))
        for i in res:
            temp = pd.concat([temp, i], axis=0)

    try:
        temp.datetime = pd.to_datetime(temp.datetime, unit='s')
    except:
        pass
    save_dir = os.path.join(database_combined_file)

    temp.to_csv(os.path.join(save_dir, f"lending_borrowing_{exches}_result_{start_dates[0].strftime('%Y-%m-%d')}_to"
                                       f"_{end_dates[0].strftime('%Y-%m-%d')}.csv"))
    print(f"Done! Lending and borrowing {exches} data saved in {save_dir}")
    return temp


if __name__ == '__main__':
    start = datetime.datetime(2022, 10, 1)
    end = datetime.datetime(2022, 10, 20)

    exch = ['cmpd', 'aav2']
    start_list = [start for exch in exch]
    end_list = [end for exch in exch]

    a = get_multiple_exches_concurrent(exches=exch, start_dates=start_list, end_dates=end_list,
                                       max_workers_process=20,
                                       max_workers_thread=20)
    print(a)
