import getpass
import os
import pandas as pd
import datetime as dt
import numpy as np
import time
import requests
import json
base = ['btc', 'usdc', 'doge', 'usdt', 'eth', 'uni', 'aave']
interv = '1d'
dfss=pd.DataFrame()
YOUR_API_KEY = '2u8d86u372f3r2a1rme8l0463saycq4h'
headers = {"X-Api-Key": YOUR_API_KEY,"Accept": "application/json","Accept-Encoding": "gzip"}
start = '2022-01-01T01:01:30.000Z'
end = '2022-10-03T11:01:00.000Z'

for bb in base:
        try:
                URL = 'https://us.market-api.kaiko.io/v1/data/trades.v1/spot_direct_exchange_rate/'+bb+'/usd?start_time='+start+'&end_time='+end+'&interval='+interv+'&page_size=1000'
                res = requests.get(URL, headers = headers, stream=True).json()
                temp_df = pd.DataFrame(res["data"])
                temp_df['asset'] = (bb)
                dfss = pd.concat([dfss, temp_df], ignore_index=True)
                while "next_url" in res.keys():
                        res = requests.get(res["next_url"], headers = headers, stream=True).json()
                        temp_df = pd.DataFrame(res["data"])
                        temp_df['asset'] = (bb)
                        dfss = pd.concat([dfss, temp_df], ignore_index=True)
        except:
                print('Cannot get liquidity snapshots data for this asset = '+str(bb))

cross_rates = dfss.copy()
cross_rates = cross_rates.reset_index()
cross_rates.ffill(inplace=True)
cross_rates = cross_rates.drop(['index'], axis=1)
cross_rates = cross_rates.rename(columns={"price": "cross_price"})
print(cross_rates)