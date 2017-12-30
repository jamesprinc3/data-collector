from bitex.api.WSS import GeminiWSS
from bitex.api.WSS import BitfinexWSS
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import fastparquet
import pyarrow
import time

def type_matcher(match_type, data):
    typ, _, _ = data
    return typ == match_type

def pair_matcher(match_pair, data):
    _, pair, _ = data
    return pair == match_pair

#
def snapshot_matcher(data):
    _, _, info = data
    return len(info[0][0]) != 3

wss = BitfinexWSS()
wss.start()
# while 1:
    # print(wss.data_q.get())
time.sleep(5)
wss.stop()

dfs = {}

for pair in wss.pairs:
    print(pair)
    dfs[pair] = pd.DataFrame(index=['orderId', 'price', 'amount', 'timestamp'])

# pd.options.display.float_format = '{:.4g}'.format
# df = pd.DataFrame(index=['orderId', 'price', 'amount', 'timestamp'])

# TODO: deal with initial snapshot
while not wss.data_q.empty():
    data = wss.data_q.get()
    _,pair,lst = data
    if type_matcher('raw_order_book', data) and (not snapshot_matcher(data)): #and pair_matcher('ETHBTC', data):
        row = lst[0][0]
        timestamp = lst[1]
        s = pd.Series({'orderId': row[0], 'price': row[1], 'amount': row[2], 'timestamp': timestamp})
        dfs[pair] = dfs[pair].append(s, ignore_index=True)

print(dfs)

for key, df in dfs.items():
    #TODO: handle case where df is empty
    print(key)
    print(df)
    df.to_parquet("parquet/" + key + ".parquet")
    df.to_pickle("pickle/" + key + ".pickle")

#TODO: get trades data
#TODO: add snapshot to dataframe
#TODO: make a separate parquet file for each pair