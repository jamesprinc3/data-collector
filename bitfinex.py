from bitex.api.WSS import GeminiWSS
from bitex.api.WSS import BitfinexWSS
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import fastparquet
import pyarrow
import time
import datetime

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

def handle_updates(wss, dfs):
    while not wss.data_q.empty():
        handle_update(wss.data_q.get(), dfs)

#PRE: timeout must be in the future
def handle_updates_sync(wss, dfs, timeout):
    while datetime.datetime.utcnow() < timeout:
        handle_update(wss.data_q.get(), dfs)

def handle_update(data, dfs):
    _, pair, lst = data
    if type_matcher('raw_order_book', data) and (not snapshot_matcher(data)):  # and pair_matcher('ETHBTC', data):
        row = lst[0][0]
        timestamp = lst[1]
        s = pd.Series({'orderId': row[0], 'price': row[1], 'amount': row[2], 'timestamp': timestamp})
        dfs[pair] = dfs[pair].append(s, ignore_index=True)

def save_dfs(dfs):
    for key, df in dfs.items():
        #TODO: handle case where df is empty
        print(key)
        print(df)
        df.to_parquet("parquet/" + key + ".parquet")
        df.to_pickle("pickle/" + key + ".pickle")


def bitfinex():
    wss = BitfinexWSS()
    wss.start()

    dfs = {}

    for pair in wss.pairs:
        print(pair)
        dfs[pair] = pd.DataFrame(index=['orderId', 'price', 'amount', 'timestamp'])

    stop_time = datetime.datetime.utcnow() + datetime.timedelta(seconds=5)

    handle_updates_sync(wss, dfs, stop_time)
    save_dfs(dfs)

    wss.stop()


#TODO: get trades data
#TODO: deal with initial snapshot

#TODO: reconstruct orderbook at a given time
# The major issue here is figuring out when we should get a snapshot
# def reconstruct_orderbook(snapshot, data, time):


