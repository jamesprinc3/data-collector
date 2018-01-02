import threading

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

def snapshot_matcher(data):
    _, _, info = data
    return len(info[0][0]) != 3


def handle_feed(wss, dfs):
    while not wss.data_q.empty():
        handle_feed_update(wss.data_q.get(), dfs)

# Writes out the dataframe every interval
def handle_feed_interval(wss, dfs, interval):
    while True:
        now = datetime.datetime.utcnow()
        timeout = now + interval
        handle_feed_sync(wss, dfs, timeout)

#PRE: timeout must be in the future
def handle_feed_sync(wss, dfs, timeout):
    while datetime.datetime.utcnow() < timeout:
        handle_feed_update(wss.data_q.get(), dfs)

def handle_feed_update(data, dfs):
    if type_matcher('raw_order_book', data) and (not snapshot_matcher(data)):  # and pair_matcher('ETHBTC', data):
        _, pair, lst = data
        row = lst[0][0]
        timestamp = lst[1]
        s = pd.Series({'orderId': row[0], 'price': row[1], 'amount': row[2], 'timestamp': timestamp})
        dfs[pair] = dfs[pair].append(s, ignore_index=True)

def handle_snapshot_update(data, dfs):
    _, pair, lst = data
    if type_matcher('raw_order_book', data) and snapshot_matcher(data):
        # print(data)
        timestamp = lst[1]
        orders = lst[0][0]
        # print(orders)
        for order in orders:
            # print(order)
            s = pd.Series({'orderId': order[0], 'price': order[1], 'amount': order[2], 'timestamp': timestamp})
            dfs[pair] = dfs[pair].append(s, ignore_index=True)

def save_dfs(dfs):
    for pair, df in dfs.items():
        #TODO: handle case where df is empty
        df.to_parquet("parquet/" + pair + ".parquet")

def save_orderbook_snapshot_df(exchange, pair, df):
    df.to_parquet("parquet/" + exchange + "/" + "orderbook/snapshots/" + pair + ".parquet")

#TODO: perhaps feed isn't the best name for this?
#TODO: make these functions just generate strings so that we can test we are generating the correct path
def save_orderbook_feed_df(exchange, pair, df):
    df.to_parquet("parquet/" + exchange + "/" + "orderbook/feed/" + pair + ".parquet")


def bitfinex():
    feed_save_interval = datetime.timedelta(seconds=5)
    snapshot_save_interval = datetime.timedelta(seconds=5)

    # feed_thread = threading.Thread(target=feed_getter(feed_save_interval))
    # feed_thread.start()
    print("here")
    snapshots_thread = threading.Thread(target=snapshots_getter(snapshot_save_interval))
    snapshots_thread.start()
    # TODO: change this to wait for some kind of termination, and shut down safely (i.e. save the current state?)
    while True:
        pass

def snapshots_getter(interval: datetime.timedelta):
    # while True:
        wss = BitfinexWSS()
        wss.start()

        dfs = {}

        for pair in wss.pairs:
            print(pair)
            dfs[pair] = pd.DataFrame(index=['orderId', 'price', 'amount', 'timestamp'])

        time.sleep(2)
        wss.stop()

        while not wss.data_q.empty():
            handle_snapshot_update(wss.data_q.get(), dfs)

        for pair, df in dfs.items():
            #TODO: handle empty df
            save_orderbook_snapshot_df("bitfinex", pair, df)

        # time.sleep(interval.seconds - 1)

def feed_getter(interval: datetime.timedelta):
    wss = BitfinexWSS()
    wss.start()

    dfs = {}

    for pair in wss.pairs:
        print(pair)
        dfs[pair] = pd.DataFrame(index=['orderId', 'price', 'amount', 'timestamp'])

    handle_feed_interval(wss, dfs, interval)

    wss.stop()

bitfinex()


#TODO: get trades data
#TODO: deal with initial snapshot
#TODO: capture reference data to run back for unit/integration tests
#TODO: reconstruct orderbook at a given time
#TODO: create file paths on startup
# The major issue here is figuring out when we should get a snapshot
# def reconstruct_orderbook(snapshot, data, time):


