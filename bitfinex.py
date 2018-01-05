import threading

from bitex.api.WSS import BitfinexWSS
import pandas as pd
import datetime
import logging
import os
import pathlib

log = logging.getLogger(__name__)


def type_matcher(match_type, data):
    typ, _, _ = data
    return typ == match_type


def pair_matcher(match_pair, data):
    _, pair, _ = data
    return pair == match_pair


def feed_snapshot_matcher(data):
    _, _, info = data
    return len(info[0][0]) != 3


def trade_snapshot_matcher(data):
    _, _, info = data
    return info[0][0] != 'te' and len(info[0][0]) != 'tu'


# Writes out the dataframe every interval
def handle_queue_with_interval(wss, interval):
    exchange = "bitfinex"
    while True:
        now = datetime.datetime.utcnow()
        timeout = now + interval
        feed_df, trades_df = handle_feed_sync(wss, timeout)
        save_orderbook_feed_df(exchange, feed_df)
        save_trades_df(exchange, trades_df)


# PRE: timeout must be in the future
def handle_feed_sync(wss, timeout):
    feed_df = pd.DataFrame(index=['pair', 'orderId', 'price', 'amount', 'timestamp'])
    trades_df = pd.DataFrame(index=['pair', 'type', 'tradeId', 'price', 'amount', 'exchange_timestamp', 'timestamp'])
    while datetime.datetime.utcnow() < timeout:
        feed_df = handle_feed_update(wss.data_q.get(), feed_df)
        trades_df = handle_trades_update(wss.data_q.get(), trades_df)
    return feed_df, trades_df


def handle_feed_update(data, df):
    if type_matcher('raw_order_book', data) and (not feed_snapshot_matcher(data)):  # and pair_matcher('ETHBTC', data):
        _, pair, lst = data
        row = lst[0][0]
        timestamp = lst[1]
        s = pd.Series({'pair': pair, 'orderId': row[0], 'price': row[1], 'amount': row[2], 'timestamp': timestamp})
        df = df.append(s, ignore_index=True)
    return df


def handle_trades_update(data, df):
    if type_matcher('trades', data) and (not trade_snapshot_matcher(data)):
        _, pair, lst = data
        trade_type = lst[0][0]
        row = lst[0][1]
        timestamp = lst[1]

        s = pd.Series({'pair': pair, 'type': trade_type, 'tradeId': row[0], 'exchange_timestamp': row[1],
                       'price': row[2], 'amount': row[3], 'timestamp': timestamp})
        df = df.append(s, ignore_index=True)
    return df


def save_trades_df(exchange, df):
    today = datetime.datetime.utcnow().date()
    path = "parquet/" + exchange + "/orderbook/trades/" + str(today) + ".parquet"

    save_df(path, df)


# TODO: perhaps feed isn't the best name for this?
# TODO: make these functions just generate strings so that we can test we are generating the correct path
def save_orderbook_feed_df(exchange, df):
    today = datetime.datetime.utcnow().date()
    path = "parquet/" + exchange + "/orderbook/feed/" + str(today) + ".parquet"

    save_df(path, df)


def save_df(path, df):
    if df.empty:
        return
    elif os.path.exists(path):
        existing_df = pd.read_parquet(path)
        df_to_save = existing_df.append(df)
    else:
        df_to_save = df

    log.debug("Saved feed df " + str(df.count()))
    df_to_save.to_parquet(path)


def bitfinex():
    pathlib.Path('parquet/bitfinex/orderbook/trades').mkdir(parents=True, exist_ok=True)
    pathlib.Path('parquet/bitfinex/orderbook/feed').mkdir(parents=True, exist_ok=True)

    save_interval = datetime.timedelta(seconds=5)

    queue_thread = threading.Thread(target=queue_getter(save_interval))
    queue_thread.start()

    # TODO: change this to wait for some kind of termination, and shut down safely (i.e. save the current state?)
    while True:
        pass


def queue_getter(interval: datetime.timedelta):
    wss = BitfinexWSS()
    wss.start()

    handle_queue_with_interval(wss, interval)

    wss.stop()


logger = logging.getLogger()
logger.setLevel(logging.WARNING)
bitfinex()


#TODO: get trades data
#TODO: deal with initial snapshot
#TODO: capture reference data to run back for unit/integration tests
#TODO: reconstruct orderbook at a given time
#TODO: create file paths on startup
# The major issue here is figuring out when we should get a snapshot
# def reconstruct_orderbook(snapshot, data, time):
