import threading

from bitex.api.WSS import BitfinexWSS
import pandas as pd
import datetime
import logging
import os
import pathlib
import time


class BitfinexClient():
    def __init__(self, interval):
        self.log = logging.getLogger(__name__)
        self.exchange = "bitfinex"
        logger = logging.getLogger()
        logger.setLevel(logging.WARNING)
        pathlib.Path('parquet/bitfinex/orderbook/trades').mkdir(parents=True, exist_ok=True)
        pathlib.Path('parquet/bitfinex/orderbook/feed').mkdir(parents=True, exist_ok=True)

        self.feed_df = pd.DataFrame(index=['pair', 'orderId', 'price', 'amount', 'timestamp'])
        self.trades_df = pd.DataFrame(index=['pair', 'type', 'tradeId', 'price', 'amount', 'exchange_timestamp', 'timestamp'])

        self.interval = interval

        self.wss = BitfinexWSS()

    def start(self):
        self.wss.start()

        save_thread = threading.Thread(target=self.handle_save)
        save_thread.daemon = True
        save_thread.start()

    def interrupt(self):
        self.wss.stop()
        self.drain_and_save()

    def message_handler(self, data):
        if self.type_matcher('trades', data) and (not self.trade_snapshot_matcher(data)):
            _, pair, lst = data
            trade_type = lst[0][0]
            row = lst[0][1]
            timestamp = lst[1]

            s = pd.Series({'pair': pair, 'type': trade_type, 'tradeId': row[0], 'exchange_timestamp': row[1],
                           'price': row[2], 'amount': row[3], 'timestamp': timestamp})
            self.trades_df = self.trades_df.append(s, ignore_index=True)

        if self.type_matcher('raw_order_book', data) and (not self.feed_snapshot_matcher(data)):
            _, pair, lst = data
            row = lst[0][0]
            timestamp = lst[1]
            s = pd.Series({'pair': pair, 'orderId': row[0], 'price': row[1], 'amount': row[2], 'timestamp': timestamp})
            self.feed_df = self.feed_df.append(s, ignore_index=True)

    def message_formatter(self, data):
        if self.type_matcher('trades', data) and (not self.trade_snapshot_matcher(data)):
            _, pair, lst = data
            trade_type = lst[0][0]
            row = lst[0][1]
            timestamp = lst[1]

            self.trades.append({'pair': pair, 'type': trade_type, 'tradeId': row[0], 'exchange_timestamp': row[1],
                                'price': row[2], 'amount': row[3], 'timestamp': timestamp})

        if self.type_matcher('raw_order_book', data) and (not self.feed_snapshot_matcher(data)):
            _, pair, lst = data
            row = lst[0][0]
            timestamp = lst[1]

            self.feed.append({'pair': pair, 'orderId': row[0], 'price': row[1], 'amount': row[2], 'timestamp': timestamp})

        return None

    def list_to_df(self, lst):
        return pd.DataFrame(lst)

    # TODO: move these matchers out into a different class
    def type_matcher(self, match_type, data):
        typ, _, _ = data
        return typ == match_type

    def pair_matcher(self, match_pair, data):
        _, pair, _ = data
        return pair == match_pair

    def feed_snapshot_matcher(self, data):
        _, _, info = data
        return len(info[0][0]) != 3

    def trade_snapshot_matcher(self, data):
        _, _, info = data
        return info[0][0] != 'te' and len(info[0][0]) != 'tu'

    # Writes out the dataframe every interval
    def handle_save(self):
        while True:
            time.sleep(self.interval.seconds)
            self.drain_and_save()

    # TODO: refactor this?
    def drain_and_save(self):
        print("Draining queue\n")
        # Drain the queue
        self.trades = list()
        self.feed = list()
        while not self.wss.data_q.empty():
            # print("queue size: ", self.wss.data_q.qsize())
            self.message_formatter(self.wss.data_q.get())

        self.feed_df = self.list_to_df(self.feed)
        self.trades_df = self.list_to_df(self.trades)

        self.save_feed_df(self.exchange, self.feed_df)
        self.save_trades_df(self.exchange, self.trades_df)

    # PRE: timeout must be in the future
    def handle_feed_sync(self):
        while True:
            msg = self.wss.data_q.get()
            self.message_handler(msg)


    def save_trades_df(self, exchange, df):
        today = datetime.datetime.utcnow().date()
        path = "parquet/" + exchange + "/orderbook/trades/" + str(today) + ".parquet"

        self.save_df(path, df)

    # TODO: perhaps feed isn't the best name for this?
    # TODO: make these functions just generate strings so that we can test we are generating the correct path
    def save_feed_df(self, exchange, df):
        today = datetime.datetime.utcnow().date()
        path = "parquet/" + exchange + "/orderbook/feed/" + str(today) + ".parquet"

        self.save_df(path, df)

    def save_df(self, path, df):
        if df.empty:
            return
        elif os.path.exists(path):
            existing_df = pd.read_parquet(path)
            df_to_save = existing_df.append(df)
            df_to_save.drop_duplicates()
        else:
            df_to_save = df

        print("Saved bitfinex df " + str(df_to_save.count()))
        df_to_save.to_parquet(path)


#TODO: capture reference data to run back for unit/integration tests
#TODO: reconstruct orderbook at a given time
#TODO: create file paths on startup
# The major issue here is figuring out when we should get a snapshot
# def reconstruct_orderbook(snapshot, data, time):
