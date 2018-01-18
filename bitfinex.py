import threading

from bitex.api.WSS import BitfinexWSS
import pandas as pd
import pathlib
import time
import log
import parquet_saver

class BitfinexClient():
    def __init__(self, interval):
        self.log = log.setup_custom_logger(__name__)
        self.exchange = "bitfinex"
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

        self.log.info("Save thread started up")

    def interrupt(self):
        self.log.info("Interrupt started")
        self.wss.stop()
        self.drain_and_save()
        self.log.info("Interrupt complete")

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
        self.log.info("Draining queue")
        # Drain the queue
        self.trades = list()
        self.feed = list()
        while not self.wss.data_q.empty():
            self.message_formatter(self.wss.data_q.get())

        self.log.info("Queue drained, " + str(len(self.trades)) + " trades, " + str(len(self.feed)) + " orders")

        self.feed_df = self.list_to_df(self.feed)
        self.trades_df = self.list_to_df(self.trades)

        parquet_saver.save_feed_df(self.exchange, self.feed_df)
        parquet_saver.save_trades_df(self.exchange, self.trades_df)


#TODO: capture reference data to run back for unit/integration tests
#TODO: reconstruct orderbook at a given time
# The major issue here is figuring out when we should get a snapshot
# def reconstruct_orderbook(snapshot, data, time):
