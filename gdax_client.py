import gdax
import time
import pandas as pd
import datetime
import logging
import os
import pathlib
import threading


class GdaxClient(gdax.WebsocketClient):
    def __init__(self, pairs=None):
        super().__init__()
        pathlib.Path('parquet/gdax/orderbook/trades').mkdir(parents=True, exist_ok=True)
        pathlib.Path('parquet/gdax/orderbook/feed').mkdir(parents=True, exist_ok=True)

        self.log = logging.getLogger(__name__)
        # feed_df = pd.DataFrame(index=['pair', 'orderId', 'price', 'amount', 'timestamp'])
        self.feed_df = pd.DataFrame()
        self.trades_df = pd.DataFrame(index=['pair', 'type', 'tradeId', 'price', 'amount', 'exchange_timestamp', 'timestamp'])
        self.exchange = "gdax"

        self.save_interval = datetime.timedelta(seconds=5)

        thread = threading.Thread(target=self.handle_queue_with_interval, args=())
        thread.daemon = True
        thread.start()

        return

    def interrupt(self):
        self.close()
        self.save_feed_df("gdax", self.feed_df)

    def on_open(self):
        self.url = "wss://ws-feed.gdax.com/"
        self.products = ["LTC-USD"]
        self.message_count = 0
        print("Lets count the messages!")

    def on_message(self, msg):
        self.message_count += 1

        # print(msg)

        s = pd.Series(msg)
        self.feed_df = self.feed_df.append(s, ignore_index=True)

    def on_close(self):
        print("-- Goodbye! --")

    # Writes out the dataframe every interval
    def handle_queue_with_interval(self):
        exchange = "gdax"
        while True:
            time.sleep(self.save_interval.seconds)
            self.save_feed_df(exchange, self.feed_df)

    # TODO: extract these methods out to a common class
    def save_feed_df(self, exchange, df):
        path = self.generate_path(exchange)
        self.save_df(path, df)

    def generate_path(self, exchange):
        today = datetime.datetime.utcnow().date()
        path = "parquet/" + exchange + "/orderbook/feed/" + str(today) + ".parquet"
        return path

    def save_df(self, path, df):
        if df.empty:
            return
        elif os.path.exists(path):
            existing_df = pd.read_parquet(path)
            df_to_save = existing_df.append(df)
            df_to_save.drop_duplicates()
        else:
            df_to_save = df

        print("Saved gdax df " + str(df_to_save.count()))
        df_to_save.to_parquet(path)


