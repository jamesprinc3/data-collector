import gdax
import time
import pandas as pd
import datetime
import logging
import os
import pathlib
import threading
import log


class GdaxClient(gdax.WebsocketClient):
    def __init__(self, interval, pairs=None):
        super().__init__()
        pathlib.Path('parquet/gdax/orderbook/trades').mkdir(parents=True, exist_ok=True)
        pathlib.Path('parquet/gdax/orderbook/feed').mkdir(parents=True, exist_ok=True)

        self.log = log.setup_custom_logger(__name__)
        # feed_df = pd.DataFrame(index=['pair', 'orderId', 'price', 'amount', 'timestamp'])
        self.feed_df = pd.DataFrame()
        self.trades_df = pd.DataFrame(index=['pair', 'type', 'tradeId', 'price', 'amount', 'exchange_timestamp', 'timestamp'])
        self.exchange = "gdax"

        self.save_interval = interval

        self.feed = list()

        self.products = list()
        self.quarantine_path = "parquet/" + self.exchange + "/orderbook/feed/quarantine"

        thread = threading.Thread(target=self.handle_queue_with_interval, args=())
        thread.daemon = True
        thread.start()

        return

    def interrupt(self):
        self.log.info("Interrupt started")
        self.drain()
        self.save_feed_df(self.exchange, self.feed_df)
        self.close()
        self.log.info("Interrupt complete")

    def on_open(self):
        prods = gdax.PublicClient().get_products()
        for product in prods:
            self.products.append(product['id'])

        self.log.info("GDAX products: ", self.products)
        self.url = "wss://ws-feed.gdax.com/"


    def on_message(self, msg):
        self.feed.append(msg)

    def on_close(self):
        print("-- Goodbye! --")

    # Writes out the dataframe every interval
    def handle_queue_with_interval(self):
        while True:
            time.sleep(self.save_interval.seconds)
            self.drain()
            self.save_feed_df(self.exchange, self.feed_df)

    def drain(self):
        self.log.info("Starting drain")
        loc_feed = self.feed
        self.feed = list()
        self.feed_df = pd.DataFrame(loc_feed)
        self.log.info("Drain complete")

    # TODO: extract these methods out to a common class
    def save_feed_df(self, exchange, df):
        self.log.info("Saving feed df")
        path = self.generate_path(exchange)
        self.save_df(path, df)

    def generate_path(self, exchange):
        today = datetime.datetime.utcnow().date()
        path = "parquet/" + exchange + "/orderbook/feed/" + str(today) + ".parquet"
        return path

    def save_df(self, path, df):
        if df.empty:
            return

        try:
            existing_df = pd.read_parquet(path)
            df_to_save = existing_df.append(df)
            df_to_save.drop_duplicates()
        except:
            self.log.info("Could not read existing df")
            if os.path.exists(path):
                self.log.info("Path exists, moving corrupt file to quarantine")
                now = datetime.datetime.utcnow()
                os.rename(path, self.quarantine_path + str(now) + ".parquet")

            df_to_save = df

        self.log.info("Saved gdax df\n" + str(df_to_save.count()))
        df_to_save.to_parquet(path)


