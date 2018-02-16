import gdax
import time
import pandas as pd
import pathlib
import threading
import log
import parquet_saver



class GdaxClient(gdax.WebsocketClient):
    def __init__(self, interval, pairs=None):
        super().__init__()
        pathlib.Path('parquet/gdax/orderbook/trades').mkdir(parents=True, exist_ok=True)
        pathlib.Path('parquet/gdax/orderbook/feed').mkdir(parents=True, exist_ok=True)

        self.log = log.setup_custom_logger(__name__)
        self.feed_df = pd.DataFrame()
        self.exchange = "gdax"

        self.save_interval = interval

        self.feed = list()
        self.products = list()

        thread = threading.Thread(target=self.handle_queue_with_interval, args=())
        thread.daemon = True
        thread.start()

        return

    def interrupt(self):
        self.log.info("Interrupt started")
        self.drain()
        parquet_saver.save_feed_df(self.exchange, self.feed_df)
        self.close()
        self.log.info("Interrupt complete")

    def on_open(self):
        prods = gdax.PublicClient().get_products()
        for product in prods:
            self.products.append(product['id'])

        self.log.info("GDAX products: ", self.products)
        self.url = "wss://ws-feed.gdax.com/"

    def on_message(self, msg):
        # print(msg)
        self.feed.append(msg)

    def on_close(self):
        print("-- Goodbye! --")

    # Writes out the dataframe every interval
    def handle_queue_with_interval(self):
        while True:
            time.sleep(self.save_interval.seconds)
            self.drain()
            parquet_saver.save_feed_df(self.exchange, self.feed_df)

    def drain(self):
        self.log.info("Starting drain, message count: " + str(len(self.feed)))
        local_feed = self.feed
        self.feed_df = pd.DataFrame(local_feed)
        self.feed.clear()
        self.log.info("Drain complete")





