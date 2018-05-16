import threading
import time

import gdax
import pandas as pd

import log
from parquet_writer import ParquetWriter


class GdaxClient(gdax.WebsocketClient):
    def __init__(self, output_root, save_interval, products):
        super().__init__()
        self.log = log.setup_custom_logger(__name__)

        # Set init arguments to be class fields
        self.output_root = output_root
        self.save_interval = save_interval
        self.products = products

        # Initialise global re-used variables
        self.feed = list()
        self.feed_df = pd.DataFrame()

        # Kick off the thread for this class
        thread = threading.Thread(target=self.handle_queue_with_interval, args=())
        thread.daemon = True
        thread.start()

        return

    def interrupt(self):
        self.log.info("Interrupt started")
        self.drain()
        ParquetWriter().save_feed_df(self.output_root, self.feed_df)
        self.close()
        self.log.info("Interrupt complete")

    def on_open(self):
        self.log.info("Collecting WebSocket data for GDAX products: ", self.products)
        self.url = "wss://ws-feed.gdax.com/"

    def on_message(self, msg):
        # print(msg)
        self.feed.append(msg)

    def on_close(self):
        print("-- WebSocket feed client closed --")

    # Writes out the dataframe every interval
    def handle_queue_with_interval(self):
        while True:
            time.sleep(self.save_interval.seconds)
            self.drain()
            ParquetWriter().save_feed_df(self.output_root, self.feed_df)

    def drain(self):
        self.log.info("Starting drain, message count: " + str(len(self.feed)))
        local_feed = self.feed

        if len(local_feed) > 0:
            self.feed = list()
            self.feed_df = pd.DataFrame(local_feed)
            self.log.info("Drain complete")
        else:
            self.close()
            self.start()


