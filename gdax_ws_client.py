import logging
import threading
import time

import gdax
import pandas as pd

from parquet_writer import ParquetWriter


class GdaxClient(gdax.WebsocketClient):
    def __init__(self, config, products):
        super().__init__()

        self.logger = logging.getLogger()

        # Set init arguments to be class fields
        self.config = config
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
        self.logger.info("-- WebSocket interrupt started --")
        self.drain()
        ParquetWriter().save_feed_df(self.config.ws_save_root, self.feed_df)
        self.close()
        self.logger.info("-- WebSocket interrupt complete --")

    def on_open(self):
        self.logger.info("Collecting WebSocket data for GDAX products: " + ", ".join(self.products))
        self.url = "wss://ws-feed.gdax.com/"

    def on_message(self, msg):
        # print(msg)
        self.feed.append(msg)

    def on_close(self):
        self.logger.info("-- WebSocket feed client closed --")

    # Writes out the dataframe every interval
    def handle_queue_with_interval(self):
        while True:
            time.sleep(self.config.ws_save_interval.seconds)
            self.drain()
            ParquetWriter().save_feed_df(self.config.ws_save_root, self.feed_df)

    def drain(self):
        self.logger.info("Starting drain, message count: " + str(len(self.feed)))
        local_feed = self.feed

        if len(local_feed) > 0:
            self.feed = list()
            self.feed_df = pd.DataFrame(local_feed)
            self.logger.info("Drain complete")
        else:
            self.close()
            self.start()


