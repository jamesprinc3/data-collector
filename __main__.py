#!/usr/bin/env python
import configparser
import logging

from gdax_ob_client import OrderBookClient
from gdax_ws_client import GdaxClient

import signal
import log
import time
import argparse
import gdax
import datetime

from pid import Pid

expected_products = ['BCH-BTC', 'BCH-USD', 'BTC-EUR', 'BTC-GBP', 'BTC-USD', 'ETH-BTC', 'ETH-EUR', 'ETH-USD',
                     'LTC-BTC', 'LTC-EUR', 'LTC-USD', 'BCH-EUR']

prods = ['BCH-BTC']

if __name__ == '__main__':

    class DataCollector:
        def __init__(self):
            self.stop = False

            # self.logger = log.setup_custom_logger(__name__)
            self.logger = logging.getLogger()
            self.logger.info('Program started')

            parser = argparse.ArgumentParser(
                description='Collect level III orderbook data from GDAX and save it to parquet')
            parser.add_argument('--config', metavar='path', type=str, nargs='?',
                                help='path to config file')

            args = parser.parse_args()
            self.logger.debug(args)

            config = configparser.ConfigParser()
            config.read(args.config)
            self.logger.debug(config)

            ws_save_interval = datetime.timedelta(seconds=int(config['behaviour']['ws_save_interval']))
            ob_save_interval = datetime.timedelta(seconds=int(config['behaviour']['ob_save_interval']))

            # TODO: confirm paths exist

            Pid.resolve_pid(config['paths']['pidfile_path'])

            products = self.get_products()

            signal.signal(signal.SIGINT, self.interrupt_handler)

            self.gdax_client = GdaxClient(config['paths']['ws_save_root'], ws_save_interval, products)
            self.gdax_client.start()

            self.obc = OrderBookClient(config['paths']['ob_save_root'], products, ob_save_interval)
            self.obc.start()

        def interrupt_handler(self, signal, frame):
            self.logger.info('Program interrupted, closing clients...')
            self.gdax_client.interrupt()
            self.gdax_client.close()
            self.obc.stop()

            self.stop = True

        def get_products(self):
            ps = list()
            prods_json = gdax.PublicClient().get_products()
            for product in prods_json:
                ps.append(product['id'])

            return ps

        def loop(self):
            while not self.stop:
                time.sleep(1)

    dc = DataCollector()
    dc.loop()
