import datetime
import time
from unittest import TestCase

from gdax_ob_client import OrderBookClient


class TestGetOrderBook(TestCase):
    # def setUp(self):
    # feed_dd = DataLoader().load_real_data("../data/real_data_sample.parquet")
    # self.feed_df = feed_dd.compute()

    # def test_can_load_data(self):
    #     json_dict = OrderBookClient.get_orderbook('BTC-USD')
    #
    #     assert json_dict != {}
    #
    # def test_can_save_data(self):
    #     json_dict = OrderBookClient.get_orderbook('BTC-USD')
    #     OrderBookClient.json_to_file(json_dict, "orderbook.json")
    #
    #     assert True

    # NOTE: Don't run this as part of the test suite, it's slow
    def test_can_init(self):
        corr_root = "./"
        products = ['BCH-BTC', 'BCH-USD', 'BTC-EUR', 'BTC-GBP', 'BTC-USD', 'ETH-BTC', 'ETH-EUR', 'ETH-USD', 'LTC-BTC',
                    'LTC-EUR', 'LTC-USD', 'BCH-EUR']
        interval = datetime.timedelta(seconds=2)

        obc = OrderBookClient(corr_root, products, interval)
        obc.start()

        for i in range(0,5):
            time.sleep(1)

        obc.stop()
