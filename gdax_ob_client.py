import datetime
import json
import logging
import pathlib
import threading
import time
from typing import List

import requests


class OrderBookClient:

    def __init__(self, obc_root: str, products: List[str], interval: datetime.timedelta):
        for product in products:
            pathlib.Path(obc_root + product).mkdir(parents=True, exist_ok=True)

        self.logger = logging.getLogger()

        self.corr_root = obc_root
        self.products = products
        self.interval = interval

        self.finished = False
        self.condition = threading.Condition()
        self.scheduled_thread: threading.Timer = None

        self.thread = threading.Thread(target=self._go, daemon=True)

        return

    def _go(self):
        self.__get_orderbooks()
        while not self.finished:
            self.scheduled_thread = threading.Timer(self.interval.seconds, self.__get_orderbooks)
            self.scheduled_thread.start()

        if self.scheduled_thread is not None:
            self.scheduled_thread.cancel()

    def __get_orderbooks(self):
        lock_acquired_time = time.time()

        for product in self.products:
            if not self.finished:
                self.__get_orderbook(product)

        time_to_sleep_for = self.interval.seconds - (time.time() - lock_acquired_time)

        # return time_to_sleep_for

    def __get_orderbook(self, product: str):
        url = self.generate_request_string(product)
        response = requests.get(url)
        if response.status_code == 200:
            corr_path = self.corr_root + product + "/" + datetime.datetime.now().isoformat() + ".json"
            self.json_to_file(response.json(), corr_path)
            response.close()
        elif response.status_code == 429:
            self.logger.error("Server is rate limiting, consider increasing the request interval")
        else:
            self.logger.error("Status code was " + str(response.status_code) + " not 200, as expected")

    def start(self):
        self.thread.start()

    def handle_exception(self, request, exception):
        self.logger.error("Orderbook fetch failed: " + exception)
        self.logger.error(request)

    def __should_stop(self) -> bool:
        return self.finished

    def stop(self):
        self.finished = True
        if self.scheduled_thread is not None:
            self.scheduled_thread.cancel()

        self.logger.info("-- Closing down Order Book feed --")

        # Terminate this thread
        # self.condition.acquire()
        # self.condition.wait()
        # self.condition.notifyAll()
        # self.condition.release()

    @staticmethod
    def generate_request_string(product: str):
        return 'https://api.gdax.com/products/' + product + '/book?level=3'

    @staticmethod
    def json_to_file(json_dict: dict, file_path: str):
        with open(file_path, 'w') as fp:
            json.dump(json_dict, fp)
