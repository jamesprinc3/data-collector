import datetime
import json
import logging
import pathlib
import threading
import time
from typing import List

import requests

from config import Config


class OrderBookClient:

    def __init__(self, config: Config, products: List[str]):
        for product in products:
            pathlib.Path(config.ob_save_root + product).mkdir(parents=True, exist_ok=True)

        self.config = config

        self.logger = logging.getLogger()
        self.products = products

        self.finished = False
        self.condition = threading.Condition()
        self.scheduled_thread = None

        self.thread = threading.Thread(target=self._go, daemon=True)

        return

    def _go(self):
        t0 = time.time()
        self.__get_orderbooks()
        time_to_wait = self.config.ob_save_interval.seconds - (time.time() - t0)

        self.condition.acquire()
        while not self.condition.wait_for(lambda: self.finished, time_to_wait):
            self.scheduled_thread = threading.Thread(target=self.__get_orderbooks)
            self.scheduled_thread.start()

    def __get_orderbooks(self):
        for product in self.products:
            if not self.finished:
                self.__get_orderbook(product)

    def __get_orderbook(self, product: str):
        url = self.generate_request_string(product)
        response = requests.get(url)
        if response.status_code == 200:
            ob_path = self.config.ob_save_root + product + "/" + datetime.datetime.now().isoformat() + ".json"
            self.json_to_file(response.json(), ob_path)
            self.logger.info("Written orderbook to " + ob_path + " at " + datetime.datetime.now().isoformat())
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
        self.logger.info("-- Closing down Order Book client --")
        self.finished = True
        if self.scheduled_thread is not None:
            self.scheduled_thread.cancel()

        self.logger.info("-- Order Book client closed --")
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
