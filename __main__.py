#!/usr/bin/env python

from bitfinex import BitfinexClient
from gdax_client import GdaxClient
import signal
import sys

if __name__ == '__main__':

    # execute only if run as the entry point into the program
    bitfinex = BitfinexClient()
    gdax = GdaxClient()

    bitfinex.start()
    gdax.start()

    print("got here")

    def interrupt_handler(signal, frame):
        print('You pressed Ctrl+C!')

        bitfinex.interrupt()

        gdax.interrupt()
        gdax.close()


    signal.signal(signal.SIGINT, interrupt_handler)
    print('Press Ctrl+C')

