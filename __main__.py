#!/usr/bin/env python
import datetime

from bitfinex import BitfinexClient
from gdax_client import GdaxClient
import signal
import sys

if __name__ == '__main__':

    interval = datetime.timedelta(seconds=60)
    if len(sys.argv) == 2:
        interval = datetime.timedelta(seconds=int(sys.argv[1]))

    print("Interval is " + str(interval.seconds) + " seconds")
    # execute only if run as the entry point into the program
    bitfinex = BitfinexClient(interval)
    gdax = GdaxClient(interval)

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

