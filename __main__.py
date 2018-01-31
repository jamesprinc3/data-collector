#!/usr/bin/env python
import datetime

from bitfinex import BitfinexClient
from gdax_client import GdaxClient
import signal
import os
import log
import time
import argparse

if __name__ == '__main__':

    def interrupt_handler(signal, frame):
        print('You pressed Ctrl+C!')

        bitfinex.interrupt()
        # print("aqui 1")
        gdax.interrupt()
        # print("aqui 2")
        gdax.close()
        # print("aqui 3")


    def resolve_pid(exchange):
        pidfile_path = "/tmp/data-collector-" + exchange + ".pid"

        if os.path.isfile(pidfile_path):
            pidfile = open(pidfile_path, 'r')
            other_pid = pidfile.read()
            pidfile.close()

            logger.info("previous process pid: ", other_pid)
            try:
                os.kill(int(other_pid), signal.SIGINT)
            except:
                logger.info("previous process doesn't exist")

        my_pid = str(os.getpid())
        pidfile = open(pidfile_path, 'w')
        pidfile.write(my_pid)
        pidfile.close()

    logger = log.setup_custom_logger(__name__)
    logger.info('Program started')

    parser = argparse.ArgumentParser(description='Collect crypto exchange data')
    parser.add_argument('-e', '--exchange', nargs='?',
                        help='<Required> Exchange to listen to', required=True)
    parser.add_argument('-v', '--verbose', nargs='?',
                        help='Verbosity, default: false')
    parser.add_argument('-i', '--interval', nargs='?',
                        help='Interval between saving incoming messages', type=int, required=True)

    args = parser.parse_args()
    print(args)

    interval = datetime.timedelta(seconds=int(args.interval))

    print("Interval is " + str(interval.seconds) + " seconds")
    # execute only if run as the entry point into the program
    bitfinex = BitfinexClient(interval)
    gdax = GdaxClient(interval)

    if "bitfinex" == args.exchange:
        resolve_pid("bitfinex")
        bitfinex.start()

    if "gdax" == args.exchange:
        resolve_pid("gdax")
        gdax.start()

    signal.signal(signal.SIGINT, interrupt_handler)

    while True:
        time.sleep(5)
