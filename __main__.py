#!/usr/bin/env python
import datetime

from bitfinex import BitfinexClient
from gdax_client import GdaxClient
import signal
import sys
import os
import logging
import log
import time

if __name__ == '__main__':

    logger = log.setup_custom_logger(__name__)
    logger.info('Program started')

    interval = datetime.timedelta(seconds=60)
    if len(sys.argv) == 3:
        interval = datetime.timedelta(seconds=int(sys.argv[1]))
        should_disable_logs = sys.argv[2] == "True"
        # if should_disable_logs:
            # logger = logging.getLogger()
            # logger.setLevel(logging.WARNING)

    print("Interval is " + str(interval.seconds) + " seconds")
    # execute only if run as the entry point into the program
    bitfinex = BitfinexClient(interval)
    gdax = GdaxClient(interval)

    bitfinex.start()
    gdax.start()

    def interrupt_handler(signal, frame):
        print('You pressed Ctrl+C!')

        bitfinex.interrupt()
        # print("aqui 1")
        gdax.interrupt()
        # print("aqui 2")
        gdax.close()
        # print("aqui 3")

    signal.signal(signal.SIGINT, interrupt_handler)

    pidfile_path = "/tmp/data-collector.pid"

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

    while True:
        time.sleep(5)

