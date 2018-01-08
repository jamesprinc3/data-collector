#!/usr/bin/env python
import datetime

from bitfinex import BitfinexClient
from gdax_client import GdaxClient
import signal
import sys
import os

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
        # print("aqui 1")
        gdax.interrupt()
        # print("aqui 2")
        gdax.close()
        # print("aqui 3")



    signal.signal(signal.SIGINT, interrupt_handler)
    print('Press Ctrl+C')

    pidfile_path = "/tmp/data-collector.pid"

    if os.path.isfile(pidfile_path):
        pidfile = open(pidfile_path, 'r')
        other_pid = pidfile.read()
        pidfile.close()

        print("pid: ", other_pid)
        try:
            os.kill(int(other_pid), signal.SIGINT)
        except:
            print("process doesn't exist")

    my_pid = str(os.getpid())
    pidfile = open(pidfile_path, 'w')
    pidfile.write(my_pid)
    pidfile.close()


