from bitex.api.WSS import GeminiWSS
from bitex.api.WSS import BitfinexWSS
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import time

def type_matcher(match_type, data):
    typ, _, _ = data
    return typ == match_type

def pair_matcher(match_pair, data):
    _, pair, _ = data
    return pair == match_pair

#
def snapshot_matcher(data):
    _, _, info = data
    return len(info[0][0]) != 3

wss = BitfinexWSS()
wss.start()
# while 1:
    # print(wss.data_q.get())
time.sleep(2)
wss.stop()

df = pd.DataFrame(index=['orderId', 'price', 'row'])

# TODO: deal with initial snapshot
while not wss.data_q.empty():
    data = wss.data_q.get()
    _,_,lst = data
    # print(data)
    if type_matcher('raw_order_book', data) and pair_matcher('ETHBTC', data) and (not snapshot_matcher(data)) :
        print(data)
        # print(len(lst[0][0]))
        row = lst[0][0]
        print(row)
        s= pd.Series({'orderId': row[0], 'price': row[1], 'amount': row[2]})
        df = df.append(s, ignore_index=True)

print(df.to_string())


# from bitex import Kraken, Bitstamp, Gemini
# k = Kraken()
# b = Bitstamp()
# g = Gemini()

# k.ticker('XBTUSD')
# b.ticker('btceur')
# g.ticker('BTC-USD')

# k.ask(pair, price, size)
# b.ask(pair, price, size)
# g.ask(pair, price, size)

# from bitex import Bitfinex

# b = Bitfinex()

# resp = b.ticker('BTCUSD')

# print(resp)

# from bitex import Kraken
# k = Kraken()
# response = k.ticker()
# print(response.formatted)  # show formatted data
# print(response.json())  # Returns all json data