from re import sub
from time import timezone
import pandas as pd
import numpy as np
import alpaca_trade_api as tradeapi
import json
from pandas.core.algorithms import diff
import websocket
from config import *
from earliest_data import Data_Processing
import datetime
import time

ALPACA_BASE_URL = 'https://paper-api.alpaca.markets'
alpaca = tradeapi.REST(alpaca_api_key, alpaca_secret_key, ALPACA_BASE_URL)
#account = alpaca.get_account()

def buy_sell_order(buy_or_sell, symbol, quantity):
    if buy_or_sell == 'buy':
        print(f'Bought {symbol}, quantity= {quantity}')
        alpaca.submit_order(symbol, quantity, buy_or_sell)
    elif buy_or_sell == 'sell':
        print(f'Bought {symbol}, quantity= {quantity}')
        alpaca.submit_order(symbol, quantity, buy_or_sell)


class AlpacaConnector:
    def __init__(self) -> None:
        self.alpaca = tradeapi.REST(alpaca_api_key, alpaca_secret_key, ALPACA_BASE_URL)
    
    def buy_sell_order(self, buy_or_sell, symbol, quantity):
        if buy_or_sell == 'buy':
            self.alpaca.submit_order(symbol, quantity, buy_or_sell)
        elif buy_or_sell == 'sell':
            self.alpaca.submit_order(symbol, quantity, buy_or_sell)

    def trade(self, path_to_current_data, window_length, min_or_max, quantity, symbol, sell_after_minutes, by_time_or_profit):
        while True:
            min_max = processing_data.filling_missing_data(path_to_current_data, window_length, min_or_max)
            if min_max:
                print('buying order')
                buy_sell_order('buy', symbol, quantity)
            else:
                return
            positions = alpaca.list_orders()
            print(positions)
            for i in positions:
                submitted_date = i.submitted_at.replace(tzinfo=None)
                now_date = datetime.datetime.now().replace(tzinfo=None)
                difference = now_date - submitted_date
    
                if by_time_or_profit == 'time':
                    if i.status == 'Filled':
                        # Trading based on time
                        if difference > datetime.timedelta(minutes=sell_after_minutes):
                            print(f'Submitted_date {submitted_date}')
                            print(f'Now date  {now_date}')
                            print(f'Difference {difference}')
                            buy_sell_order('sell', i.symbol, i.qty)
                    else:
                        print('Your order status isnt \'Filled\'')
                else:
                    if i.status == 'Filled':
                        # Trading based on profit/loss
                        profit_loss = float(i.unrealized_pl)
                        qty = i.qty
                        if profit_loss > 1:
                            print('sprzedawanie')
                            buy_sell_order('sell', symbol, qty)
                    else:
                        print('Your order status isnt \'Filled\'')
            time.sleep(70)



alpacaObj = AlpacaConnector()

processing_data = Data_Processing('AAPL')
alpacaObj.trade('C:/Users/adam/Desktop/tradeBOT/AAPL_data/current_data/', 7, 'min', 1, 'AAPL', 1, 'profit')
