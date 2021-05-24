import pandas as pd
import numpy as np
import alpaca_trade_api as tradeapi
import json
import websocket
from config import *
from download_price_data import create_directory

create_directory(r'C:\Users\adam\Desktop\tradeBOT\AAPL_data\current_data')
def on_message(ws, message):
    jsonObject = json.loads(message)
    #print(jsonObject)
    try:
        df = pd.DataFrame(jsonObject['data'])
        #print(df)
        df.to_parquet('C:/Users/adam/Desktop/tradeBOT/AAPL_data/current_data/temporary.pq')
    except Exception as e:
        print(e)
    df = pd.read_parquet('C:/Users/adam/Desktop/tradeBOT/AAPL_data/current_data/temporary.pq')
    price = df['p'][0]
    time = df['t'][0]
    df.to_parquet(f'C:/Users/adam/Desktop/tradeBOT/AAPL_data/current_data/{time}')

    print(df)
    print(price)
    print(time)

    return {'price': price, 'time': time}

#x = pd.read_parquet(r'C:\Users\adam\Desktop\tradeBOT\AAPL_data\current_data\1621503523189')
#print(x)


def on_error(ws, error):
    print(error)

def on_close(ws):
    print("### closed ###")

def on_open(ws):
    ws.send('{"type":"subscribe","symbol":"AAPL"}')


def function():
    if __name__ == "__main__":
        websocket.enableTrace(True)
        ws = websocket.WebSocketApp(f"wss://ws.finnhub.io?token={api_key}",
                                  on_message = on_message,
                                  on_error = on_error,
                                  on_close = on_close)
        ws.on_open = on_open
        ws.run_forever()

function()