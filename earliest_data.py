from os import path
import pandas as pd
import numpy as np
import os
import time
from datetime import timedelta
import datetime
import requests
from config import api_key


def select_the_latest_file(path_to_files):
    files = []
    #time.sleep(10)
    for filename in os.listdir(path_to_files):
        if filename.startswith('1'):
            filename = int(filename)
            #print(filename)
            files.append(filename)
    latest_file = max(files)
    return latest_file

def resample_data(dataframe):
    df = dataframe
    #print('printowanie normalnego df')
    #print(df)
    df.index = pd.to_datetime(df['t'], unit='s')
    #print('Printowanie df.index')
    #print(df)
    df = df.resample(rule='1T').agg({'c':'first','h':'first','l':'first','o':'first','s':'first','v':'first'})
    df = df.fillna(method='ffill')
    #print(f'Printowanie resampled data')
    #print(df)

def make_request_create_parquet(crypto_symbol, resolution, from_timestamp, to_timestamp):
    api_link = f'https://finnhub.io/api/v1/stock/candle?symbol={crypto_symbol}&resolution={resolution}&from={from_timestamp}&to={to_timestamp}&token={api_key}'
    api_request = requests.get(api_link)
    max_trials = 10
    trials = 0
    while True:
        trials += 1
        api_request = requests.get(api_link)
        if api_request.status_code == 200:
            response = api_request.json()
            #print('Creating DataFrame')
            df = pd.DataFrame(response)
            resample_data(df)
            price_list = df['o'].to_list()
            #print('printowanie price daty')
            #print(price_list)
            return list(price_list)
        else:
            print(f'Kod nie wynosi 200, wynosi: {api_request.status_code}')
            print(f'Response for api_request is empty, retrying in 5 seconds...')
            time.sleep(5)
        if trials > max_trials:
            print('Too many trials')
            break

def check_whether_min_or_max(min_or_max, price_list, window_length):
    latest_file_name = str(select_the_latest_file(r'C:\Users\adam\Desktop\tradeBOT\AAPL_data\current_data'))
    #print(f'thats the latest file name {latest_file_name}')
    if min_or_max == 'max':
        max_price = max(price_list[:-window_length])
        #print(max_price)
    else:
        min_price = min(price_list[:-window_length])
        #print(min_price)
    df = pd.read_parquet(f'C:/Users/adam/Desktop/tradeBOT/AAPL_data/current_data/{latest_file_name}')
    latest_price = df['p'][0]
    if min_or_max == 'max':
        if latest_price > max_price:
            print(f'Max price - {latest_price}')
            print(latest_file_name)
            return True
        else:
            print('No max price')
            return False
    else:
        if latest_price < min_price:
            print(f'Min price - {latest_price}')
            return True
        else:
            print('No min price')
            return False


class Data_Processing:
    def __init__(self, symbol) -> None:
        self.symbol = symbol

    def filling_missing_data(self, path_to_current_price_files, window_length, min_or_max):
        timestampobj = select_the_latest_file(path_to_current_price_files)
        timestampobj = str(timestampobj)[:-3]
        timestampobj = int(timestampobj)
        window_length = window_length * 1440
        datetime_current_request = datetime.datetime.fromtimestamp(timestampobj)
        from_timestamp_current_request = datetime_current_request - timedelta(weeks=5)
        to_timestamp_current_request = datetime_current_request
        from_timestamp_current_request = int(datetime.datetime.timestamp(from_timestamp_current_request))
        to_timestamp_current_request = int(datetime.datetime.timestamp(to_timestamp_current_request))

        #if to_timestamp_current_request > datetime.datetime.timestamp(datetime.datetime.now()):
        #    break
        print(f'Making request from {from_timestamp_current_request},to {to_timestamp_current_request}')
        price_list = make_request_create_parquet(self.symbol, 1, from_timestamp_current_request, to_timestamp_current_request)
        result = check_whether_min_or_max(min_or_max, price_list, window_length)

        yield result
        #print(price_list)



#check_whether_min_or_max('asdsad','asdasd','asdsad')

processing_data = Data_Processing('AAPL')

#loading_dat.select_the_latest_file(r'C:\Users\adam\Desktop\tradeBOT\AAPL_data\current_data')
#processing_data.filling_missing_data('C:/Users/adam/Desktop/tradeBOT/AAPL_data/current_data/', 7, 'max')

#x = datetime.datetime.fromtimestamp(1621517433)
#print(x)

