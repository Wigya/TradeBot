import requests
import pandas as pd
import finnhub
from datetime import datetime, timedelta
from pathlib import Path
import numpy as np
import time
import os

# My finnhub client ID
api_key = 'c296p1iad3ia90tkmglg'
finnhub_client = finnhub.Client(api_key=api_key)


def make_request_create_parque(crypto_symbol, resolution, from_timestamp, to_timestamp, folder_filename):

    print('Making a request')
    api_link = 'https://finnhub.io/api/v1/crypto/candle?symbol=BINANCE:{}&resolution={}&from={}&to={}&token=c296p1iad3ia90tkmglg'.format(crypto_symbol, resolution, from_timestamp, to_timestamp)
    api_request = requests.get(api_link)
    max_trials = 5
    trials = 0
    while True:
        trials += 1
        api_request = requests.get(api_link)
        if api_request.status_code == 200:
            response = api_request.json()
            print('Creating DataFrame')
            df = pd.DataFrame(response)
            print('Creating Parquet')
            parquet = df.to_parquet(folder_filename)
            return print(pd.read_parquet(folder_filename))
        else:
            print(f'Kod nie wynosi 200, wynosi: {api_request.status_code}')
            print(f'Response for api_request is empty, retrying in 5 seconds...')
            time.sleep(5)
        if trials > max_trials:
            print('Too many trials')
            break


def timestamp_to_datetime(timestamp):
    datetime_obj = datetime.fromtimestamp(timestamp)
    return datetime_obj


def save_generated_files_to_csv(path_to_files, file_name):
    # stackoverflow code
    data_dir = Path(path_to_files)
    with open(file_name, "w") as csv_handle:
        for i, parquet_path in enumerate(data_dir.glob('*.pq')):
            df = pd.read_parquet(parquet_path)
            write_header = i == 0 #  write header only on the 0th file
            df.to_csv(csv_handle, header=write_header, index=False)
            os.remove(parquet_path)

def convert_csv_to_parquet(csv_name, parquet_name):
    df = pd.read_csv(csv_name)
    df = df.drop_duplicates()
    df.sort_values(['t'], inplace=True)
    os.remove(csv_name)
    df.to_parquet(parquet_name)


def loop_every_500_rows_and_make_request(from_date: datetime, to_date: datetime) -> None:
    """Main function"""
    from_date_current_request = from_date
    to_date_current_request = from_date + timedelta(minutes=500)
    from_timestamp_current_request = int(datetime.timestamp(from_date_current_request))
    to_timestamp_current_request = int(datetime.timestamp(to_date_current_request))
    while True:
        if to_date_current_request > to_date:
            break
        print(f'Making request{from_date_current_request}, {to_date_current_request}')
        make_request_create_parque('BTCUSDT', 1, from_timestamp_current_request, to_timestamp_current_request, 'Bitcoin_data\Bitcoin'+ '-' + str(from_timestamp_current_request) + '.pq')

        from_date_current_request += timedelta(minutes=500)
        to_date_current_request += timedelta(minutes=500)
        from_timestamp_current_request = int(datetime.timestamp(from_date_current_request))
        to_timestamp_current_request = int(datetime.timestamp(to_date_current_request))
    save_generated_files_to_csv('C:/Users/adam\Desktop/tradeBOT/Bitcoin_data', 'Bitcoin_data/bitcoin_data.csv')
    convert_csv_to_parquet('Bitcoin_data/bitcoin_data.csv', 'Bitcoin_data/bitcoin_data.pq')

    
    # assertion whether data is sorted by time or not
    df = pd.read_parquet('Bitcoin_data/bitcoin_data.pq')
    timestampdiff = np.diff(df.t.values)
    print(timestampdiff)
    print(np.all(timestampdiff == 60))
    assert np.all(timestampdiff == 60)


loop_every_500_rows_and_make_request(datetime(2020, 4, 20), datetime(2020, 4, 23))





#df = pd.read_parquet('Bitcoin_data/bitcoin_data.pq')
#timestampdiff = np.diff(df.t.values)
#print(timestampdiff)
#print(np.all(timestampdiff == 60))











#df = pd.read_parquet('Bitcoin_data/bitcoin_data.pq')
#xd = pd.read_csv('Bitcoin_data/bitcoin_data.csv')
#timestampdiff = np.diff(df.t.values)
#x = timestampdiff == 60
#s = 0
#for d in np.diff(df.t.values):
#    print(d)
#    if d != 60:
#        s += 1

#print(s)
#print(np.all(timestampdiff != 60))
#date = datetime.datetime(2016,2,1, minute=30)
#oneday = datetime.timedelta(minutes=10)
#
#date_counter = 0
#while 2 > 0:
#     date += oneday
#     print(date)