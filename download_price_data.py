import requests
import pandas as pd
import finnhub
import json
from datetime import datetime, timedelta
from pathlib import Path
import numpy as np

# My finnhub client ID
api_key = 'c296p1iad3ia90tkmglg'
finnhub_client = finnhub.Client(api_key=api_key)

def make_request_create_parque(crypto_symbol, resolution, from_timestamp, to_timestamp, folder_filename):
    r = requests.get('https://finnhub.io/api/v1/crypto/candle?symbol=BINANCE:{}&resolution={}&from={}&to={}&token=c296p1iad3ia90tkmglg'.format(crypto_symbol, resolution, from_timestamp, to_timestamp))
    df = pd.DataFrame(r.json())
    parque = df.to_parquet(folder_filename)
    read_parque = pd.read_parquet(folder_filename)
    return read_parque

def save_generated_files_to_csv(path_to_files, file_name):
    # stackoverflow code
    data_dir = Path(path_to_files)
    with open(file_name, "w") as csv_handle:
        for i, parquet_path in enumerate(data_dir.glob('*.pq')):
            df = pd.read_parquet(parquet_path)
            write_header = i == 0 # write header only on the 0th file
            df.to_csv(csv_handle, header=write_header, index=False)


def convert_csv_to_parquet(csv_name, parquet_name):
    df = pd.read_csv(csv_name, error_bad_lines=False)
    return df.to_parquet(parquet_name)

def loop_every_500_rows_and_make_request(from_year, from_month, from_day, from_minute, to_year, to_month, to_day, to_minute):
    """Main function"""
    from_date = datetime(from_year, from_month, from_day, minute=from_minute)
    to_date = datetime(year=to_year, month=to_month, day=to_day, minute=to_minute)
    to_timestamp = int(datetime.timestamp(to_date)) # Translate datetime object to timestamp and convert it to INT

    while from_date < to_date:
        from_date += timedelta(minutes=500)
        from_timestamp = int(datetime.timestamp(from_date)) # Translate datetime object to timestamp and convert it to INT
        print(from_timestamp)
        if to_date > from_date: # If destination date is greater than from_date then make request
            print(make_request_create_parque('BTCUSDT', 1, from_timestamp, to_timestamp, 'Bitcoin_data\Bitcoin'+ '-' + str(from_date.year)+ '-' + str(from_date.month)+ '-' + str(from_date.day)+ '-' + str(from_date.minute) + '.pq'))
        if from_date > to_date: # Prevent from from_date being greater than destination date
            from_date -= timedelta(minutes=500)
            break
    save_generated_files_to_csv('C:/Users/adam\Desktop/tradeBOT/Bitcoin_data', 'Bitcoin_data/bitcoin_data.csv')
    convert_csv_to_parquet('Bitcoin_data/bitcoin_data.csv', 'Bitcoin_data/bitcoin_data.pq')

    assert 
#loop_every_500_rows_and_make_request(2021, 4, 1, 1, 2021, 4, 5, 30)

df = pd.read_parquet('Bitcoin_data/bitcoin_data.pq')

#timestampdiff = np.diff(df.t.values)
#print(np.all(timestampdiff == 60))
#timestampdiff = np.append(timestampdiff, 60)
#print(df.loc[timestampdiff != 60])
