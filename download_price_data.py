import requests
import pandas as pd
import finnhub
from datetime import datetime, timedelta
from pathlib import Path
import numpy as np
import time
import os
import pyarrow as pa
import pyarrow.parquet as pq
from config import api_key  # Local file


# My finnhub client ID
finnhub_client = finnhub.Client(api_key=api_key)


def make_request_create_parque(crypto_symbol, resolution, from_timestamp, to_timestamp, folder_filename):
    print('Making a request')
    api_link = f'https://finnhub.io/api/v1/crypto/candle?symbol=BINANCE:{crypto_symbol}&resolution={resolution}&from={from_timestamp}&to={to_timestamp}&token={api_key}'
    api_request = requests.get(api_link)
    max_trials = 10
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
            print(pd.read_parquet(folder_filename))
            break  # IF SOMETHING DOES NOT WORK REMOVING THIS MIGHT BE SOLUTION
        else:
            print(f'Kod nie wynosi 200, wynosi: {api_request.status_code}')
            print(f'Response for api_request is empty, retrying in 5 seconds...')
            time.sleep(5)
        if trials > max_trials:
            print('Too many trials')
            break


def set_index_to_datetime(file_path: Path, column_with_current_date: int):
    df = pd.read_parquet(file_path)
    times = []
    for time in df.t:
        if time != None or time != "None":
            time = timestamp_to_datetime(time)
            times.append(time)
    df.t = times
    df_index = df.set_index(df['t'])
    df_index = df_index.drop(df.columns[column_with_current_date], axis=1)
    df_index.to_parquet(file_path)
    print(df_index)


def timestamp_to_datetime(timestamp):
    datetime_obj = datetime.fromtimestamp(timestamp)
    return datetime_obj


def save_generated_files_to_csv(path_to_files, file_name):
    # stackoverflow code
    data_dir = Path(path_to_files)
    with open(file_name, "w") as csv_handle:
        for i, parquet_path in enumerate(data_dir.glob('*.pq')):
            df = pd.read_parquet(parquet_path)
            write_header = i == 0  # write header only on the 0th file
            df.to_csv(csv_handle, header=write_header, index=False)
            os.remove(parquet_path)


def convert_csv_to_parquet(csv_name, parquet_name):
    df = pd.read_csv(csv_name)
    df = df.drop_duplicates()
    #df.sort_values(['Date'], inplace=True)
    # os.remove(csv_name)
    df.to_parquet(parquet_name)


def create_directory(path):
    if not os.path.exists('C:/Users/adam/Desktop/test/test_1'):
        try:
            os.makedirs(path)
        except OSError:
            print('Directory already exists')
            pass


def resample_data(file_path):
    df = pd.read_parquet(file_path)
    print(df)
    df.index = pd.to_datetime(df.index)
    df = df.resample(rule='1T').agg(
        {'c': 'first', 'h': 'first', 'l': 'first', 'o': 'first', 's': 'first', 'v': 'first'})
    df = df.fillna(method='ffill')
    df.to_parquet(file_path)
    print(df)


def test(path):
    df = pd.read_parquet(path)
    timestampdiff = np.diff(df.index)
    print(timestampdiff)
    print(np.all(timestampdiff == 1))
    assert np.all(timestampdiff == 1)


def loop_every_500_rows_and_make_request(from_date: datetime, to_date: datetime, cryptosymbol):
    """Main function"""
    create_directory(f'C:/Users/adam\Desktop/tradeBOT/{cryptosymbol}_data')
    from_date_current_request = from_date
    to_date_current_request = from_date + timedelta(minutes=500)
    from_timestamp_current_request = int(
        datetime.timestamp(from_date_current_request))
    to_timestamp_current_request = int(
        datetime.timestamp(to_date_current_request))
    while True:
        if to_date_current_request > to_date:
            break
        print(
            f'Making request{from_date_current_request}, {to_date_current_request}')
        make_request_create_parque(cryptosymbol, 1, from_timestamp_current_request, to_timestamp_current_request,
                                   f'{cryptosymbol}_data\{cryptosymbol}' + '-' + str(from_timestamp_current_request) + '.pq')

        from_date_current_request += timedelta(minutes=500)
        to_date_current_request += timedelta(minutes=500)
        from_timestamp_current_request = int(
            datetime.timestamp(from_date_current_request))
        to_timestamp_current_request = int(
            datetime.timestamp(to_date_current_request))

    print('saving generated files to csv')
    save_generated_files_to_csv(
        f'C:/Users/adam\Desktop/tradeBOT/{cryptosymbol}_data/', f'{cryptosymbol}_data/{cryptosymbol}.csv')
    print('converting csv to parquet')
    convert_csv_to_parquet(
        f'C:/Users/adam\Desktop/tradeBOT/{cryptosymbol}_data/{cryptosymbol}.csv', f'{cryptosymbol}_data/{cryptosymbol}.pq')
    print('Setting index to datetime')
    set_index_to_datetime(
        f'C:/Users/adam/Desktop/tradeBOT/{cryptosymbol}_data/{cryptosymbol}.pq', 5)
    print('Resampling data')
    resample_data(
        f'C:/Users/adam/Desktop/tradeBOT/{cryptosymbol}_data/{cryptosymbol}.pq')

    # assertion whether data is sorted by time or not
    df = pd.read_parquet(f'{cryptosymbol}_data/{cryptosymbol}.pq')
    timestampdiff = np.diff(df.t.values)
    print(timestampdiff)
    print(np.all(timestampdiff == 60))
    assert np.all(timestampdiff == 60)

#loop_every_500_rows_and_make_request(datetime(2015, 5, 30), datetime(2020, 5, 10), 'ETHUSDT')
