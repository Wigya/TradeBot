import requests
import pandas as pd
import finnhub
from datetime import datetime, timedelta
from pathlib import Path
import numpy as np
import time
import os

# My finnhub client ID
api_key = 'c2adi7aad3iegn22dis0'
finnhub_client = finnhub.Client(api_key=api_key)


def make_request_create_parque(crypto_symbol, resolution, from_timestamp, to_timestamp, folder_filename):

    print('Making a request')
    api_link = 'https://finnhub.io/api/v1/crypto/candle?symbol=BINANCE:{}&resolution={}&from={}&to={}&token=c2adi7aad3iegn22dis0'.format(crypto_symbol, resolution, from_timestamp, to_timestamp)
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


def set_index_to_datetime(dataframe: 'eg. df', file_name: 'give absolute path also', column_with_current_date: int):
    times = []
    for time in dataframe.t:
        if time != None or time != "None":
            time = timestamp_to_datetime(time)
            times.append(time)
    dataframe.t = times
    df_index = df.set_index(df['t'])
    df_index = df_index.drop(df.columns[column_with_current_date], axis=1)
    df_index.to_parquet(file_name)

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


#loop_every_500_rows_and_make_request(datetime(2016, 6, 20), datetime(2021, 4, 23))

#df = pd.read_parquet('Bitcoin_data/bitcoin_data.pq')
df_index = pd.read_parquet('C:/Users/adam/Desktop/tradeBOT/Bitcoin_data/bitcoin_data_indexx.pq')
#x = df_index.fillna(method='ffill', inplace=True, axis=1)
#print(df_index)
#print(df_index)
#timestampdiff = np.diff(df_index.o.values)
#print(timestampdiff)
#print(timestampdiff)
#print(np.all(timestampdiff == 0))
#d = np.where(df_index.o == 0)
#print(d)
#print(df_index)

#df = pd.read_parquet('C:/Users/adam/Desktop/tradeBOT\Bitcoin_data/bitcoin_data_indexx.pq')
#timestampdiff = np.diff(df.t.values).astype(np.int64)
#
#print(np.all(timestampdiff == 60000000000))
#print(timestampdiff)
#xdiff = np.diff(d)
#print(xdiff)

#df.sort_values(['t'], inplace=True)
#
#print(df)
#
#timestampdiff = np.diff(df.t.values)
#print(timestampdiff)
#print(np.all(timestampdiff == 60))

#########df.fillna(method='ffill', inplace=True)
df_index.replace(0, np.nan, inplace=True)
df_index.fillna(method='ffill', inplace=True)
df_index.to_csv('C:/Users/adam/Desktop/tradeBOT/Bitcoin_data/bitcoin_data_indexxxx.csv')
#print(df_index)

##TEST
#print(np.where(timestampdiff != 60))
#print(df.iloc[[8878]])
#print(df.iloc[[8879]])
#print(df.iloc[[8880]])
#
#print(df.iloc[[10378]])
#print(df.iloc[[10379]])
#print(df.iloc[[10380]])

## TEST THAT THERE ARE BLANK FIELDS IN THE DATA
#api_link = 'https://finnhub.io/api/v1/crypto/candle?symbol=BINANCE:{}&resolution={}&from={}&to={}&token=c2adi7aad3iegn22dis0'.format('BTCUSDT', 1, 1529978400, 1529978460)
#api_request = requests.get(api_link)
#response = api_request.json()
#print(response)