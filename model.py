import numpy as np
import pandas as pd
from download_price_data import create_directory


def search_for_min(window_length: 'in days', absolute_path: 'path to the file', cryptosymbol) -> 'boolean_list':
    window_length = window_length * 1440 # Translate window_length to minutes
    data = pd.read_parquet(absolute_path)
    prices = data["o"].tolist()
    min_list = []
    counter = 0
    while True:
        if counter > len(prices):
            break
        if counter+1 > len(prices):
            break
        if counter <= window_length:
            min_list.append(False)
        else:
            min_value = min(prices[counter-window_length:counter+1])
            #print(f'Searching in range {counter-window_length} to {counter}')
            if prices[counter] == min_value:
                min_list.append(True)
            else:
                min_list.append(False)

        counter += 1

    if True in min_list:
        print('jest')
    #print(min_list)
    df = pd.DataFrame(min_list)
    df.columns = ['Value']
    create_directory(f'C:/Users/adam\Desktop/tradeBOT/{cryptosymbol}_data/model_{window_length//1440}_days_min')
    df.to_parquet(f'C:/Users/adam/Desktop/tradeBOT/{cryptosymbol}_data/model_{window_length//1440}_days_min/min_{cryptosymbol}_tf.pq')
    print(pd.read_parquet(f'C:/Users/adam/Desktop/tradeBOT/{cryptosymbol}_data/model_{window_length//1440}_days_min/min_{cryptosymbol}_tf.pq'))


def search_for_max(window_length: 'in days', absolute_path_with_main_data: 'path to the file', cryptosymbol) -> 'boolean_list':
    window_length = window_length * 1440 # Translate window_length to minutes
    data = pd.read_parquet(absolute_path_with_main_data)
    prices = data["o"].tolist()
    max_list = []
    counter = 0
    while True:
        if counter > len(prices):
            break
        if counter+1 > len(prices):
            break
        if counter <= window_length:
            max_list.append(False)
        else:
            max_value = max(prices[counter-window_length:counter+1])
            #print(f'Searching in range {counter-window_length} to {counter}')
            if prices[counter] == max_value:
                max_list.append(True)
            else:
                max_list.append(False)

        counter += 1

    if True in max_list:
        print('jest')
    #print(max_list)
    df = pd.DataFrame(max_list)
    df.columns = ['Value']
    create_directory(f'C:/Users/adam\Desktop/tradeBOT/{cryptosymbol}_data/model_{window_length//1440}_days_max')
    df.to_parquet(f'C:/Users/adam/Desktop/tradeBOT/{cryptosymbol}_data/model_{window_length//1440}_days_max/max_{cryptosymbol}_tf.pq')
    print(pd.read_parquet(f'C:/Users/adam/Desktop/tradeBOT/{cryptosymbol}_data/model_{window_length//1440}_days_max/max_{cryptosymbol}_tf.pq'))



#search_for_max(7, 'C:/Users/adam/Desktop/tradeBOT/Bitcoin_data/bitcoin_data_indexxxx.pq', 'BTCUSDT')
data = pd.read_parquet('C:/Users/adam/Desktop/tradeBOT/Bitcoin_data/bitcoin_data_indexxxx.pq')
maxtf = pd.read_parquet('C:/Users/adam/Desktop/tradeBOT/BTCUSDT_data/model_7_days_max/max_BTCUSDT_tf.pq')
#print(data)
x = 0
min_list = []
for index in data.index:
    if maxtf.iloc[index].Value == True:
        min_list.append(data.iloc[index].t)


df = pd.DataFrame(min_list, columns=['Dates'])
df.to_parquet('C:/Users/adam/Desktop/tradeBOT/BTCUSDT_data/model_7_days_max/max_BTCUSDT_dates.pq')
print(pd.read_parquet('C:/Users/adam/Desktop/tradeBOT/BTCUSDT_data/model_7_days_max/max_BTCUSDT_dates.pq'))
#print(len(prices))