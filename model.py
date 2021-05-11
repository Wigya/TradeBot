import numpy as np
import pandas as pd

data = pd.read_parquet('C:/Users/adam/Desktop/tradeBOT/Bitcoin_data/bitcoin_data_indexxxx.pq')
prices = data["o"].tolist()

def search_for_min(window_length: 'in days', absolute_path: 'path to the file') -> 'boolean_list':
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
            print(f'Searching in range {counter-window_length} to {counter}')
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
    df.to_parquet('C:/Users/adam/Desktop/tradeBOT/Bitcoin_data/bitcoin_usd_min.pq')
    print(pd.read_parquet('C:/Users/adam/Desktop/tradeBOT/Bitcoin_data/bitcoin_usd_min.pq'))


def search_for_max(window_length: 'in days', absolute_path: 'path to the file') -> 'boolean_list':
    window_length = window_length * 1440 # Translate window_length to minutes
    data = pd.read_parquet(absolute_path)
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
            print(f'Searching in range {counter-window_length} to {counter}')
            if prices[counter] == max_value:
                max_list.append(True)
            else:
                max_list.append(False)

        counter += 1

    if True in max_list:
        print('jest')
    #print(min_list)
    df = pd.DataFrame(max_list)
    df.columns = ['Value']
    df.to_parquet('C:/Users/adam/Desktop/tradeBOT/Bitcoin_data/bitcoin_usd_min.pq')
    print(pd.read_parquet('C:/Users/adam/Desktop/tradeBOT/Bitcoin_data/bitcoin_usd_min.pq'))



search_for_min(30,'C:/Users/adam/Desktop/tradeBOT/Bitcoin_data/bitcoin_data_indexxxx.pq')

#print(len(prices))