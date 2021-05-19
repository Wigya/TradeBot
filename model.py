from pathlib import Path
import numpy as np
import pandas as pd
from download_price_data import create_directory


def search_for_min_max(window_length: int, absolute_path: Path, cryptosymbol, min_or_max) -> list:
    window_length = window_length * 1440 # Translate window_length to minutes
    data = pd.read_parquet(absolute_path)
    prices = data["o"].tolist()
    min_max_list = []
    counter = 0
    while True:
        if counter > len(prices):
            break
        if counter+1 > len(prices):
            break
        if counter <= window_length:
            min_max_list.append(False)
        else:
            if min_or_max == 'min':
                min_max_value = min(prices[counter-window_length:counter+1])
            else:
                min_max_value = max(prices[counter-window_length:counter+1])
            print(f'Searching in range {counter-window_length} to {counter}')
            if prices[counter] == min_max_value:
                min_max_list.append(True)
            else:
                min_max_list.append(False)

        counter += 1

    if True in min_max_list:
        print('jest')
    #print(min_list)
    df = pd.DataFrame(min_max_list)
    df.columns = ['Value']
    create_directory(f'C:/Users/adam/Desktop/tradeBOT/{cryptosymbol}_data/model_{window_length//1440}_days_{min_or_max}')
    df.to_parquet(f'C:/Users/adam/Desktop/tradeBOT/{cryptosymbol}_data/model_{window_length//1440}_days_{min_or_max}/{min_or_max}_{cryptosymbol}_tf.pq')
    print(pd.read_parquet(f'C:/Users/adam/Desktop/tradeBOT/{cryptosymbol}_data/model_{window_length//1440}_days_{min_or_max}/{min_or_max}_{cryptosymbol}_tf.pq'))

#search_for_min_max(7, r'C:\Users\adam\Desktop\tradeBOT\AAPL_data\AAPLL.pq', 'AAPL', 'max')

def true_false_list_to_dates(main_data_file_path, true_false_file_path, min_or_max: str, cryptosymbol: str, windowlength: str):
    data = pd.read_parquet(main_data_file_path)
    print(data)
    data.reset_index(inplace=True)
    true_false_df = pd.read_parquet(true_false_file_path)
    #print(data)
    min_list = []
    for index in data.index:
        if true_false_df.iloc[index].Value == True:
            min_list.append(data.iloc[index].t)


    df = pd.DataFrame(min_list, columns=['Dates'])
    df.to_parquet(f'C:/Users/adam/Desktop/tradeBOT/{cryptosymbol}_data/model_{windowlength}_days_{min_or_max}/{min_or_max}_{cryptosymbol}_dates.pq')
    print(pd.read_parquet(f'C:/Users/adam/Desktop/tradeBOT/{cryptosymbol}_data/model_{windowlength}_days_{min_or_max}/{min_or_max}_{cryptosymbol}_dates.pq'))

true_false_list_to_dates(r'C:\Users\adam\Desktop\tradeBOT\AAPL_data\AAPLL.pq', r'C:\Users\adam\Desktop\tradeBOT\AAPL_data\model_7_days_min\min_AAPL_tf.pq', 'max', 'AAPL', '7')
