import pandas as pd
import numpy as np
import os
import time
import functools
from download_price_data import create_directory


def timer(func):
    """Print the runtime of the decorated function"""
    @functools.wraps(func)
    def wrapper_time(*args, **kwargs):
        # Do something before\
        start_time = time.perf_counter() # 1
        value = func(*args, **kwargs)
        # Do something after
        end_time = time.perf_counter() # 2
        run_time = end_time - start_time
        print(f'Finished {func.__name__!r} in {run_time:.4f} secs')
        return value
    return wrapper_time


#@timer
def overall_min_most_profitable(cryptosymbol, window_length):
    create_directory(f'C:/Users/adam/Desktop/tradeBOT/{cryptosymbol}_data/simulation/{window_length}_days_window/min')
    min_directory = f'C:/Users/adam/Desktop/tradeBOT/{cryptosymbol}_data/simulation/{window_length}_days_window/min'
    new_most_profitable = 1
    most_profitable_filename_list = []
    new_most_profitable_list = []
    highest_lose_list = []
    most_profitable_filename = ''
    highest_lose = -999999999999 # Default value
    for filename in os.listdir(min_directory):
        path = os.path.join(min_directory, filename)
        if os.path.isfile(path):
            print(os.path.join(min_directory, filename))
            data = pd.read_parquet(f'C:/Users/adam/Desktop/tradeBOT/{cryptosymbol}_data/simulation/{window_length}_days_window/min/{filename}')
            profit = data['balance'].sum()
            old_most_profitable = profit
            new_most_profitable = old_most_profitable
            most_profitable_filename = filename.replace('2o', '').replace('1o', '')
            print(most_profitable_filename)
            highest_lose = data.values.min(axis=0)
            most_profitable_filename_list.append(most_profitable_filename)
            new_most_profitable_list.append(new_most_profitable)
            highest_lose_list.append(highest_lose.round(2))

    return {'most_profitable_filename': most_profitable_filename_list, 'new_most_profitable': new_most_profitable_list, 'highest_lose': highest_lose_list}


#@timer
def overall_max_most_profitable(cryptosymbol, window_length):
    create_directory(f'C:/Users/adam/Desktop/tradeBOT/{cryptosymbol}_data/simulation/{window_length}_days_window/max')
    max_directory = f'C:/Users/adam/Desktop/tradeBOT/{cryptosymbol}_data/simulation/{window_length}_days_window/max'
    new_most_profitable = 1
    most_profitable_filename_list = []
    new_most_profitable_list = []
    highest_lose_list = []
    most_profitable_filename = ''
    highest_lose = -999999999999 # Default value
    for filename in os.listdir(max_directory):
        print(os.path.join(max_directory, filename))
        data = pd.read_parquet(f'C:/Users/adam/Desktop/tradeBOT/{cryptosymbol}_data/simulation/{window_length}_days_window/max/{filename}')
        #print(data)
        profit = data['balance'].sum()
        old_most_profitable = profit
        #if old_most_profitable > new_most_profitable:
        new_most_profitable = old_most_profitable
        most_profitable_filename = filename.replace('2o', '').replace('1o', '')
        highest_lose = data.values.min(axis=0)
        most_profitable_filename_list.append(most_profitable_filename)
        new_most_profitable_list.append(new_most_profitable)
        highest_lose_list.append(highest_lose.round(2))
            #yield {'most_profitable_filename': most_profitable_filename.replace('2o', ''), 'new_most_profitable': new_most_profitable, 'highest_lose': highest_lose}
        #print(profit)
    return {'most_profitable_filename': most_profitable_filename_list, 'new_most_profitable': new_most_profitable_list, 'highest_lose': highest_lose_list}


def result(cryptosymbol, window_length):
    most_profitable_max = overall_max_most_profitable(cryptosymbol, window_length)['new_most_profitable']
    most_profitable_filename_max = overall_max_most_profitable(cryptosymbol, window_length)['most_profitable_filename']
    highest_max_lose = overall_max_most_profitable(cryptosymbol, window_length)['highest_lose']
    
    most_profitable_min = overall_min_most_profitable(cryptosymbol, window_length)['new_most_profitable']
    most_profitable_filename_min = overall_min_most_profitable(cryptosymbol, window_length)['most_profitable_filename']
    highest_min_lose = overall_min_most_profitable(cryptosymbol, window_length)['highest_lose']

    table_max = pd.DataFrame(
        {'Sell after(days):': most_profitable_filename_max,
         'Profit($)/Loss($)': most_profitable_max,
         'Highest Loss($)': highest_max_lose
        })
    table_max.sort_values(['Profit($)/Loss($)'], inplace=True)

    table_min = pd.DataFrame(
        {'Sell after(days):': most_profitable_filename_min,
         'Profit($)/Loss($)': most_profitable_min,
         'Highest Loss($)': highest_min_lose
        })
    table_min.sort_values(['Profit($)/Loss($)'], inplace=True)
    #print(table_min)
    
    return {'table_min': table_min, 'table_max': table_max}