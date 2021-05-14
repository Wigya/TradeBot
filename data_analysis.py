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


@timer
def overall_min_most_profitable(cryptosymbol, window_length):
    create_directory(f'C:/Users/adam/Desktop/tradeBOT/{cryptosymbol}_data/simulation/{window_length}_days_window/min')
    min_directory = f'C:/Users/adam/Desktop/tradeBOT/{cryptosymbol}_data/simulation/{window_length}_days_window/min'
    new_most_profitable = 1
    most_profitable_filename = ''
    highest_lose = -999999999999 # Default value
    for filename in os.listdir(min_directory):
        print(os.path.join(min_directory, filename))
        data = pd.read_parquet(f'C:/Users/adam/Desktop/tradeBOT/{cryptosymbol}_data/simulation/{window_length}_days_window/min/{filename}')
        #print(data)
        profit = data['balance'].sum()
        old_most_profitable = profit
        if old_most_profitable > new_most_profitable:
            new_most_profitable = old_most_profitable
            most_profitable_filename = filename
            highest_lose = data.values.min(axis=0)
            #yield {'most_profitable_filename': most_profitable_filename.replace('2o', ''), 'new_most_profitable': new_most_profitable, 'highest_lose': highest_lose}
        #print(profit)
    return {'most_profitable_filename': most_profitable_filename.replace('2o', ''), 'new_most_profitable': new_most_profitable, 'highest_lose': highest_lose}


@timer
def overall_max_most_profitable(cryptosymbol, window_length):
    create_directory(f'C:/Users/adam/Desktop/tradeBOT/{cryptosymbol}_data/simulation/{window_length}_days_window/max')
    max_directory = f'C:/Users/adam/Desktop/tradeBOT/{cryptosymbol}_data/simulation/{window_length}_days_window/max'
    new_most_profitable = 1
    most_profitable_filename = ''
    highest_lose = -999999999999 # Default value
    for filename in os.listdir(max_directory):
        print(os.path.join(max_directory, filename))
        data = pd.read_parquet(f'C:/Users/adam/Desktop/tradeBOT/{cryptosymbol}_data/simulation/{window_length}_days_window/max/{filename}')
        #print(data)
        profit = data['balance'].sum()
        old_most_profitable = profit
        if old_most_profitable > new_most_profitable:
            new_most_profitable = old_most_profitable
            most_profitable_filename = filename
            highest_lose = data.values.min(axis=0)
            #yield {'most_profitable_filename': most_profitable_filename.replace('2o', ''), 'new_most_profitable': new_most_profitable, 'highest_lose': highest_lose}
        #print(profit)
    return {'most_profitable_filename': most_profitable_filename.replace('2o', ''), 'new_most_profitable': new_most_profitable, 'highest_lose': highest_lose}


def result(cryptosymbol, window_length):
    most_profitable_max = overall_max_most_profitable(cryptosymbol, window_length)['new_most_profitable']
    most_profitable_filename_max = overall_max_most_profitable(cryptosymbol, window_length)['most_profitable_filename']
    highest_max_lose = overall_max_most_profitable(cryptosymbol, window_length)['highest_lose']
    
    most_profitable_min = overall_min_most_profitable(cryptosymbol, window_length)['new_most_profitable']
    most_profitable_filename_min = overall_min_most_profitable(cryptosymbol, window_length)['most_profitable_filename']
    highest_min_lose = overall_min_most_profitable(cryptosymbol, window_length)['highest_lose']

    #if most_profitable_max > most_profitable_min:
    #print(f'Most profitable strategy is MAX: buy at peak and sell after {most_profitable_filename_max} days, profit {most_profitable_max}, highest lose is {highest_max_lose}')
    table_max = pd.DataFrame([[most_profitable_filename_max, most_profitable_max, int(highest_max_lose[0])]], columns=[['Sell after(days):', 'Profit($)/Loss($)', 'Highest Loss($)']])
    print(table_max)
    #else:
    table_min = pd.DataFrame([[most_profitable_filename_min, most_profitable_min, int(highest_min_lose[0])]], columns=[['Sell after(days):', 'Profit($)/Loss($)', 'Highest Loss($)']])
    print(table_min)
    #print(f'Most profitable strategy is MIN: buy at the lowest point and sell after {most_profitable_filename_min} days, profit {most_profitable_min}, highest lose is {highest_min_lose}')

result('BTCUSDT', '7')



#most_half_profitable_min = overall_half_min_most_profitable()['new_most_half_profitable']
#most_half_profitable_filename_min = overall_half_min_most_profitable()['most_half_profitable_filename'][1:]
#highest_half_min_lose = overall_half_min_most_profitable()['highest_half_lose']

#most_2half_profitable_min = overall_2half_min_most_profitable()['new_most_half_profitable']
#most_2half_profitable_filename_min = overall_2half_min_most_profitable()['most_half_profitable_filename'][1:]
#highest_2half_min_lose = overall_2half_min_most_profitable()['highest_half_lose']

#most_half_profitable_max = overall_half_max_most_profitable()['new_most_half_profitable']
#most_half_profitable_filename_max = overall_half_max_most_profitable()['most_half_profitable_filename'][1:]
#highest_half_max_lose = overall_half_max_most_profitable()['highest_half_lose']

#most_2half_profitable_max = overall_2half_max_most_profitable()['new_most_half_profitable']
#most_2half_profitable_filename_max = overall_2half_max_most_profitable()['most_half_profitable_filename'][1:]
#highest_2half_max_lose = overall_2half_max_most_profitable()['highest_half_lose']
