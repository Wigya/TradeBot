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
        # Do something before
        start_time = time.perf_counter() # 1
        value = func(*args, **kwargs)
        # Do something after
        end_time = time.perf_counter() # 2
        run_time = end_time - start_time
        print(f'Finished {func.__name__!r} in {run_time:.4f} secs')
        return value
    return wrapper_time


@timer
def overall_most_profitable(cryptosymbol, window_length, min_or_max):
    create_directory(f'C:/Users/adam/Desktop/tradeBOT/{cryptosymbol}_data/simulation/{window_length}_days_window/{min_or_max}')
    directory = f'C:/Users/adam/Desktop/tradeBOT/{cryptosymbol}_data/simulation/{window_length}_days_window/{min_or_max}'
    most_profitable_filename = None # Declaration of variable
    highest_lose = None # Declaration of variable
    most_profitable_filename_list = []
    most_profitable_list = []
    highest_lose_list = []
    for filename in os.listdir(directory):
        path = os.path.join(directory, filename)
        if os.path.isfile(path):
            print(os.path.join(directory, filename))
            data = pd.read_parquet(f'C:/Users/adam/Desktop/tradeBOT/{cryptosymbol}_data/simulation/{window_length}_days_window/{min_or_max}/{filename}')
            profit = data['balance'].sum()
            most_profitable_filename = filename.replace('2o', '').replace('1o', '')
            print(most_profitable_filename)
            highest_lose = data.values.min(axis=0)
            most_profitable_filename_list.append(most_profitable_filename)
            most_profitable_list.append(profit)
            highest_lose_list.append(highest_lose.round(2))

    return {'most_profitable_filename': most_profitable_filename_list, 'most_profitable': most_profitable_list, 'highest_lose': highest_lose_list}



def result(cryptosymbol, window_length: int, min_or_max: str):
    most_profitable = overall_most_profitable(cryptosymbol, window_length, min_or_max)['most_profitable']
    most_profitable_filename = overall_most_profitable(cryptosymbol, window_length, min_or_max)['most_profitable_filename']
    highest_lose = overall_most_profitable(cryptosymbol, window_length, min_or_max)['highest_lose']

    table = pd.DataFrame(
        {'Sell after(days):': most_profitable_filename,
         'Profit($)/Loss($)': most_profitable,
         'Highest Loss($)': highest_lose
        })
    table.sort_values(['Profit($)/Loss($)'], inplace=True)

    return table
