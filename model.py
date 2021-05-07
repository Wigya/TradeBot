from datetime import datetime, timedelta
import numpy as np
import pandas as pd


def search_for_min(window_length, absolute_path: 'path to the file') -> list:
    data = pd.read_parquet(absolute_path)
    prices = data["o"].tolist()
    counter = 0
    min_list = []
    end_list_counter = 30
    start_list_counter = 0
    while True:
        prices = data['o'][start_list_counter:end_list_counter].tolist()
        if prices[0] == min(prices):
            print(f'to jest najmniejsza liczba{prices[0]}')
            break
        if prices[0] != min(prices):
            print(f'to nie jest najmniejsza liczba{prices[0]}')
        counter += 1
        if counter == 30:
            start_list_counter += 30
            end_list_counter += 30

search_for_min(20,'C:/Users/adam/Desktop/tradeBOT/Bitcoin_data/bitcoin_data_indexxxx.pq')
