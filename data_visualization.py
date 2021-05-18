from datetime import datetime
import matplotlib.pyplot as plt
from numpy.core.fromnumeric import resize
from numpy.core.numeric import full
from numpy.lib.function_base import copy
import pandas as pd
import numpy as np
import os
from pandas.core.tools.datetimes import to_datetime
from data_analysis import result
from download_price_data import create_directory


def price_plot(path):
    # Data
    data = pd.read_parquet(path)
    date = data['t']
    open_price = data['o']

    # Plotting
    plt.plot(open_price)

    # Customization
    plt.ylabel('Price')
    plt.xlabel('Date')
    plt.xticks(date[::50000])
    plt.title('Bitcoin price history 2018-2021')
    plt.show()

#price_plot('C:/Users/adam/Desktop/tradeBOT/Bitcoin_data/bitcoin_data_indexxxx.pq')


def profit_plot(path):
    # Data 
    balance = pd.read_parquet(path)
    print(balance)

    # Plotting
    plt.plot(balance)

    # Customization
    print(balance.index[::150])
    plt.xticks(balance.index[::150])
    plt.title('Bitcoin profit analyse')
    plt.ylabel('Profit/Loss')
    plt.xlabel('Order number')

    plt.show()


#profit_plot('C:/Users/adam/Desktop/tradeBOT/BTCUSDT_data/simulation/7_days_window/max/2o8')


def loss_plot(path):
    # Data 
    balance = pd.read_parquet(path)
    balance = balance[balance > 0].dropna()
    # Plotting
    plt.plot(balance)

    # Customization
    plt.xticks(balance.index[::30])
    plt.title('Bitcoin loss analyse')
    plt.ylabel('Profit/Loss')
    plt.xlabel('Order number')

    plt.show()


def data_analysis_result_visualization():
    # Data
    table_min = result('BTCUSDT', '7')['table_min']
    print(table_min)
    table_min['Sell after(days):'] = table_min['Sell after(days):'].astype(int)
    table_min.sort_values(by='Sell after(days):', ascending=True, inplace=True)
    print(table_min['Profit($)/Loss($)'].values)
    print(table_min['Sell after(days):'].values)
    
    # Plotting
    plt.plot(table_min['Sell after(days):'].values, table_min['Profit($)/Loss($)'].values)
    plt.show()


#data_analysis_result_visualization()


def concatenate_into_full_data(balance_directory, initial_dates_directory):
    """Function concatenates dates and balances into one file"""
    full_data_directory = 'C:/Users/adam/Desktop/tradeBOT/BTCUSDT_data/simulation/7_days_window/min/full_data/'
    create_directory(full_data_directory)
    for filename in os.listdir(balance_directory):
        file_directory = balance_directory + filename
        try:
            df = pd.read_parquet(f'C:/Users/adam/Desktop/tradeBOT/BTCUSDT_data/simulation/7_days_window/min/dates/end_dates{filename}.pq')
            fd = pd.read_parquet(f'C:/Users/adam/Desktop/tradeBOT/BTCUSDT_data/simulation/7_days_window/min/dates/initial_dates{filename}.pq')
            fdd = pd.read_parquet(f'C:/Users/adam/Desktop/tradeBOT/BTCUSDT_data/simulation/7_days_window/min/{filename}')
        except FileNotFoundError as e:
            break
        full_data = pd.concat([fdd['balance'], fd['initial_date'], df['close_date']], axis=1)
        full_data.dropna(inplace=True)
        full_data.to_parquet(f'C:/Users/adam/Desktop/tradeBOT/BTCUSDT_data/simulation/7_days_window/min/full_data/full_data{filename}.pq')

#concatenate_into_full_data('C:/Users/adam/Desktop/tradeBOT/BTCUSDT_data/simulation/7_days_window/min/', 'C:/Users/adam/Desktop/tradeBOT/BTCUSDT_data/simulation/7_days_window/min/dates/')


def resample_data(full_data_directory):
    """Resampling data to visualization"""
    for filename in os.listdir(full_data_directory):
        file_directory = full_data_directory + filename
        print(file_directory)
        df = pd.read_parquet(file_directory)
        df['initial_date'] =pd.to_datetime(df['initial_date'])
        df.set_index(['initial_date'], inplace=True)
        df_resampled = df.resample(rule='1T').agg({'close_date': 'last', 'balance': 'last'})
        df_resampled['balance'] = df_resampled['balance'].fillna(0)
        df_resampled['balance'] = df_resampled['balance'].cumsum()
        df_resampled.to_parquet(file_directory)
    
#resample_data(r'C:/Users/adam/Desktop/tradeBOT/BTCUSDT_data/simulation/7_days_window/min/full_data/')


def full_data_visualization(full_data_directory):
    plt.figure(figsize=[16, 9])
    for filename in os.listdir(full_data_directory):
        file_directory = full_data_directory + filename
        df = pd.read_parquet(file_directory)
        plt.plot(df['balance'], label=filename)
    plt.legend()
    plt.show()


full_data_visualization(r'C:/Users/adam/Desktop/tradeBOT/BTCUSDT_data/simulation/7_days_window/min/full_data/')
