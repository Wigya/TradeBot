import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

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
    plt.xticks(balance.index[::150])
    plt.title('Bitcoin profit analyse')
    plt.ylabel('Profit/Loss')
    plt.xlabel('Order number')

    plt.show()
profit_plot('C:/Users/adam/Desktop/tradeBOT/BTCUSDT_data/simulation/7_days_window/max/2o8')

def loss_plot(path):
    # Data 
    balance = pd.read_parquet(path)
    balance = balance[balance > 0].dropna()
    # Plotting
    plt.plot(balance, linestyle='-')

    # Customization
    plt.xticks(balance.index[::30])
    plt.title('Bitcoin loss analyse')
    plt.ylabel('Profit/Loss')
    plt.xlabel('Order number')

    plt.show()

#loss_plot('C:/Users/adam/Desktop/tradeBOT/BTCUSDT_data/simulation/7_days_window/min/2o20')

#def all_simulations_plot():
