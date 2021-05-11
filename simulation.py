from datetime import timedelta
import pandas as pd
import numpy as np

def simulation(amount_of_crypto, after_how_many_days_sell):
    datas = pd.read_parquet('C:/Users/adam/Desktop/tradeBOT/Bitcoin_data/bitcoin_data_indexxxx.pq')
    when_to_buy = pd.read_parquet('C:/Users/adam/Desktop/tradeBOT/Bitcoin_data/bitcoin_usd_min_dates.pq')

    list_of_when_to_buy = when_to_buy['Dates'].tolist()
    datas.set_index(['t'], inplace=True)
    print(list_of_when_to_buy)
    for data in datas.index:
        if data in list_of_when_to_buy:
            bought_crypto_value = datas.loc[data]['o'] * amount_of_crypto
            after_some_days = data + timedelta(days=after_how_many_days_sell)
            eventual_balance = (datas.loc[after_some_days]['o'] * amount_of_crypto) - bought_crypto_value
            print(eventual_balance)
simulation(2, 30)
