from datetime import date, datetime, timedelta
import pandas as pd
import numpy as np
import functools
import time
from pandas.io.parquet import read_parquet
from download_price_data import create_directory

def test(path):
    df = pd.read_parquet(path)
    timestampdiff = np.diff(df.t.values)
    #print(timestampdiff)
    #print(np.where(timestampdiff != 60000000000))
    #assert np.all(timestampdiff == 60000000000)


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
        return wrapper_time
    return wrapper_time


#@timer
def simulation(main_file, file_with_dates, amount_of_crypto, after_how_many_days_sell, cryptosymbol):
    datas = pd.read_parquet(main_file)
    when_to_buy = pd.read_parquet(file_with_dates)
    bought_crypto_value = 0
    list_of_when_to_buy = when_to_buy['Dates'].tolist()
    datas.set_index(['t'], inplace=True)
    print(datas)
    balance = []
    initial_date = []
    close_date = []
    for date in datas.index:
        #print(data)
        if date in list_of_when_to_buy:
            #print(f'Data {data}')
            try:
                #print(bought_crypto_value)
                bought_crypto_value = datas.loc[date]['o'] * amount_of_crypto
                #print(bought_crypto_value)
                after_some_days = date + timedelta(days=after_how_many_days_sell)
                initial_date.append(date.strftime("%Y-%m-%d %H:%M:%S"))
                close_date.append(str(after_some_days))
                print(date)
                #print(close_date)
                eventual_balance = (datas.loc[after_some_days]['o'] * amount_of_crypto) - bought_crypto_value
                balance.append(eventual_balance)
                #print(eventual_balance)
            except Exception as e:
                print(f'error found: {e}')

    print(balance)
    print(initial_date)
    print(close_date)
    create_directory(f'C:/Users/adam/Desktop/tradeBOT/{cryptosymbol}_data/simulation/7_days_window/min/dates')
    initial_datedf = pd.DataFrame(initial_date, columns=['initial_date']).to_parquet(f'C:/Users/adam/Desktop/tradeBOT/{cryptosymbol}_data/simulation/7_days_window/min/dates/initial_dates{amount_of_crypto}o{after_how_many_days_sell}.pq')
    close_datedf = pd.DataFrame(close_date, columns=['close_date']).to_parquet(f'C:/Users/adam/Desktop/tradeBOT/{cryptosymbol}_data/simulation/7_days_window/min/dates/end_dates{amount_of_crypto}o{after_how_many_days_sell}.pq')
    eventualdf = pd.DataFrame(balance, columns=['balance'])
    create_directory(f'C:/Users/adam/Desktop/tradeBOT/{cryptosymbol}_data/simulation/7_days_window/min')
    eventualdf.to_parquet(f'C:/Users/adam/Desktop/tradeBOT/{cryptosymbol}_data/simulation/7_days_window/min/{amount_of_crypto}o{after_how_many_days_sell}')
    print(eventualdf)

x = 1


while x < 50:
    simulation('C:/Users/adam/Desktop/tradeBOT/Bitcoin_data/bitcoin_data_indexxxx.pq', 'C:/Users/adam/Desktop/tradeBOT/BTCUSDT_data/model_7_days_min/min_BTCUSDT_dates.pq', 1, x, 'BTCUSDT')
    x += 2