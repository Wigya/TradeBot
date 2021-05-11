import numpy as np
import pandas as pd
from datetime import datetime, timedelta


def test_min(dates_file_path, main_file_path):
    dates = pd.read_parquet(dates_file_path)
    main_file = pd.read_parquet(main_file_path)
    #dates = pd.to_datetime(dates['Dates']) 
    #print(dates)
    dates_list = dates['Dates'].tolist()
    print(main_file)
    #print(dates)
    main_file.set_index(['t'], inplace=True)
    for dates in main_file.index:
        if dates in dates_list:
            print(f'Original date is {dates}')
            print(f'Date 30 days back is {dates - timedelta(days=30)}')
            #print(main_file.loc[dates-timedelta(days=30)])
            period_of_dates = main_file[(main_file.index <= dates) & (main_file.index >= dates - timedelta(days=30))]
            min_value = min(period_of_dates['o'].values)

            assert min_value == period_of_dates['o'].values[-1]

test_min('C:/Users/adam/Desktop/tradeBOT/Bitcoin_data/bitcoin_usd_min_dates.pq', 'C:/Users/adam/Desktop/tradeBOT/Bitcoin_data/bitcoin_data_indexxxx.pq')
