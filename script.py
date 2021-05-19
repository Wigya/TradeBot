import numpy as np
import pandas as pd

main_file = pd.read_parquet('C:/Users/adam/Desktop/tradeBOT/Bitcoin_data/bitcoin_data_indexxxx.pq')
list = pd.read_parquet('C:/Users/adam/Desktop/tradeBOT/Bitcoin_data/bitcoin_usd_min_tf.pq')

TFlist = list['Value'].tolist()
true_np_list = np.where(list['Value']==True)


concatenated_list = pd.concat([main_file, list], axis=1)
true_list = []
print(true_np_list[0])
for item in true_np_list[0]:
#    print(item)
    true_list.append(item)
x = 0
dates_of_mins = []


for index in concatenated_list.index:
    if index in true_list:
        print(f'Adding {concatenated_list.iloc[index].t} to list')
        dates_of_mins.append(concatenated_list.iloc[index].t)
    else:
        print('Nothing')

df = pd.DataFrame(dates_of_mins, columns=['Dates'])
df.to_parquet('C:/Users/adam/Desktop/tradeBOT/Bitcoin_data/bitcoin_usd_min_dates.pq')
print(pd.read_parquet('C:/Users/adam/Desktop/tradeBOT/Bitcoin_data/bitcoin_usd_min_dates.pq'))
