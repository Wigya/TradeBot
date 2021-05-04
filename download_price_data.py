import requests
import pandas as pd
import finnhub
import json
from datetime import datetime

# My finnhub client ID
api_key = 'c28gcdqad3ib284haabg'
finnhub_client = finnhub.Client(api_key=api_key)

def timestamp_to_daytime(timestamp):
    dt_object = datetime.fromtimestamp(timestamp)
    return dt_object

# OGARNAC PRZEKSZATALCANIE DAYTIME NA TIMESTAMP
def daytime_to_timestamp(years):
    timestamp_object = datetime.timestamp(datetime.date())
    return timestamp_object



def get_crypto_history_and_save_as_parquet(crypto_symbol, resolution, from_unix, to_unix, filename):
    """Get candlestick data for crypto symbol and save it as a parquet"""
    r = requests.get('https://finnhub.io/api/v1/crypto/candle?symbol=BINANCE:{}&resolution={}&from={}&to={}&token=c28gcdqad3ib284haabg'.format(crypto_symbol, resolution, from_unix, to_unix))
    print('Tworze dataframe')
    df = pd.DataFrame(r.json())
    print('Tworze parquet')
    parquet = df.to_parquet(filename)
    return df

#get_crypto_data = get_crypto_history_and_save_as_parquet('LTCUSDT', 1, 1430697600, 1620131275, 'LitCoin_price_history' + '.pq')

#print(get_crypto_data)


#daytime_to_timestamp()
#print(timestamp_to_daytime(1293836400))
