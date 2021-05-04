import requests
import pandas as pd
import finnhub
import json

# My finnhub client ID
api_key = 'c28gcdqad3ib284haabg'
finnhub_client = finnhub.Client(api_key=api_key)


def get_crypto_history_and_save_as_parquet(crypto_symbol, resolution, from_unix, to_unix, filename):
    """Get candlestick data for crypto symbol and save it as a parquet"""
    r = requests.get('https://finnhub.io/api/v1/crypto/candle?symbol=BINANCE:{}&resolution={}&from={}&to={}&token=c28gcdqad3ib284haabg'.format(crypto_symbol, resolution, from_unix, to_unix))
    df = pd.DataFrame(r.json())
    parquet = df.to_parquet(filename)
    return parquet

get_crypto_data = get_crypto_history_and_save_as_parquet('BTCEUR', 1, 1293836400, 1620131275, 'bitcoin_history')

print(get_crypto_data)
