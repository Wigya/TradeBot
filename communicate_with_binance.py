import requests
import alpaca_trade_api as tradeapi
import time
sec = 'z7u9DREWbmiJMM27KTTbLrOq11LJlgNA2lvx5iwU'
key = 'PK2R9D910NWD76P5YR4Y'

# API endpoint url
url = 'https://paper-api.alpaca.markets'


api = tradeapi.REST(key, sec, url, api_version='v2')

# Init our account var
account = api.get_account()

print(account.status)

order = api.submit_order(symbol='ETH', qty=2, side='buy', type='market', time_in_force='day')

time.sleep(5)

print(order)