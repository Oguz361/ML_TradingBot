from dotenv import load_dotenv
import os
import pandas as pd
from binance.client import Client
import datetime

load_dotenv()

api_key = os.getenv('BINANCE_API_KEY')
api_secret = os.getenv('BINANCE_API_SECRET')
client = Client(api_key,api_secret,testnet=True)

tickers = client.get_all_tickers()
df = pd.DataFrame(tickers)
print(df.head())

res = client.get_server_time()
ts = res['serverTime'] / 1000
your_datetime = datetime.datetime.fromtimestamp(ts)
your_datetime.strftime("%Y-%m-%d %H:%M:%S")
print(your_datetime)