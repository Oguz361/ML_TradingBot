from dotenv import load_dotenv
import os
import pandas as pd
from binance.client import Client
import datetime

load_dotenv()

api_key = os.getenv('BINANCE_API_KEY')
api_secret = os.getenv('BINANCE_API_SECRET')
client = Client(api_key,api_secret,testnet=False)

def fetch_historical_data(symbol, interval, start_date, end_date):
    """
    Holt historische Kryptowährungsdaten von Binance
    
    :param symbol: Handelssymbol (z.B. 'BTCUSDT')
    :param interval: Zeitintervall ('1h', '4h', '1d')
    :param start_date: Startdatum für Datenabfrage
    :param end_date: Enddatum für Datenabfrage
    :return: pandas DataFrame mit historischen Daten
    """
    try:
        #Daten von Binance abrufen
        klines = client.get_historical_klines(
            symbol,
            interval,
            start_str=start_date,
            end_str=end_date
        )
        # Daten in Dataframe umwandeln
        df = pd.DataFrame(klines, columns=[
            'Open Time', 'Open', 'High', 'Low', 'Close', 'Volume', 
            'Close Time', 'Quote Asset Volume', 'Number of Trades', 
            'Taker Buy Base Asset Volume', 'Taker Buy Quote Asset Volume', 'Ignore'
        ])

         # Zeitstempel und numerische Spalten konvertieren
        df['Open Time'] = pd.to_datetime(df['Open Time'], unit='ms')
        df['Close Time'] = pd.to_datetime(df['Close Time'], unit='ms')
        
        # Numerische Spalten konvertieren
        numeric_columns = ['Open', 'High', 'Low', 'Close', 'Volume']
        df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric, errors='coerce')
        
        return df

        
    except Exception as e:
        print(f"Fehler bei Datenabfrage für {symbol}: {e}")
        return None  

