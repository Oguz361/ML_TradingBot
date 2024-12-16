import os
from dotenv import load_dotenv
import pandas as pd
import numpy as np
from binance.client import Client
import datetime
import time
import math

# Umgebungsvariablen laden
load_dotenv()

# Binance API Anmeldedaten
api_key = os.getenv('BINANCE_API_KEY')
api_secret = os.getenv('BINANCE_API_SECRET')

# Client initialisieren
client = Client(api_key, api_secret)

def fetch_historical_minute_data(symbol, start_date, end_date, batch_size=500):
    """
    Holt historische 1-Minuten-Daten mit Batching
    
    :param symbol: Handelssymbol (z.B. 'BTCUSDT')
    :param start_date: Startdatum
    :param end_date: Enddatum
    :param batch_size: Anzahl der Klines pro Anfrage
    :return: pandas DataFrame mit historischen Daten
    """
    # Konvertiere Datumsstring zu datetime
    start = datetime.datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.datetime.strptime(end_date, '%Y-%m-%d')
    
    # Initialisiere leeren DataFrame
    all_data = []
    
    # Aktuelle Position
    current = start
    
    while current < end:
        # Berechne Endzeitpunkt des Batches
        batch_end = min(current + datetime.timedelta(days=batch_size), end)
        
        try:
            # Hole Klines für den Batch
            klines = client.get_historical_klines(
                symbol, 
                Client.KLINE_INTERVAL_1MINUTE, 
                current.strftime('%Y-%m-%d'),
                batch_end.strftime('%Y-%m-%d')
            )
            
            # Wandle Klines in DataFrame um
            batch_df = pd.DataFrame(klines, columns=[
                'Open Time', 'Open', 'High', 'Low', 'Close', 'Volume', 
                'Close Time', 'Quote Asset Volume', 'Number of Trades', 
                'Taker Buy Base Asset Volume', 'Taker Buy Quote Asset Volume', 'Ignore'
            ])
            
            all_data.append(batch_df)
            
            # Bewege zum nächsten Batch
            current = batch_end
            
            # Pause zwischen Anfragen
            time.sleep(0.5)
            
            print(f"Verarbeitet bis: {current}")
        
        except Exception as e:
            print(f"Fehler bei Datenabfrage: {e}")
            # Warte und versuche es erneut
            time.sleep(5)
    
    # Kombiniere alle Batches
    if all_data:
        result_df = pd.concat(all_data, ignore_index=True)
        
        # Konvertiere Zeitstempel
        result_df['Open Time'] = pd.to_datetime(result_df['Open Time'], unit='ms')
        result_df['Close Time'] = pd.to_datetime(result_df['Close Time'], unit='ms')
        
        # Konvertiere numerische Spalten
        numeric_columns = ['Open', 'High', 'Low', 'Close', 'Volume']
        result_df[numeric_columns] = result_df[numeric_columns].astype(float)
        
        return result_df
    
    return None

# Beispielaufruf
symbols = ['BTCUSDT', 'BNBUSDT']
start_date = '2017-01-01'
end_date = '2024-12-31'

for symbol in symbols:
    df = fetch_historical_minute_data(symbol, start_date, end_date)
    
    if df is not None:
        filename = f'{symbol}_minute_data.csv'
        df.to_csv(filename, index=False)
        print(f"Daten für {symbol} gespeichert: {filename}")
        print(f"Anzahl der Datenpunkte: {len(df)}")