import os
from dotenv import load_dotenv
import pandas as pd
import numpy as np
from binance.client import Client
import datetime
import time

# Umgebungsvariablen laden
load_dotenv()

# Binance API Anmeldedaten
api_key = os.getenv('BINANCE_API_KEY')
api_secret = os.getenv('BINANCE_API_SECRET')

# Client initialisieren
client = Client(api_key, api_secret)

def fetch_combined_minute_data(symbols, start_date, end_date, batch_size=500):
    """
    Holt historische 1-Minuten-Daten für mehrere Symbole
    
    :param symbols: Liste von Handelssymbolen
    :param start_date: Startdatum
    :param end_date: Enddatum
    :param batch_size: Anzahl der Klines pro Anfrage
    :return: pandas DataFrame mit historischen Daten
    """
    # Konvertiere Datumsstring zu datetime
    start = datetime.datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.datetime.strptime(end_date, '%Y-%m-%d')
    
    # Initialisiere leeren DataFrame für kombinierte Daten
    combined_data = []
    
    for symbol in symbols:
        # Initialisiere leeren DataFrame für dieses Symbol
        symbol_data = []
        
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
                
                # Wandle Klines in DataFrame um mit Symbol-Präfix
                batch_df = pd.DataFrame(klines, columns=[
                    f'{symbol}_Open Time', f'{symbol}_Open', f'{symbol}_High', 
                    f'{symbol}_Low', f'{symbol}_Close', f'{symbol}_Volume', 
                    f'{symbol}_Close Time', f'{symbol}_Quote Asset Volume', 
                    f'{symbol}_Number of Trades', 
                    f'{symbol}_Taker Buy Base Asset Volume', 
                    f'{symbol}_Taker Buy Quote Asset Volume', 
                    f'{symbol}_Ignore'
                ])
                
                symbol_data.append(batch_df)
                
                # Bewege zum nächsten Batch
                current = batch_end
                
                # Pause zwischen Anfragen
                time.sleep(0.5)
                
                print(f"{symbol} verarbeitet bis: {current}")
            
            except Exception as e:
                print(f"Fehler bei Datenabfrage für {symbol}: {e}")
                # Warte und versuche es erneut
                time.sleep(5)
        
        # Kombiniere Batches für dieses Symbol
        if symbol_data:
            symbol_result = pd.concat(symbol_data, ignore_index=True)
            combined_data.append(symbol_result)
    
    # Kombiniere Daten aller Symbole
    if combined_data:
        # Wähle den ersten DataFrame als Basis
        result_df = combined_data[0]
        
        # Führe Joins für die weiteren DataFrames durch
        for additional_df in combined_data[1:]:
            result_df = pd.merge(
                result_df, 
                additional_df, 
                left_on=f'{symbols[0]}_Open Time', 
                right_on=f'{symbols[1]}_Open Time', 
                how='outer'
            )
        
        # Sortiere nach Zeitstempel
        result_df.sort_values(by=f'{symbols[0]}_Open Time', inplace=True)
        
        return result_df
    
    return None

# Beispielaufruf
symbols = ['BTCUSDT', 'BNBUSDT']
start_date = '2017-01-01'
end_date = '2024-12-16'

# Daten abrufen
combined_data = fetch_combined_minute_data(symbols, start_date, end_date)

# Daten speichern
if combined_data is not None:
    filename = 'combined_minute_crypto_data.csv'
    combined_data.to_csv(filename, index=False)
    print(f"Daten gespeichert: {filename}")
    print(f"Gesamtanzahl der Datenpunkte: {len(combined_data)}")