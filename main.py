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
client = Client(api_key, api_secret, testnet=False)

def fetch_combined_historical_data(symbols, interval, start_date, end_date):
    """
    Holt historische Kryptowährungsdaten für mehrere Symbole
    
    :param symbols: Liste von Handelssymbolen (z.B. ['BTCUSDT', 'BNBUSDT'])
    :param interval: Zeitintervall ('1h', '4h', '1d')
    :param start_date: Startdatum für Datenabfrage
    :param end_date: Enddatum für Datenabfrage
    :return: pandas DataFrame mit kombinierten historischen Daten
    """
    combined_data = []
    
    for symbol in symbols:
        try:
            # Daten von Binance abrufen
            klines = client.get_historical_klines(
                symbol, 
                interval, 
                start_str=start_date, 
                end_str=end_date
            )
            
            # Daten in DataFrame umwandeln
            df = pd.DataFrame(klines, columns=[
                f'{symbol}_Open Time', f'{symbol}_Open', f'{symbol}_High', 
                f'{symbol}_Low', f'{symbol}_Close', f'{symbol}_Volume', 
                f'{symbol}_Close Time', f'{symbol}_Quote Asset Volume', 
                f'{symbol}_Number of Trades', 
                f'{symbol}_Taker Buy Base Asset Volume', 
                f'{symbol}_Taker Buy Quote Asset Volume', 
                f'{symbol}_Ignore'
            ])
            
            # Zeitstempel und numerische Spalten konvertieren
            time_columns = [f'{symbol}_Open Time', f'{symbol}_Close Time']
            df[time_columns] = df[time_columns].apply(pd.to_datetime, unit='ms')
            
            numeric_columns = [
                f'{symbol}_Open', f'{symbol}_High', f'{symbol}_Low', 
                f'{symbol}_Close', f'{symbol}_Volume'
            ]
            df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric, errors='coerce')
            
            combined_data.append(df)
            
            # Kleine Pause zwischen Anfragen
            time.sleep(0.5)
        
        except Exception as e:
            print(f"Fehler bei Datenabfrage für {symbol}: {e}")
    
    # Kombiniere DataFrames basierend auf Zeitstempel
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

# Anwendungsbeispiel
symbols = ['BTCUSDT', 'BNBUSDT']
interval = '1h'
start_date = '2023-01-01'
end_date = datetime.datetime.now().strftime('%Y-%m-%d')

# Daten abrufen
combined_data = fetch_combined_historical_data(symbols, interval, start_date, end_date)

# Daten speichern
if combined_data is not None:
    combined_data.to_csv('combined_crypto_data.csv', index=False)
    print("Daten erfolgreich gespeichert.")
    print(f"Datensatz-Größe: {combined_data.shape}")
    print("\nSpaltenbeispiel:")
    print(combined_data.columns.tolist())