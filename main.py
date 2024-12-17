import os
import time
import datetime
import pandas as pd
from dotenv import load_dotenv
from binance.client import Client

# Logging einrichten
import logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s: %(message)s',
    filename='data_extraction.log'
)

class CryptoDataExtractor:
    def __init__(self, api_key, api_secret):
        self.client = Client(api_key, api_secret)
        self.checkpoint_file = 'extraction_checkpoint.json'

    def load_checkpoint(self):
        """Lädt den letzten Checkpoint der Extraktion"""
        try:
            import json
            if os.path.exists(self.checkpoint_file):
                with open(self.checkpoint_file, 'r') as f:
                    return json.load(f)
        except Exception as e:
            logging.error(f"Fehler beim Laden des Checkpoints: {e}")
        return None

    def save_checkpoint(self, symbol, current_date):
        """Speichert den aktuellen Fortschritt"""
        try:
            import json
            checkpoint = {
                'symbol': symbol,
                'last_processed_date': current_date.strftime('%Y-%m-%d')
            }
            with open(self.checkpoint_file, 'w') as f:
                json.dump(checkpoint, f)
        except Exception as e:
            logging.error(f"Fehler beim Speichern des Checkpoints: {e}")

    def fetch_historical_minute_data(self, symbol, start_date, end_date, batch_size=30):
        """
        Extrahiert historische Minutendaten mit Fehlerbehandlung und Checkpoints
        """
        # Konvertiere Datumsstring zu datetime
        start = datetime.datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.datetime.strptime(end_date, '%Y-%m-%d')
        
        # Lade vorherigen Checkpoint
        checkpoint = self.load_checkpoint()
        if checkpoint and checkpoint['symbol'] == symbol:
            start = datetime.datetime.strptime(checkpoint['last_processed_date'], '%Y-%m-%d')
            logging.info(f"Fortgesetzt von {start} für {symbol}")

        # Initialisiere Datensammlung
        all_data = []
        current = start

        while current < end:
            try:
                # Batch-Ende berechnen
                batch_end = min(current + datetime.timedelta(days=batch_size), end)
                
                # Daten extrahieren
                klines = self.client.get_historical_klines(
                    symbol, 
                    Client.KLINE_INTERVAL_1MINUTE, 
                    current.strftime('%Y-%m-%d'),
                    batch_end.strftime('%Y-%m-%d')
                )
                
                # In DataFrame umwandeln
                batch_df = pd.DataFrame(klines, columns=[
                    f'{symbol}_Open Time', f'{symbol}_Open', f'{symbol}_High', 
                    f'{symbol}_Low', f'{symbol}_Close', f'{symbol}_Volume', 
                    f'{symbol}_Close Time', f'{symbol}_Quote Asset Volume', 
                    f'{symbol}_Number of Trades', 
                    f'{symbol}_Taker Buy Base Asset Volume', 
                    f'{symbol}_Taker Buy Quote Asset Volume', 
                    f'{symbol}_Ignore'
                ])
                
                # Zeitstempel konvertieren
                batch_df[f'{symbol}_Open Time'] = pd.to_datetime(batch_df[f'{symbol}_Open Time'], unit='ms')
                batch_df[f'{symbol}_Close Time'] = pd.to_datetime(batch_df[f'{symbol}_Close Time'], unit='ms')
                
                # Numerische Spalten konvertieren
                numeric_cols = [
                    f'{symbol}_Open', f'{symbol}_High', 
                    f'{symbol}_Low', f'{symbol}_Close', f'{symbol}_Volume'
                ]
                batch_df[numeric_cols] = batch_df[numeric_cols].astype(float)
                
                all_data.append(batch_df)
                
                # Checkpoint speichern
                self.save_checkpoint(symbol, batch_end)
                
                # Fortschritt loggen
                logging.info(f"{symbol}: Verarbeitet bis {batch_end}")
                
                # Zum nächsten Batch
                current = batch_end
                
                # Pause zwischen Anfragen
                time.sleep(0.5)
            
            except Exception as e:
                logging.error(f"Fehler bei {symbol} für Zeitraum {current} - {batch_end}: {e}")
                # Kurze Pause bei Fehler
                time.sleep(5)
                continue

        # Daten kombinieren
        if all_data:
            result_df = pd.concat(all_data, ignore_index=True)
            return result_df
        
        return None

def main():
    # Umgebungsvariablen laden
    load_dotenv()

    # API-Schlüssel aus .env
    api_key = os.getenv('BINANCE_API_KEY')
    api_secret = os.getenv('BINANCE_API_SECRET')

    # Extractor initialisieren
    extractor = CryptoDataExtractor(api_key, api_secret)

    # Symbole und Zeitraum
    symbols = ['BTCUSDT', 'BNBUSDT']
    start_date = '2017-01-01'
    end_date = '2017-06-16'

    # Finale Datensammlung
    final_data = None

    # Daten für jedes Symbol extrahieren
    dfs = {}
    for symbol in symbols:
        symbol_data = extractor.fetch_historical_minute_data(symbol, start_date, end_date)
        
        if symbol_data is not None:
            # Drucken Sie die ersten Zeilen, um zu prüfen, ob Daten vorhanden sind
            print(f"{symbol} - Erste Zeilen:")
            print(symbol_data.head())
            print(f"{symbol} - Anzahl Zeilen: {len(symbol_data)}")
            
            dfs[symbol] = symbol_data

    if len(dfs) == 2:
        final_data = pd.merge(
            dfs['BTCUSDT'], 
            dfs['BNBUSDT'], 
            how='outer'
        )        

    # Daten speichern
        filename = 'test.csv'
        final_data.to_csv(filename, index=False)
        logging.info(f"Daten gespeichert: {filename}")
        logging.info(f"Gesamtanzahl der Datenpunkte: {len(final_data)}")
    else:
        print("Nicht genügend Daten für beide Symbole")

if __name__ == "__main__":
    main()