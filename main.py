import os
import time
import datetime
import pandas as pd
from dotenv import load_dotenv
from binance.client import Client

# Logging einrichten mit detaillierterem Format
import logging
logging.basicConfig(
    level=logging.DEBUG,  # Änderung auf DEBUG Level
    format='%(asctime)s - %(levelname)s - %(funcName)s: %(message)s',
    filename='data_extraction.log'
)

class CryptoDataExtractor:
    def __init__(self, api_key, api_secret):
        try:
            self.client = Client(api_key, api_secret)
            logging.info("Binance Client erfolgreich initialisiert")
        except Exception as e:
            logging.error(f"Fehler bei der Initialisierung des Binance Clients: {e}")
            raise
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
            
    def validate_data(self, df, symbol, start_date, end_date):
        """Überprüft die Vollständigkeit und Qualität der extrahierten Daten"""
        if df is None or df.empty:
            logging.error(f"Keine Daten für {symbol}")
            return False
            
        # Prüfen ob alle erwarteten Spalten vorhanden sind
        expected_columns = [
            f'{symbol}_Open Time', f'{symbol}_Open', f'{symbol}_High',
            f'{symbol}_Low', f'{symbol}_Close', f'{symbol}_Volume'
        ]
        if not all(col in df.columns for col in expected_columns):
            logging.error(f"Fehlende Spalten für {symbol}: {[col for col in expected_columns if col not in df.columns]}")
            return False
            
        # Prüfen auf Lücken in den Zeitstempeln
        df[f'{symbol}_Open Time'] = pd.to_datetime(df[f'{symbol}_Open Time'])
        time_diff = df[f'{symbol}_Open Time'].diff()
        max_gap = time_diff.max()
        if max_gap > pd.Timedelta(minutes=2):
            logging.warning(f"Zeitlücken gefunden für {symbol}. Maximale Lücke: {max_gap}")
            
        # Prüfen des Zeitraums
        actual_start = df[f'{symbol}_Open Time'].min()
        actual_end = df[f'{symbol}_Open Time'].max()
        expected_start = pd.to_datetime(start_date)
        expected_end = pd.to_datetime(end_date)
        
        if actual_start > expected_start + pd.Timedelta(days=1):
            logging.error(f"Daten beginnen zu spät für {symbol}. Erwartet: {expected_start}, Tatsächlich: {actual_start}")
            return False
            
        if actual_end < expected_end - pd.Timedelta(days=1):
            logging.error(f"Daten enden zu früh für {symbol}. Erwartet: {expected_end}, Tatsächlich: {actual_end}")
            return False
            
        return True
            
    def fetch_historical_minute_data(self, symbol, start_date, end_date, batch_size=30, max_retries=3):
        """
        Extrahiert historische Minutendaten mit erweitertem Logging
        """
        try:
            # Konvertiere Datumsstring zu datetime
            start = datetime.datetime.strptime(start_date, '%Y-%m-%d')
            end = datetime.datetime.strptime(end_date, '%Y-%m-%d')
            
            logging.info(f"Starte Datenextraktion für {symbol} von {start} bis {end}")
            
            # Lade vorherigen Checkpoint
            checkpoint = self.load_checkpoint()
            if checkpoint and checkpoint['symbol'] == symbol:
                checkpoint_date = datetime.datetime.strptime(checkpoint['last_processed_date'], '%Y-%m-%d')
                if checkpoint_date > start:
                    start = checkpoint_date
                    logging.info(f"Fortgesetzt von Checkpoint: {start} für {symbol}")
            
            # Initialisiere Datensammlung
            all_data = []
            current = start
            
            while current < end:
                retries = 0
                success = False
                batch_data = None
                
                # Batch-Ende berechnen
                batch_end = min(current + datetime.timedelta(days=batch_size), end)
                
                while not success and retries < max_retries:
                    try:
                        logging.debug(f"Versuche Daten zu holen für {symbol} von {current} bis {batch_end}")
                        
                        # Daten extrahieren
                        klines = self.client.get_historical_klines(
                            symbol,
                            Client.KLINE_INTERVAL_1MINUTE,
                            current.strftime('%Y-%m-%d'),
                            batch_end.strftime('%Y-%m-%d')
                        )
                        
                        logging.debug(f"Anzahl der erhaltenen Klines: {len(klines)}")
                        
                        if not klines:
                            logging.warning(f"Keine Klines erhalten für {symbol} von {current} bis {batch_end}")
                            return None
                        
                        # In DataFrame umwandeln
                        batch_data = pd.DataFrame(klines, columns=[
                            f'{symbol}_Open Time', f'{symbol}_Open', f'{symbol}_High',
                            f'{symbol}_Low', f'{symbol}_Close', f'{symbol}_Volume',
                            f'{symbol}_Close Time', f'{symbol}_Quote Asset Volume',
                            f'{symbol}_Number of Trades',
                            f'{symbol}_Taker Buy Base Asset Volume',
                            f'{symbol}_Taker Buy Quote Asset Volume',
                            f'{symbol}_Ignore'
                        ])
                        
                        logging.debug(f"DataFrame erstellt mit Shape: {batch_data.shape}")
                        
                        if not batch_data.empty:
                            # Zeitstempel konvertieren
                            batch_data[f'{symbol}_Open Time'] = pd.to_datetime(batch_data[f'{symbol}_Open Time'], unit='ms')
                            batch_data[f'{symbol}_Close Time'] = pd.to_datetime(batch_data[f'{symbol}_Close Time'], unit='ms')
                            
                            # Numerische Spalten konvertieren
                            numeric_cols = [
                                f'{symbol}_Open', f'{symbol}_High',
                                f'{symbol}_Low', f'{symbol}_Close', f'{symbol}_Volume',
                                f'{symbol}_Quote Asset Volume',
                                f'{symbol}_Taker Buy Base Asset Volume',
                                f'{symbol}_Taker Buy Quote Asset Volume'
                            ]
                            batch_data[numeric_cols] = batch_data[numeric_cols].astype(float)
                            
                            all_data.append(batch_data)
                            
                            # Checkpoint speichern
                            self.save_checkpoint(symbol, batch_end)
                            
                            logging.info(f"{symbol}: Erfolgreich verarbeitet bis {batch_end}")
                            
                        success = True
                        time.sleep(1)  # Pause zwischen Requests
                        
                    except Exception as e:
                        retries += 1
                        logging.error(f"Fehler bei {symbol} für Zeitraum {current} - {batch_end}: {e} (Versuch {retries})")
                        if retries < max_retries:
                            wait_time = 10 * retries
                            logging.info(f"Warte {wait_time} Sekunden vor erneutem Versuch")
                            time.sleep(wait_time)
                        else:
                            logging.error(f"Maximale Anzahl von Versuchen erreicht für {symbol}")
                            return None
                
                # Zum nächsten Batch
                current = batch_end
            
            # Daten kombinieren
            if all_data:
                logging.info(f"Kombiniere {len(all_data)} Batches für {symbol}")
                result_df = pd.concat(all_data, ignore_index=True)
                logging.info(f"Finale Datengröße für {symbol}: {result_df.shape}")
                return result_df
            else:
                logging.error(f"Keine Daten gesammelt für {symbol}")
                return None
                
        except Exception as e:
            logging.error(f"Unerwarteter Fehler in fetch_historical_minute_data für {symbol}: {e}")
            return None
        
    def reset_checkpoint(self):
        """Setzt den Checkpoint zurück"""
        try:
            if os.path.exists(self.checkpoint_file):
                os.remove(self.checkpoint_file)
                logging.info("Checkpoint zurückgesetzt")
        except Exception as e:
            logging.error(f"Fehler beim Zurücksetzen des Checkpoints: {e}")    

def main():
    try:
        # Umgebungsvariablen laden
        load_dotenv()
        
        # API-Schlüssel aus .env
        api_key = os.getenv('BINANCE_API_KEY')
        api_secret = os.getenv('BINANCE_API_SECRET')
        
        if not api_key or not api_secret:
            logging.error("API-Schlüssel oder Secret nicht gefunden in .env")
            return
            
        logging.info(f"API-Konfiguration geladen: {api_key[:5]}...")
        
        # Extractor initialisieren
        extractor = CryptoDataExtractor(api_key, api_secret)
        
        # Symbole und Zeitraum
        symbols = ['BTCUSDT', 'BNBUSDT']
        start_date = '2017-01-01'
        end_date = '2024-12-19'
        
        data_frames = {}
        
        for symbol in symbols:
            logging.info(f"Starte Extraktion für {symbol}")
            extractor.reset_checkpoint()
            
            symbol_data = extractor.fetch_historical_minute_data(symbol, start_date, end_date)
            
            if symbol_data is not None and not symbol_data.empty:
                data_frames[symbol] = symbol_data
                logging.info(f"Extraktion für {symbol} erfolgreich. Shape: {symbol_data.shape}")
            else:
                logging.error(f"Keine Daten für {symbol} extrahiert")
                return
        
        if len(data_frames) == len(symbols):
            logging.info("Starte Merge-Prozess")
            final_data = pd.merge(
                data_frames['BTCUSDT'],
                data_frames['BNBUSDT'],
                left_on='BTCUSDT_Open Time',
                right_on='BNBUSDT_Open Time',
                how='outer'
            )
            
            filename = 'BTCBNB.csv'
            final_data.to_csv(filename, index=False)
            logging.info(f"Daten gespeichert in: {filename}")
            logging.info(f"Finale Datengröße: {final_data.shape}")
        else:
            logging.error(f"Nicht alle Symbole extrahiert. Verfügbar: {list(data_frames.keys())}")
            
    except Exception as e:
        logging.error(f"Hauptprogramm-Fehler: {e}")

if __name__ == "__main__":
    main()