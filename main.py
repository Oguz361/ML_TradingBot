import numpy as np
import pandas as pd
import talib

class dataPrep:
    def __init__(self):
        self.dataset = pd.read_csv("C:/Users/oguzk/Desktop/Projekte/ML_TradingBot1/BTCBNB.csv")
        self.dataset = self.dataset.drop([
            'BTCUSDT_Close Time', 'BNBUSDT_Close Time', 
            'BTCUSDT_Quote Asset Volume', 'BNBUSDT_Quote Asset Volume',
            'BTCUSDT_Number of Trades', 'BNBUSDT_Number of Trades',
            'BTCUSDT_Taker Buy Base Asset Volume', 'BNBUSDT_Taker Buy Base Asset Volume',
            'BTCUSDT_Taker Buy Quote Asset Volume', 'BNBUSDT_Taker Buy Quote Asset Volume',
            'BTCUSDT_Ignore', 'BNBUSDT_Ignore'
        ], axis=1)

        # Adding indicators
        self.dataset['RSI'] = talib.RSI(self.dataset['BTCUSDT_Close'], timeperiod=15)
        self.dataset['EMAF'] = talib.EMA(self.dataset['BTCUSDT_Close'], timeperiod=20)
        self.dataset['EMAM'] = talib.EMA(self.dataset['BTCUSDT_Close'], timeperiod=100)
        self.dataset['EMAS'] = talib.EMA(self.dataset['BTCUSDT_Close'], timeperiod=150)
        
        self.dataset['TargetNextClose'] = self.dataset['BNBUSDT_Close'].shift(-1)
        
        print(self.dataset.head())

def main():
    dataPrep()

if __name__ == '__main__':
    main()