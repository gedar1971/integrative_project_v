import sqlite3
import pandas as pd
import csv
import logging
from datetime import datetime
import yfinance as yf
import os
import argparse
import time
from src.logger import logger
from src.utils.helpers import create_file

class DataCollector:
    def __init__(self, db_path='src/palladium/static/data/historical.db', csv_path='src/palladium/static/data/historical.csv'):
        self.db_path = db_path
        self.csv_path = csv_path
        self.logger = logger

        db_dir = os.path.dirname(self.db_path)
        if not os.path.exists(db_dir):
            os.makedirs(db_dir)
            self.logger.info(f'Directorio creado: {db_dir}')
  
    def download_data(self, start_date=None, end_date=None):
        self.logger.info('Descargando datos históricos del paladio...')
        palladium = yf.Ticker("PA=F")
        

        retries = 5
        for attempt in range(retries):
            try:
                if start_date and end_date:
                    data = palladium.history(start=start_date, end=end_date, interval="1h")
                else:
                    data = palladium.history(period="1y", interval="1d")
                    

                self.logger.info('Datos descargados correctamente.')
                return [{
                    'datetime': index.strftime('%Y-%m-%d-%H'),
                    'open': row['Open'],
                    'high': row['High'],
                    'low': row['Low'],
                    'close': row['Close'],
                    'volume': row['Volume'],
                    'dividends': row['Dividends'],
                    'stock_splits': row['Stock Splits']
                } for index, row in data.iterrows()]
            except Exception as e:
                if 'Rate limited' in str(e) and attempt < retries - 1:
                    wait_time = 2 ** attempt
                    self.logger.warning(f'Rate limit alcanzado. Reintentando en {wait_time} segundos...')
                    time.sleep(wait_time)
                else:
                    self.logger.error(f'Error al descargar datos: {e}')
                    raise

    def save_to_db(self, data):
        self.logger.info('Guardando datos en la base de datos SQLite...')
        connection = sqlite3.connect(self.db_path)
        cursor = connection.cursor()

        # Drop existing table to ensure clean schema
        cursor.execute('DROP TABLE IF EXISTS historical')
        
        # Create table with proper schema
        cursor.execute('''CREATE TABLE historical (
                            datetime TEXT PRIMARY KEY,
                            open REAL,
                            high REAL,
                            low REAL,
                            close REAL,
                            volume REAL,
                            dividends REAL,
                            stock_splits REAL,
                            volatility REAL,
                            SMA_20 REAL,
                            EMA_20 REAL,
                            RSI REAL,
                            daily_return REAL,
                            cumulative_return REAL,
                            momentum REAL)''')
        
        duplicates = []
        for record in data:
            try:
                # Redondear todos los valores numéricos a 2 decimales
                rounded_record = {
                    'datetime': record['datetime'],
                    'open': round(record['open'], 4),
                    'high': round(record['high'], 4),
                    'low': round(record['low'], 4),
                    'close': round(record['close'], 4),
                    'volume': round(record['volume'], 4),
                    'dividends': round(record['dividends'], 4),
                    'stock_splits': round(record['stock_splits'], 4),
                    'volatility': round(record.get('volatility', 0), 4),
                    'SMA_20': round(record.get('SMA_20', 0), 4),
                    'EMA_20': round(record.get('EMA_20', 0), 4),
                    'RSI': round(record.get('RSI', 0), 4),
                    'daily_return': round(record.get('daily_return', 0), 4),
                    'cumulative_return': round(record.get('cumulative_return', 0), 4),
                    'momentum': round(record.get('momentum', 0), 4)
                }
                
                cursor.execute('''INSERT INTO historical 
                    (datetime, open, high, low, close, volume, dividends, stock_splits,
                     volatility, SMA_20, EMA_20, RSI, daily_return, cumulative_return, momentum) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                    (rounded_record['datetime'], rounded_record['open'], rounded_record['high'], 
                     rounded_record['low'], rounded_record['close'], rounded_record['volume'], 
                     rounded_record['dividends'], rounded_record['stock_splits'],
                     rounded_record['volatility'], rounded_record['SMA_20'], 
                     rounded_record['EMA_20'], rounded_record['RSI'], 
                     rounded_record['daily_return'], rounded_record['cumulative_return'], 
                     rounded_record['momentum']))
            except sqlite3.IntegrityError:
                duplicates.append(record['datetime'])
        
        if duplicates:
            self.logger.warning(f'Se encontraron {len(duplicates)} registros duplicados. No fueron incertados en la base de datos.')
        
        connection.commit()
        connection.close()
        self.logger.info('Datos guardados en la base de datos correctamente.')

    def save_to_csv(self, data):
        self.logger.info('Guardando datos en el archivo CSV...')
        df_api = pd.DataFrame(data)
        create_file(df_api, self.csv_path, file_format='csv')
        self.logger.info('Datos guardados en el archivo CSV correctamente.')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Descargar y guardar datos históricos del paladio.')
    parser.add_argument('--start_date', type=str, help='Fecha de inicio en formato YYYY-MM-DD')
    parser.add_argument('--end_date', type=str, help='Fecha de fin en formato YYYY-MM-DD')
    args = parser.parse_args()

    collector = DataCollector()
    data = collector.download_data(start_date=args.start_date, end_date=args.end_date)
    collector.save_to_db(data)
    collector.save_to_csv(data)