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
                    data = palladium.history(period="5d", interval="1h")

                self.logger.info('Datos descargados correctamente.')
                return [{'datetime': index.strftime('%Y-%m-%d-%H'), 'value': row['Close']} for index, row in data.iterrows()]
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
        cursor.execute('''CREATE TABLE IF NOT EXISTS historical (
                            datetime TEXT PRIMARY KEY,
                            value REAL)''')
        
        duplicates = []
        for record in data:
            try:
                cursor.execute('''INSERT INTO historical (datetime, value) VALUES (?, ?)''',
                               (record['datetime'], record['value']))
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