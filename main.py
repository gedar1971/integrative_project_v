import argparse
from src.collector import DataCollector  # Aseguramos que la importación sea absoluta

def main():
    parser = argparse.ArgumentParser(description='Aplicación para recolectar y guardar datos históricos del palladium.')
    parser.add_argument('--start_date', type=str, help='Fecha de inicio en formato YYYY-MM-DD')
    parser.add_argument('--end_date', type=str, help='Fecha de fin en formato YYYY-MM-DD')
    args = parser.parse_args()

    collector = DataCollector()
    data = collector.download_data(start_date=args.start_date, end_date=args.end_date)
    collector.save_to_db(data)
    collector.save_to_csv(data)

if __name__ == '__main__':
    main()