import argparse
from src.collector import DataCollector
from src.enricher import Enricher
from src.logger import logger

def main():
    parser = argparse.ArgumentParser(description='Aplicación para recolectar, enriquecer y guardar datos históricos del palladium.')
    parser.add_argument('--start_date', type=str, help='Fecha de inicio en formato YYYY-MM-DD')
    parser.add_argument('--end_date', type=str, help='Fecha de fin en formato YYYY-MM-DD')
    args = parser.parse_args()

    # Inicializar colector y enriquecedor
    collector = DataCollector()
    enricher = Enricher(logger)

    # Descargar datos
    raw_data = collector.download_data(start_date=args.start_date, end_date=args.end_date)
    
    # Enriquecer datos con KPIs
    enriched_data = enricher.enrich_data(raw_data)
    
    # Guardar datos enriquecidos
    collector.save_to_db(enriched_data)
    collector.save_to_csv(enriched_data)

if __name__ == '__main__':
    main()