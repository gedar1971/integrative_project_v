import argparse
import pandas as pd
from src.collector import DataCollector
from src.enricher import Enricher
from src.modeller import Modeller
from src.logger import logger

def main():
    parser = argparse.ArgumentParser(description='Aplicación para recolectar, enriquecer y predecir datos históricos del palladium.')
    parser.add_argument('--start_date', type=str, help='Fecha de inicio en formato YYYY-MM-DD')
    parser.add_argument('--end_date', type=str, help='Fecha de fin en formato YYYY-MM-DD')
    parser.add_argument('--train', action='store_true', help='Entrenar el modelo')
    parser.add_argument('--predict', action='store_true', help='Realizar predicciones')
    args = parser.parse_args()

    # Inicializar componentes
    collector = DataCollector()
    enricher = Enricher(logger)
    modeller = Modeller(logger)

    # Descargar datos
    raw_data = collector.download_data(start_date=args.start_date, end_date=args.end_date)
    
    # Enriquecer datos con KPIs
    enriched_data = enricher.enrich_data(raw_data)
    
    # Guardar datos enriquecidos
    collector.save_to_db(enriched_data)
    collector.save_to_csv(enriched_data)

    # Convertir a DataFrame para el modelo
    df = pd.DataFrame(enriched_data)
    df.set_index('datetime', inplace=True)

    # Preparar datos para el modelo
    prepared_df, success = modeller.preparar_df(df)
    if not success:
        logger.error("Error al preparar los datos para el modelo")
        return

    if args.train:
        # Entrenar el modelo
        logger.info("Iniciando entrenamiento del modelo...")
        _, success = modeller.entrenar_df(prepared_df)
        if success:
            logger.info("Modelo entrenado exitosamente")
        else:
            logger.error("Error en el entrenamiento del modelo")
            return

    if args.predict:
        # Realizar predicciones
        logger.info("Realizando predicciones...")
        df_predicciones, success, ultimo_valor, ultima_fecha, _ = modeller.predecir_df(prepared_df)
        if success:
            logger.info(f"Última predicción de volatilidad: {ultimo_valor:.4f} para la fecha {ultima_fecha}")
            # Guardar predicciones
            df_predicciones.to_csv('src/palladium/static/data/predictions.csv')
            logger.info("Predicciones guardadas en 'predictions.csv'")
        else:
            logger.error("Error al realizar las predicciones")

if __name__ == '__main__':
    main()