import os
import numpy as np
import pandas as pd
import pickle
import json
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, r2_score
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import StandardScaler
from .enricher import Enricher
from .utils.helpers import create_file


class Modeller:
    def __init__(self, logger, pkl_path='src/palladium/static/models/palladium_model.pkl'):
        self.logger = logger
        self.pkl_ruta = pkl_path
        self.enricher = Enricher(logger)
        self.logger.info(f"Ruta del modelo configurada en: {self.pkl_ruta}")

    def preparar_df(self, df=pd.DataFrame()):
        """
        Prepara los datos para predecir la volatilidad.
        Utiliza el enricher existente para calcular la volatilidad.
        """
        df = df.copy()
        try:
            # Asegurarse de que el índice es datetime
            if not isinstance(df.index, pd.DatetimeIndex):
                df.index = pd.to_datetime(df.index)

            # Renombrar columnas si es necesario para coincidir con el formato del enricher
            column_mapping = {
                'Close': 'close',
                'Open': 'open',
                'High': 'high',
                'Low': 'low',
                'Volume': 'volume'
            }
            df = df.rename(columns=column_mapping)
            
            # Usar el enricher para calcular la volatilidad
            df['hist_volatility'] = self.enricher.calculate_volatility(df)
            
            # Características adicionales útiles para predecir volatilidad
            df['volume_ma5'] = df['volume'].rolling(window=5).mean()
            df['volume_ma20'] = df['volume'].rolling(window=20).mean()
            df['price_range'] = (df['high'] - df['low']) / df['close']
            df['price_ma5'] = df['close'].rolling(window=5).mean()
            df['price_ma20'] = df['close'].rolling(window=20).mean()
            
            # Crear el target (volatilidad futura)
            df['target_volatility'] = df['hist_volatility'].shift(-1)
            
            # Eliminar filas con valores NaN
            df = df.dropna()
            
            # Seleccionar las características para el modelo
            features = [ 'close', 'open', 'high', 'low', 'volume',
            'volume_ma5', 'volume_ma20',
            'price_range', 'price_ma5', 'price_ma20']
            
            # Asegurarse de que todas las características estén presentes
            df = df[features + ['target_volatility']]
            
            self.logger.info("Datos preparados exitosamente para predicción de volatilidad")
            return df, True
        except Exception as e:
            self.logger.error(f"Error en la preparación de datos: {str(e)}")
            return df, False   
         
    def guardar_modelo(self, modelo):
        """Guarda el modelo entrenado usando pickle"""
        try:

            self.logger.info(f"Intentando guardar modelo en: {self.pkl_ruta}")
            
            # Usar create_file para guardar el modelo
            create_file(modelo,self.pkl_ruta, file_format='pkl')

            
            # Verificar que el archivo se creó correctamente
            if os.path.exists(self.pkl_ruta):
                file_size = os.path.getsize(self.pkl_ruta)
                self.logger.info(f"Modelo guardado exitosamente en {self.pkl_ruta} (Tamaño: {file_size/1024:.2f} KB)")
                return True
            else:
                raise Exception("El archivo no se creó correctamente")
                
        except Exception as e:
            self.logger.error(f"Error al guardar el modelo: {str(e)}")
            self.logger.error(f"Ruta intentada: {self.pkl_ruta}")
            return False

    def cargar_modelo(self):
        """Carga un modelo previamente guardado"""
        try:
            with open(self.pkl_ruta, 'rb') as archivo:
                modelo = pickle.load(archivo)
            self.logger.info("Modelo cargado exitosamente")
            return modelo
        except FileNotFoundError:
            self.logger.warning("No se encontró un modelo guardado")
            return None
        except Exception as e:
            self.logger.error(f"Error al cargar el modelo: {str(e)}")
            return None

    def entrenar_df(self, df=pd.DataFrame()):
        """Entrena el modelo con los datos proporcionados"""
        df = df.copy()
        try:
            # Preparar los datos
            # Asumimos que la última columna es el target (precio futuro)
            X = df.iloc[:, :-1]  # Todas las columnas excepto la última
            y = df.iloc[:, -1]   # Última columna como target
            
            # Dividir los datos en conjuntos de entrenamiento y prueba
            X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
            
            # Escalar los datos
            scaler = StandardScaler()
            X_train_scaled = scaler.fit_transform(X_train)
            X_test_scaled = scaler.transform(X_test)
            
            # Crear y entrenar el modelo
            modelo = RandomForestRegressor(
                n_estimators=100,  # número de árboles
                max_depth=10,      # profundidad máxima de cada árbol
                random_state=42    # para reproducibilidad
            )
            modelo.fit(X_train_scaled, y_train)
            
            # Guardar el modelo y el scaler
            self.guardar_modelo({
                'model': modelo,
                'scaler': scaler
            })
            
            # Evaluar el modelo
            y_pred = modelo.predict(X_test_scaled)
            r2 = r2_score(y_test, y_pred)
            mse = mean_squared_error(y_test, y_pred)
            
            # Calcular la importancia de las características
            feature_importance = pd.DataFrame({
                'feature': X.columns,
                'importance': modelo.feature_importances_
            }).sort_values('importance', ascending=False)
            
            self.logger.info(f"Modelo entrenado. R2: {r2:.4f}, MSE: {mse:.4f}")
            self.logger.info("\nImportancia de características:\n" + str(feature_importance))
            
            return df, True
        except Exception as e:
            self.logger.error(f"Error en el entrenamiento: {str(e)}")
            return df, False

    def predecir_df(self, df=pd.DataFrame()):
        """Realiza predicciones usando el modelo entrenado"""
        df = df.copy()
        try:
            # Cargar el modelo y el scaler
            saved_objects = self.cargar_modelo()
            if saved_objects is None:
                raise Exception("No se encontró un modelo guardado")
            
            modelo = saved_objects['model']
            scaler = saved_objects['scaler']
            
            # Preparar los datos para la predicción
            X = df.iloc[:, :-1]  # Todas las columnas excepto la última
            
            # Escalar los datos
            X_scaled = scaler.transform(X)
            
            # Realizar predicción
            predicciones = modelo.predict(X_scaled)
            
            # Agregar las predicciones al DataFrame
            df['prediccion'] = predicciones
            
            # Obtener el último valor predicho y su fecha
            ultimo_valor = predicciones[-1]
            ultima_fecha = df.index[-1] if isinstance(df.index, pd.DatetimeIndex) else ""
            ultima_fila = len(df) - 1
            
            return df, True, ultimo_valor, ultima_fecha, ultima_fila
        except Exception as e:
            self.logger.error(f"Error en la predicción: {str(e)}")
            return df, False, 0, "", 0

