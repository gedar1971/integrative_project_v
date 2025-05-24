import os
import pandas as pd
import pickle
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, r2_score


class Modeller:
    def __init__(self, logger):
        self.logger = logger
        self.model_ruta = "src/palladium/static/models/"
        self.pkl_ruta = "src/palladium/static/models/{}".format("palladium_model.pkl")
        if not os.path.exists(self.model_ruta):
            os.makedirs(self.model_ruta)

    def preparar_df(self, df=pd.DataFrame()):
        df = df.copy()
        return df, True,

    def guardar_modelo(self, modelo):
        """Guarda el modelo entrenado usando pickle"""
        try:
            with open(self.pkl_ruta, 'wb') as archivo:
                pickle.dump(modelo, archivo)
            self.logger.info(f"Modelo guardado exitosamente en {self.pkl_ruta}")
            return True
        except Exception as e:
            self.logger.error(f"Error al guardar el modelo: {str(e)}")
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
            # Aquí deberías agregar tu lógica de entrenamiento
            # Por ejemplo:
            # X = df.drop('target', axis=1)
            # y = df['target']
            # X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)
            # modelo = TuModelo()  # Reemplaza con tu modelo específico
            # modelo.fit(X_train, y_train)

            # Guardar el modelo entrenado
            # self.guardar_modelo(modelo)

            # Evaluar el modelo
            # y_pred = modelo.predict(X_test)
            # r2 = r2_score(y_test, y_pred)
            # mse = mean_squared_error(y_test, y_pred)

            # self.logger.info(f"Modelo entrenado. R2: {r2:.4f}, MSE: {mse:.4f}")
            return df, True
        except Exception as e:
            self.logger.error(f"Error en el entrenamiento: {str(e)}")
            return df, False

    def predecir_df(self, df=pd.DataFrame()):
        df = df.copy()
        valor, fecha_pre, fila = 0, "", 0
        return df, True, valor, fecha_pre, fila

