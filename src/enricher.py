import numpy as np
import pandas as pd

class Enricher:
    def __init__(self,logger):
        self.logger = logger
        
    def calculate_volatility(self, df, window=20):
        """Calcula la volatilidad usando la desviación estándar"""
        return df['close'].rolling(window=window).std()

    def calculate_moving_averages(self, df):
        """Calcula medias móviles para identificar tendencias"""
        df['SMA_20'] = df['close'].rolling(window=20).mean()
        df['EMA_20'] = df['close'].ewm(span=20, adjust=False).mean()
        return df

    def calculate_rsi(self, df, periods=14):
        """Calcula el RSI para detectar sobrecompra/sobreventa"""
        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=periods).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=periods).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        return rsi

    def calculate_returns(self, df):
        """Calcula diferentes métricas de rendimiento"""
        df['daily_return'] = df['close'].pct_change()
        df['cumulative_return'] = (1 + df['daily_return']).cumprod()
        return df    
    def calculate_momentum(self, df, period=14):
        """Calcula el momentum para medir la fuerza del movimiento"""
        return df['close'].diff(period)
        
    def enrich_data(self, data):
        """
        Enriquece los datos con varios KPIs:
        - Volatilidad
        - Tendencias (SMA, EMA)
        - RSI (sobrecompra/sobreventa)
        - Rendimientos
        - Momentum
        """
        try:
            # Convertir los datos a DataFrame
            df = pd.DataFrame(data)
            df.set_index('datetime', inplace=True)
            
            # Asegurarse de que los datos estén ordenados cronológicamente
            df = df.sort_index()
            
            # 1. Volatilidad del precio
            df['volatility'] = self.calculate_volatility(df)
            
            # 2. Tendencias del mercado (Medias móviles)
            df = self.calculate_moving_averages(df)
            
            # 3. Detección de sobrecompra/sobreventa
            df['RSI'] = self.calculate_rsi(df)
            
            # 4. Rendimiento y riesgo
            df = self.calculate_returns(df)
            
            # 5. Fuerza del movimiento
            df['momentum'] = self.calculate_momentum(df)
            
            # Redondear KPIs a 4 decimales
            kpi_cols = ['volatility', 'SMA_20', 'EMA_20', 'RSI', 'daily_return', 'cumulative_return', 'momentum']
            for col in kpi_cols:
                if col in df.columns:
                    df[col] = df[col].round(4)
        
            # Llenar valores NaN con 0
            df = df.fillna(0)
            
            self.logger.info('Datos enriquecidos exitosamente con KPIs')
            
            # Devolver el DataFrame como diccionario incluyendo el índice
            return df.reset_index().to_dict('records')
            
        except Exception as e:
            self.logger.error(f'Error al enriquecer los datos: {str(e)}')
            return data

  

    # def calcular_kpi(self,df=pd.DataFrame()): #mínimo 5 KPI (tasa de variación, media móvil, volatilidad, retorno acumulado, desviación estándar, etc.).
    #     try:
    #       df = df.copy()
    #       # ordenar
    #       df = df.sort_values('fecha')
    #       for col in df.columns:
    #          if col != "fecha":
    #             df[col]= pd.to_numeric(df[col].str.replace(',', '.',regex=False),errors='coerce')
    #       # 1er indicador volatilidad (desviacion de los ult. 5 dias)
    #       df['volatilidad']=df['cerrar'].rolling(window=5).std().fillna(0)
    #       self.logger.info('Enricher','calcular_kpi','agregar indicadores KPI')
    #       return df
    #     except Exception as errores:
    #       self.logger.error(f'Enricher','calcular_kpi','Error al enriquecer el df{errores}')
    #       df=pd.DataFrame()
    #       return df