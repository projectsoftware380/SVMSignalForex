import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from datetime import datetime, timedelta, timezone
import pytz
import threading
import logging
import os
import json
from concurrent.futures import ThreadPoolExecutor
import unicodedata

# Configurar un logger específico para este módulo
logger = logging.getLogger(__name__)

class ForexAnalyzer:
    def __init__(self, db_config, pairs):
        """
        Inicializa el analizador Forex con la configuración de la base de datos y la lista de pares.
        """
        self.db_config = db_config  # Configuración de la base de datos
        self.pairs = pairs  # Lista de pares de divisas para analizar
        self.last_trend = {}  # Almacena las tendencias de cada par
        self.lock = threading.Lock()  # Proteger el acceso a recursos compartidos
        
        # Crear la conexión a la base de datos PostgreSQL
        self.engine = create_engine(
            f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}/{db_config['database']}"
        )

    def normalizar_string(self, valor):
        """Normaliza un string eliminando caracteres especiales."""
        if isinstance(valor, str):
            return unicodedata.normalize('NFKD', valor).encode('ascii', 'ignore').decode('ascii')
        return valor

    def obtener_hora_colombia(self):
        """Obtiene la hora actual en la zona horaria de Colombia."""
        zona_colombia = pytz.timezone('America/Bogota')
        hora_actual_colombia = datetime.now(zona_colombia)
        return hora_actual_colombia.strftime('%Y-%m-%d %H:%M:%S')

    def obtener_datos_postgresql(self, pair, registros=156):
        """
        Obtiene los datos más recientes de la base de datos PostgreSQL para un par de divisas específico.
        """
        query = f"""
        SELECT timestamp, open, high, low, close
        FROM forex_data_4h
        WHERE pair = '{pair}'
        ORDER BY timestamp DESC
        LIMIT {registros};
        """
        
        try:
            # Ejecutar la consulta
            df = pd.read_sql(query, self.engine)
            df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
            df.set_index('timestamp', inplace=True)
            
            # Ordenar en orden ascendente para asegurar que los datos estén en la secuencia correcta
            df = df.sort_index()
            
            # Verificar si los datos cubren un período reciente
            if not df.empty:
                ultimo_timestamp = df.index[-1]
                fecha_actual = datetime.now(timezone.utc)
                diferencia = fecha_actual - ultimo_timestamp

                # Si la diferencia es mayor a un cierto umbral (por ejemplo, 4 horas), los datos pueden estar desactualizados
                if diferencia > timedelta(hours=4):
                    logger.warning(f"Los datos para {pair} podrían estar desactualizados. Último timestamp: {ultimo_timestamp}")
                else:
                    logger.info(f"Datos obtenidos para {pair} están actualizados. Último timestamp: {ultimo_timestamp}")
                
            return df
        except Exception as e:
            logger.error(f"Error al obtener datos para {pair} desde la base de datos: {str(e)}")
            return pd.DataFrame()

    def obtener_datos_validos(self, pair):
        """
        Obtiene los datos de mercado válidos para un par de divisas, asegurando que haya suficientes registros para el cálculo de Ichimoku.
        """
        df = self.obtener_datos_postgresql(pair)

        logger.info(f"Número de registros obtenidos para {pair}: {len(df)}")

        if df.empty or len(df) < 156:
            logger.warning(f"Datos insuficientes para {pair}. Se requieren al menos 156 registros.")
            return pd.DataFrame(), None

        df = df.tail(156)  # Tomar solo los últimos 156 registros
        ultimo_close = df[['close']].iloc[-1]  # Último valor de cierre
        logger.info(f"Último valor de cierre para {pair}: {ultimo_close['close']}")
        return df, ultimo_close

    def calcular_ichimoku(self, df):
        """
        Calcula los componentes del indicador Ichimoku.
        """
        if len(df) < 156:
            logger.warning("Datos insuficientes para calcular Ichimoku. Se requieren 156 períodos.")
            return pd.DataFrame()

        df['Tenkan-sen'] = (df['high'].rolling(window=9).max() + df['low'].rolling(window=9).min()) / 2
        df['Kijun-sen'] = (df['high'].rolling(window=26).max() + df['low'].rolling(window=26).min()) / 2
        df['Senkou Span A'] = ((df['Tenkan-sen'] + df['Kijun-sen']) / 2).shift(26)
        df['Senkou Span B'] = ((df['high'].rolling(window=52).max() + df['low'].rolling(window=52).min()) / 2).shift(26)
        df['Chikou Span'] = df['close'].shift(-26)
        
        df_dropna = df.dropna()

        if df_dropna.empty:
            logger.warning("No hay suficientes datos después de calcular Ichimoku (NaNs presentes).")
            return df

        ultimo_valor = df_dropna.iloc[-1]
        logger.info(f"Últimos valores de Ichimoku:\nTenkan-sen: {ultimo_valor['Tenkan-sen']}, "
                    f"Kijun-sen: {ultimo_valor['Kijun-sen']}, Senkou Span A: {ultimo_valor['Senkou Span A']}, "
                    f"Senkou Span B: {ultimo_valor['Senkou Span B']}, Chikou Span: {ultimo_valor['Chikou Span']}")
        return df

    def analizar_par(self, pair):
        """
        Analiza un par de divisas usando el indicador Ichimoku y determina la tendencia.
        """
        try:
            logger.info(f"Analizando par: {pair}")
            df, ultimo_close = self.obtener_datos_validos(pair)

            if df.empty:
                with self.lock:
                    self.last_trend[pair] = "Datos insuficientes"
                logger.warning(f"Datos insuficientes para {pair}")
                return "Datos insuficientes"

            df = self.calcular_ichimoku(df)
            if df.empty:
                with self.lock:
                    self.last_trend[pair] = "Datos insuficientes"
                logger.warning(f"Datos insuficientes después de calcular Ichimoku para {pair}")
                return "Datos insuficientes"

            ultimo_valor = df.dropna().iloc[-1]
            if ultimo_valor['Tenkan-sen'] > ultimo_valor['Kijun-sen'] and ultimo_close['close'] > ultimo_valor['Senkou Span A']:
                tendencia = "Tendencia Alcista"
            elif ultimo_valor['Tenkan-sen'] < ultimo_valor['Kijun-sen'] and ultimo_close['close'] < ultimo_valor['Senkou Span B']:
                tendencia = "Tendencia Bajista"
            else:
                tendencia = "Neutral"

            # Normalizar la tendencia antes de guardarla
            tendencia_normalizada = self.normalizar_string(tendencia)

            logger.info(f"Tendencia determinada para {pair}: {tendencia_normalizada}. Último precio de cierre: {ultimo_close['close']}")
            with self.lock:
                self.last_trend[pair] = tendencia_normalizada
            return tendencia_normalizada
        except Exception as e:
            logger.error(f"Error al analizar par {pair}: {str(e)}")
            with self.lock:
                self.last_trend[pair] = "Error"
            return "Error"

    def analizar_pares(self):
        """
        Analiza todos los pares de divisas utilizando ThreadPoolExecutor para paralelizar el análisis.
        """
        logger.info("Iniciando análisis de todos los pares.")
        with ThreadPoolExecutor(max_workers=5) as executor:
            executor.map(self.analizar_par, self.pairs)
        logger.info("Análisis de todos los pares completado.")
        with self.lock:
            return self.last_trend.copy()
