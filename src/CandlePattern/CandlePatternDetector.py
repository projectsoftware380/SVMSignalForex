import psycopg2
import pandas as pd
import talib
import logging
import os
from datetime import datetime
import pytz

# Configuración básica de logging
logging.basicConfig(
    filename=os.path.join(os.path.dirname(__file__), '..', 'logs', 'candle_pattern_analyzer.log'),
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class CandlePatternAnalyzer:
    def __init__(self, db_config):
        self.db_config = db_config

    def obtener_datos_bd(self, symbol, timeframe):
        """
        Obtiene los datos desde la base de datos correspondientes al símbolo y la temporalidad.
        """
        try:
            # Conectarse a la base de datos
            connection = psycopg2.connect(**self.db_config)
            cursor = connection.cursor()

            # Seleccionar la tabla en función de la temporalidad
            if timeframe == '3m':
                table = 'forex_data_3m'
            elif timeframe == '15m':
                table = 'forex_data_15m'
            elif timeframe == '4h':
                table = 'forex_data_4h'
            else:
                raise ValueError(f"Temporalidad {timeframe} no válida")

            # Consulta SQL para obtener los datos
            query = f"""
            SELECT timestamp, open, high, low, close, volume
            FROM {table}
            WHERE pair = %s
            ORDER BY timestamp DESC
            LIMIT 100;
            """
            cursor.execute(query, (symbol,))
            rows = cursor.fetchall()

            # Convertir los resultados a DataFrame
            df = pd.DataFrame(rows, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df.set_index('timestamp', inplace=True)

            logging.info(f"Datos obtenidos correctamente desde la base de datos para {symbol} en {timeframe}: {df.shape[0]} filas.")
            return df[['open', 'high', 'low', 'close', 'volume']]
        except Exception as e:
            logging.error(f"Error al obtener datos de la base de datos para {symbol} en {timeframe}: {e}")
            return pd.DataFrame()
        finally:
            if connection:
                cursor.close()
                connection.close()

    def detectar_patrones_talib(self, df):
        """
        Detecta patrones de velas japonesas utilizando la librería TA-Lib.
        Devuelve un diccionario con los patrones detectados.
        """
        patrones = {}
        patrones_alcistas = ['CDLHAMMER', 'CDLINVERTEDHAMMER', 'CDLPIERCING', 'CDLMORNINGSTAR', 'CDL3WHITESOLDIERS']
        patrones_bajistas = ['CDLSHOOTINGSTAR', 'CDLENGULFING', 'CDLDARKCLOUDCOVER', 'CDLEVENINGSTAR', 'CDL3BLACKCROWS']

        try:
            # TA-Lib pattern recognition
            for pattern in talib.get_function_groups()['Pattern Recognition']:
                result = getattr(talib, pattern)(df['open'], df['high'], df['low'], df['close'])

                # Penúltimo valor indica el patrón detectado (positivo para alcista, negativo para bajista)
                if result.iloc[0] > 0 and pattern in patrones_alcistas:
                    patrones[pattern] = 'alcista'
                elif result.iloc[0] < 0 and pattern in patrones_bajistas:
                    patrones[pattern] = 'bajista'

            if patrones:
                logging.info(f"Patrones detectados: {patrones}")
            else:
                logging.info(f"No se detectaron patrones.")
        except Exception as e:
            logging.error(f"Error al detectar patrones con TA-Lib: {e}")

        return patrones

    def detectar_patrones_para_par(self, symbol):
        """
        Detecta patrones de velas japonesas para un par de divisas específico en 4h, 15m y 3m.
        """
        logging.info(f"Iniciando la detección de patrones para {symbol}.")

        patrones_resultantes = {}

        # Detección en diferentes temporalidades
        temporalidades = ['4h', '15m', '3m']

        for temporalidad in temporalidades:
            df = self.obtener_datos_bd(symbol, temporalidad)
            if not df.empty:
                patrones_detectados = self.detectar_patrones_talib(df)
                if patrones_detectados:
                    patrones_resultantes[temporalidad] = patrones_detectados

        return patrones_resultantes
