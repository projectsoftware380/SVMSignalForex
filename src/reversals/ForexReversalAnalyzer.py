import psycopg2
import pandas as pd
import talib as ta
from datetime import datetime, timedelta, timezone
import pytz
import logging
import os
import json
import threading
import time

# Verificar si el directorio 'logs' existe, si no, crearlo
logs_directory = os.path.join(os.path.dirname(__file__), '..', 'logs')
if not os.path.exists(logs_directory):
    os.makedirs(logs_directory)

# Configurar logging
logging.basicConfig(
    filename=os.path.join(logs_directory, 'reversal_server.log'),
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    encoding='utf-8'
)
logger = logging.getLogger(__name__)

# Cargar configuración desde src/config/config.json
CONFIG_FILE = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.json')
with open(CONFIG_FILE, "r", encoding='utf-8') as f:
    config = json.load(f)

class ForexReversalAnalyzer:
    def __init__(self, db_config):
        self.db_config = db_config

    def obtener_datos_bd(self, symbol, horas=100):
        """Obtiene los datos de la base de datos y los prepara para el análisis."""
        try:
            logger.debug(f"Obteniendo datos para {symbol} durante las últimas {horas} horas.")
            
            # Establecer conexión con la base de datos usando los datos de config.json
            connection = psycopg2.connect(**self.db_config)
            cursor = connection.cursor()

            # Consulta SQL para obtener datos ordenados en orden ascendente
            query = f"""
            SELECT timestamp, open, high, low, close, volume
            FROM forex_data_15m
            WHERE pair = %s
            AND timestamp >= NOW() - INTERVAL '{horas} HOURS'
            ORDER BY timestamp ASC;
            """
            cursor.execute(query, (symbol,))
            rows = cursor.fetchall()

            # Convertir a DataFrame
            df = pd.DataFrame(rows, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df.set_index('timestamp', inplace=True)

            if df.empty:
                logger.warning(f"No se encontraron resultados en la base de datos para {symbol}.")
                return pd.DataFrame()

            # Convertir los valores a float para asegurar compatibilidad con TA-Lib
            cols_to_convert = ['open', 'high', 'low', 'close', 'volume']
            df[cols_to_convert] = df[cols_to_convert].apply(pd.to_numeric, errors='coerce')

            # Eliminar filas con valores NaN
            df.dropna(subset=cols_to_convert, inplace=True)

            # Asegurar timestamps uniformes cada 15 minutos
            all_times = pd.date_range(start=df.index[0], end=df.index[-1], freq='15min')  
            df = df.reindex(all_times)

            # Interpolar los valores faltantes
            df[cols_to_convert] = df[cols_to_convert].interpolate(method='time')

            # Eliminar filas con valores NaN restantes después de la interpolación
            df.dropna(subset=cols_to_convert, inplace=True)

            # Verificar que el DataFrame tiene suficientes filas
            if len(df) < 22:
                logger.error(f"Datos insuficientes para {symbol}. Se requieren al menos 22 registros.")
                return pd.DataFrame()

            logger.debug(f"Datos obtenidos para {symbol}: {df.tail()}")
            return df
        except Exception as e:
            logger.error(f"Error al obtener datos de la base de datos para {symbol}: {e}", exc_info=True)
            return pd.DataFrame()
        finally:
            if 'connection' in locals() and connection:
                cursor.close()
                connection.close()

    def calcular_indicadores(self, df):
        """Calcula las Bandas de Bollinger y niveles de Fibonacci."""
        try:
            # Calcular Bandas de Bollinger
            df['upper'], df['mid'], df['lower'] = ta.BBANDS(df['close'], timeperiod=20)
            logger.debug(f"Bandas de Bollinger calculadas:\nUpper: {df['upper'].iloc[-1]}, Mid: {df['mid'].iloc[-1]}, Lower: {df['lower'].iloc[-1]}")

            # Eliminar filas con NaN en las Bandas de Bollinger
            df.dropna(subset=['upper', 'mid', 'lower'], inplace=True)

            # Calcular niveles de Fibonacci
            df_recent = df.tail(20)
            max_price = df_recent['high'].max()
            min_price = df_recent['low'].min()
            rango = max_price - min_price

            logger.debug(f"Rango de precios para Fibonacci:\nMax: {max_price}, Min: {min_price}, Rango: {rango}")

            # Evitar división por cero
            if rango == 0:
                logger.error("El rango de precios es cero al calcular Fibonacci.")
                return None

            fibonacci_levels = {
                '23.6%': max_price - 0.236 * rango,
                '38.2%': max_price - 0.382 * rango,
                '76.8%': max_price - 0.768 * rango
            }

            logger.debug(f"Niveles de Fibonacci calculados: {fibonacci_levels}")
            return fibonacci_levels
        except Exception as e:
            logger.error(f"Error al calcular indicadores: {e}", exc_info=True)
            return None

    def detectar_reversion(self, df, fibonacci_levels):
        """Detecta si hay una reversión en el penúltimo período."""
        try:
            # Usar la penúltima vela para análisis
            precio_close = df['close'].iloc[-2]
            banda_media = df['mid'].iloc[-2]

            logger.debug(f"Analizando reversión en la penúltima vela:\nPrecio Close: {precio_close}, Banda Media: {banda_media}")

            # Reversión Alcista
            if precio_close < fibonacci_levels['23.6%'] and precio_close > min(banda_media, fibonacci_levels['76.8%']):
                logger.info("Reversión Alcista detectada.")
                return "Reversión Alcista"

            # Reversión Bajista
            if precio_close > fibonacci_levels['23.6%'] and precio_close < max(banda_media, fibonacci_levels['76.8%']):
                logger.info("Reversión Bajista detectada.")
                return "Reversión Bajista"

            logger.info("Sin reversión detectada.")
            return "Sin Reversión"
        except Exception as e:
            logger.error(f"Error al detectar reversión: {e}", exc_info=True)
            return "Sin Reversión"

    def analizar_reversion_para_par(self, symbol):
        """Analiza reversión para un par específico."""
        logger.info(f"Analizando reversión para {symbol}")
        df = self.obtener_datos_bd(symbol)
        if df.empty:
            return None

        fibonacci_levels = self.calcular_indicadores(df)
        if fibonacci_levels is None:
            return None

        return self.detectar_reversion(df, fibonacci_levels)

    def tiempo_para_proxima_vela(self):
        """Calcula el tiempo restante hasta la próxima vela de 15 minutos."""
        ahora = datetime.now(timezone.utc)
        minutos_para_15 = 15 - (ahora.minute % 15)
        proxima_vela = ahora + timedelta(minutes=minutos_para_15, seconds=-ahora.second, microseconds=-ahora.microsecond)
        return (proxima_vela - ahora).total_seconds()

    def ejecutar_analisis_cuando_nueva_vela(self):
        """Ejecuta el análisis al inicio de cada nueva vela de 15 minutos."""
        while True:
            try:
                tiempo_restante = self.tiempo_para_proxima_vela()
                logger.info(f"Esperando {tiempo_restante} segundos para la próxima vela.")
                time.sleep(tiempo_restante)
                logger.info("Iniciando análisis de reversión.")
                self.analizar_reversiones()
            except Exception as e:
                logger.error(f"Error en el bucle principal: {e}", exc_info=True)
                time.sleep(60)

def iniciar_hilo_analisis():
    reversal_analyzer = ForexReversalAnalyzer(db_config=config['db_config'])
    hilo_analisis = threading.Thread(target=reversal_analyzer.ejecutar_analisis_cuando_nueva_vela)
    hilo_analisis.daemon = True
    hilo_analisis.start()

if __name__ == "__main__":
    iniciar_hilo_analisis()
    while True:
        time.sleep(1)
