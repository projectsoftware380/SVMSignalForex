import psycopg2
import pandas as pd
import talib as ta
from datetime import datetime, timedelta, timezone
import logging
import os
import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configuración del logger
logs_directory = os.path.join(os.path.dirname(__file__), '..', 'logs')
os.makedirs(logs_directory, exist_ok=True)

logging.basicConfig(
    filename=os.path.join(logs_directory, 'reversal_server.log'),
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    encoding='utf-8'
)
logger = logging.getLogger(__name__)

# Cargar configuración desde config.json
CONFIG_FILE = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.json')
with open(CONFIG_FILE, "r", encoding='utf-8') as f:
    config = json.load(f)

class ForexReversalAnalyzer:
    def __init__(self, db_config):
        self.db_config = db_config

    def obtener_conexion(self):
        """Obtiene la conexión a la base de datos."""
        try:
            return psycopg2.connect(**self.db_config)
        except Exception as e:
            logger.error(f"Error al conectar a la base de datos: {e}", exc_info=True)
            return None

    def registrar_reversion(self, pair, tipo_reversion, precio_actual, timestamp):
        """Registra la reversión en la base de datos y agrega logs detallados."""
        query = """
            INSERT INTO reversiones (timestamp, par_de_divisas, tipo_reversion, origen, precio_actual)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (timestamp, par_de_divisas)
            DO UPDATE SET tipo_reversion = EXCLUDED.tipo_reversion,
                          precio_actual = EXCLUDED.precio_actual;
        """
        try:
            connection = self.obtener_conexion()
            if connection is None:
                raise ValueError("Conexión a la base de datos fallida.")
            with connection.cursor() as cursor:
                cursor.execute(query, (
                    timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                    pair, tipo_reversion, 'ForexReversalAnalyzer', float(precio_actual)
                ))
                connection.commit()
                logger.info(f"Reversión registrada: {pair} -> '{tipo_reversion}' at {timestamp} "
                            f"con precio actual: {precio_actual}")
        except Exception as e:
            logger.error(f"Error al registrar reversión para {pair}: {e}", exc_info=True)
        finally:
            if connection:
                connection.close()

    def obtener_datos_bd(self, symbol, horas=100):
        """Obtiene los datos recientes de la base de datos."""
        query = """
            SELECT timestamp, open, high, low, close, volume
            FROM forex_data_15m
            WHERE pair = %s AND timestamp >= NOW() - INTERVAL %s
            ORDER BY timestamp ASC;
        """
        try:
            connection = self.obtener_conexion()
            if connection is None:
                raise ValueError("Conexión a la base de datos fallida.")
            with connection.cursor() as cursor:
                cursor.execute(query, (symbol, f'{horas} HOURS'))
                rows = cursor.fetchall()
            df = pd.DataFrame(rows, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df.set_index('timestamp', inplace=True)
            logger.debug(f"Datos obtenidos para {symbol}: {df.tail(2).to_dict()}")
            return df
        except Exception as e:
            logger.error(f"Error al obtener datos para {symbol}: {e}", exc_info=True)
            return pd.DataFrame()

    def calcular_indicadores(self, df):
        """Calcula los indicadores técnicos y registra logs de los valores."""
        try:
            df['upper'], df['mid'], df['lower'] = ta.BBANDS(df['close'], timeperiod=20)
            df.dropna(inplace=True)
            max_price = float(df['high'].max())
            min_price = float(df['low'].min())
            rango = max_price - min_price
            niveles_fibonacci = {
                '23.6%': max_price - 0.236 * rango,
                '38.2%': max_price - 0.382 * rango,
                '76.8%': max_price - 0.768 * rango
            }
            logger.debug(f"Niveles de Fibonacci calculados: {niveles_fibonacci}")
            return niveles_fibonacci
        except Exception as e:
            logger.error(f"Error al calcular indicadores: {e}", exc_info=True)
            return None

    def detectar_reversion(self, df, fibonacci_levels):
        """Detecta si hay una reversión con los niveles de Fibonacci."""
        try:
            close_price = df['close'].iloc[-2]
            mid_band = df['mid'].iloc[-2]
            logger.debug(f"Precio de cierre: {close_price}, Banda media: {mid_band}")
            if close_price < fibonacci_levels['23.6%'] and close_price > min(mid_band, fibonacci_levels['76.8%']):
                return 'alcista'
            elif close_price > fibonacci_levels['23.6%'] and close_price < max(mid_band, fibonacci_levels['76.8%']):
                return 'bajista'
            return 'neutral'
        except Exception as e:
            logger.error(f"Error al detectar reversión: {e}", exc_info=True)
            return 'neutral'

    def analizar_reversion_para_par(self, symbol):
        """Analiza la reversión para un par específico."""
        try:
            df = self.obtener_datos_bd(symbol)
            if df.empty:
                logger.warning(f"No hay datos suficientes para {symbol}.")
                return False

            fibonacci_levels = self.calcular_indicadores(df)
            if fibonacci_levels is None:
                logger.warning(f"No se pudieron calcular los indicadores para {symbol}.")
                return False

            reversion = self.detectar_reversion(df, fibonacci_levels)
            if reversion != 'neutral':
                timestamp = datetime.now(timezone.utc)
                precio_actual = df['close'].iloc[-2]
                self.registrar_reversion(symbol, reversion, precio_actual, timestamp)
                return True
            else:
                logger.info(f"Reversión neutral detectada para {symbol}. No se registra en la base de datos.")
                return False
        except Exception as e:
            logger.error(f"Error al analizar reversión para {symbol}: {e}", exc_info=True)
            return False

    def ejecutar_analisis_cuando_nueva_vela(self):
        """Ejecuta el análisis cuando comienza una nueva vela."""
        while True:
            tiempo_restante = self.tiempo_para_proxima_vela()
            logger.info(f"Esperando {tiempo_restante} segundos para la próxima vela.")
            time.sleep(tiempo_restante)
            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = {executor.submit(self.analizar_reversion_para_par, pair): pair for pair in config['pairs']}
                for future in as_completed(futures):
                    pair = futures[future]
                    try:
                        resultado = future.result()
                        if resultado:
                            logger.info(f"Análisis para {pair} completado con éxito.")
                        else:
                            logger.warning(f"Análisis para {pair} no completado.")
                    except Exception as e:
                        logger.error(f"Error en el análisis para {pair}: {e}", exc_info=True)

def iniciar_hilo_analisis():
    """Inicia el hilo para el análisis periódico."""
    analyzer = ForexReversalAnalyzer(config['db_config'])
    hilo = threading.Thread(target=analyzer.ejecutar_analisis_cuando_nueva_vela)
    hilo.daemon = True
    hilo.start()

if __name__ == "__main__":
    iniciar_hilo_analisis()
    while True:
        time.sleep(1)
