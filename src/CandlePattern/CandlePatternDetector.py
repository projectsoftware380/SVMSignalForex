import psycopg2
import pandas as pd
import talib as ta
import logging
import threading
import time
from datetime import datetime, timezone
import pytz
import os

# Configuración del logger
log_dir = os.path.join(os.path.dirname(__file__), '..', 'logs')
os.makedirs(log_dir, exist_ok=True)

logging.basicConfig(
    filename=os.path.join(log_dir, 'candle_pattern_server.log'),
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class CandlePatternAnalyzer:
    def __init__(self, db_config, pairs):
        self.db_config = db_config
        self.pairs = pairs
        self.lock = threading.Lock()
        self.pattern_functions = {
            'CDLDOJI': ta.CDLDOJI,
            'CDLENGULFING': ta.CDLENGULFING,
            'CDLHAMMER': ta.CDLHAMMER,
            'CDLSHOOTINGSTAR': ta.CDLSHOOTINGSTAR,
        }

    def obtener_conexion(self):
        """Establece y retorna una conexión con la base de datos."""
        try:
            return psycopg2.connect(**self.db_config)
        except Exception as e:
            logger.error(f"Error al conectar a la base de datos: {e}", exc_info=True)
            return None

    def obtener_datos_bd(self, symbol, timeframe):
        """Obtiene los datos más recientes para un par y timeframe desde la base de datos."""
        query = f"""
            SELECT timestamp, open, high, low, close, volume
            FROM forex_data_{timeframe}
            WHERE pair = %s
            ORDER BY timestamp DESC
            LIMIT 100;
        """
        try:
            connection = self.obtener_conexion()
            if connection is None:
                return pd.DataFrame()

            with connection.cursor() as cursor:
                cursor.execute(query, (symbol,))
                rows = cursor.fetchall()

            df = pd.DataFrame(rows, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df.set_index('timestamp', inplace=True)
            return df.sort_index()
        except Exception as e:
            logger.error(f"Error al obtener datos para {symbol}: {e}", exc_info=True)
            return pd.DataFrame()
        finally:
            if connection:
                connection.close()

    def obtener_datos_por_timestamp(self, symbol, timeframe, timestamp):
        """Obtiene datos exactos de una vela con un timestamp dado."""
        query = f"""
            SELECT timestamp, open, high, low, close, volume
            FROM forex_data_{timeframe}
            WHERE pair = %s AND timestamp = %s;
        """
        try:
            connection = self.obtener_conexion()
            if connection is None:
                return None

            with connection.cursor() as cursor:
                cursor.execute(query, (symbol, timestamp))
                row = cursor.fetchone()

            if row:
                logger.info(f"Datos obtenidos para {symbol} en {timestamp}: {row}")
                return {
                    "timestamp": row[0],
                    "open": float(row[1]),
                    "high": float(row[2]),
                    "low": float(row[3]),
                    "close": float(row[4]),
                    "volume": row[5],
                }
            else:
                logger.error(f"No se encontraron datos para {symbol} en {timeframe} con timestamp {timestamp}.")
                return None
        except Exception as e:
            logger.error(f"Error al obtener datos: {e}", exc_info=True)
            return None
        finally:
            if connection:
                connection.close()

    def detectar_patrones_para_par(self, symbol, timeframe):
        """Detecta patrones de velas para un par y timeframe específicos."""
        df = self.obtener_datos_bd(symbol, timeframe)

        if df.empty:
            logger.error(f"No se encontraron datos para {symbol} en {timeframe}.")
            return

        timestamp_detectado = df.index[-1].strftime('%Y-%m-%d %H:%M:%S')

        for pattern_name, pattern_function in self.pattern_functions.items():
            result = pattern_function(df['open'], df['high'], df['low'], df['close'])

            if result.iloc[-1] != 0:
                tipo = 'alcista' if result.iloc[-1] > 0 else 'bajista'
            else:
                tipo = 'neutral'

            datos_vela = self.obtener_datos_por_timestamp(symbol, timeframe, timestamp_detectado)

            if datos_vela and datos_vela['close'] is not None:
                price_signal = float(datos_vela['close'])
                self.registrar_patron(symbol, pattern_name, tipo, timeframe, timestamp_detectado, price_signal)
            else:
                logger.error(f"No se pudo obtener el precio de cierre para {symbol} en {timestamp_detectado}.")

    def registrar_patron(self, symbol, pattern, tipo, timeframe, timestamp, price_signal):
        """Registra un patrón en la base de datos o actualiza el price_signal si ya existe."""
        query = """
            INSERT INTO patrones_velas (timestamp, par_de_divisas, patron, tipo, timeframe, price_signal)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (par_de_divisas, patron, timeframe) 
            DO UPDATE SET price_signal = EXCLUDED.price_signal,
                          timestamp = EXCLUDED.timestamp,
                          tipo = EXCLUDED.tipo;
        """
        try:
            connection = self.obtener_conexion()
            if connection is None:
                return

            with connection.cursor() as cursor:
                cursor.execute(query, (timestamp, symbol, pattern, tipo, timeframe, price_signal))
            connection.commit()
            logger.info(f"Patrón {pattern} registrado o actualizado para {symbol} en {timeframe} con tipo '{tipo}'.")
        except Exception as e:
            logger.error(f"Error al registrar patrón: {e}", exc_info=True)
        finally:
            if connection:
                connection.close()

    def analizar_patrones(self):
        """Analiza patrones para todos los pares y guarda el resultado en la base de datos."""
        for symbol in self.pairs:
            for timeframe in ['3m', '15m', '4h']:
                self.detectar_patrones_para_par(symbol, timeframe)
        logger.info(f"Análisis de patrones completado a las {self.obtener_hora_colombia()}.")

    def obtener_hora_colombia(self):
        """Obtiene la hora actual en Colombia."""
        zona_colombia = pytz.timezone('America/Bogota')
        return datetime.now(zona_colombia).strftime('%Y-%m-%d %H:%M:%S')

    def tiempo_para_proxima_vela(self):
        """Calcula el tiempo restante para la próxima vela de 3 minutos."""
        ahora = datetime.now(timezone.utc)
        segundos_restantes = (180 - (ahora.minute % 3) * 60 - ahora.second) % 180
        return segundos_restantes

    def ejecutar_analisis_cuando_nueva_vela(self):
        """Ejecuta el análisis periódico."""
        while True:
            tiempo_restante = self.tiempo_para_proxima_vela()
            logger.info(f"Esperando {tiempo_restante} segundos para la próxima vela.")
            time.sleep(tiempo_restante)
            self.analizar_patrones()
