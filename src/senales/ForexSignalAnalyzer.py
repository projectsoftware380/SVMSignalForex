import os
import logging
import json
import threading
import pandas as pd
import pandas_ta as ta
from datetime import datetime, timezone, timedelta
import psycopg2

# Configuración del logger
log_dir = os.path.join(os.path.dirname(__file__), '..', 'logs')
os.makedirs(log_dir, exist_ok=True)

logger = logging.getLogger('ForexSignalAnalyzer')
logger.setLevel(logging.INFO)

file_handler = logging.FileHandler(os.path.join(log_dir, 'forex_signal_server.log'), encoding='utf-8')
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(file_handler)

class ForexSignalAnalyzer:
    def __init__(self, db_config):
        """Inicializa la clase con la configuración de la base de datos."""
        self.db_config = db_config
        self.lock = threading.Lock()

        config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.json')
        with open(config_path, "r", encoding='utf-8') as f:
            self.config = json.load(f)

        self.pairs = self.config.get("pairs", [])
        self.analysis_mode = self.config.get("analysis_mode", "estado")
        logger.info("ForexSignalAnalyzer inicializado correctamente con los pares: %s", self.pairs)

    def obtener_conexion(self):
        """Establece y verifica la conexión con la base de datos."""
        try:
            connection = psycopg2.connect(**self.db_config)
            logger.info("Conexión a la base de datos establecida correctamente.")
            return connection
        except Exception as e:
            logger.error("Error al conectar a la base de datos: %s", e, exc_info=True)
            return None

    def obtener_datos_bd(self, symbol):
        """Obtiene los datos más recientes de la base de datos para un par de divisas."""
        query = """
            SELECT timestamp AT TIME ZONE 'America/Guatemala' AS timestamp_local, 
                   open, high, low, close, volume
            FROM forex_data_3m
            WHERE pair = %s
            ORDER BY timestamp DESC
            LIMIT 20;
        """
        try:
            connection = self.obtener_conexion()
            if not connection:
                raise ConnectionError("No se pudo conectar a la base de datos.")

            with connection.cursor() as cursor:
                cursor.execute(query, (symbol,))
                rows = cursor.fetchall()

            if rows:
                df = pd.DataFrame(rows, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])

                # Convertir timestamps a UTC
                df['timestamp'] = pd.to_datetime(df['timestamp']).dt.tz_convert('UTC')
                return df

            logger.warning("No se encontraron datos para %s.", symbol)
            return None

        except Exception as e:
            logger.error("Error al obtener datos para %s: %s", symbol, e, exc_info=True)
            return None
        finally:
            if connection:
                connection.close()

    def tiempo_para_proxima_vela(self):
        """Calcula el tiempo restante para la próxima vela."""
        ahora = datetime.now(timezone.utc)
        siguiente_minuto = (ahora + timedelta(minutes=1)).replace(second=0, microsecond=0)
        tiempo_restante = (siguiente_minuto - ahora).total_seconds()
        logger.info("Tiempo restante para la próxima vela: %.2f segundos", tiempo_restante)
        return tiempo_restante

    def analizar_senales(self):
        """Analiza señales para todos los pares configurados."""
        resultados = {}
        for pair in self.pairs:
            df = self.obtener_datos_bd(pair)
            if df is None or df.empty:
                logger.warning("No se obtuvieron datos para %s.", pair)
                continue

            precio_actual = df['close'].iloc[-1]
            timestamp = df['timestamp'].iloc[-1]
            logger.info("Precio actual obtenido desde la base de datos: %.5f", precio_actual)

            signal = self.generar_senal(df)
            if signal:
                self.registrar_senal(pair, signal, precio_actual, timestamp)
                resultados[pair] = signal

        logger.info("Señales analizadas: %s", resultados)
        return resultados

    def generar_senal(self, df):
        """Genera una señal basada en el modo seleccionado."""
        try:
            # Validar si hay suficientes datos para calcular el Supertrend
            if len(df) < 11:
                logger.warning("Datos insuficientes para calcular Supertrend.")
                return None

            df[['high', 'low', 'close']] = df[['high', 'low', 'close']].astype(float)

            # Rellenar valores NaN si existen
            df = df.fillna(method='ffill').fillna(method='bfill')

            # Calcular el Supertrend
            supertrend = ta.supertrend(df['high'], df['low'], df['close'], length=10, multiplier=3)

            if supertrend is None or supertrend.empty:
                logger.error("Supertrend no se pudo calcular o devolvió None.")
                return None

            df = df.join(supertrend)

            logger.info("Precio actual: %.5f, Supertrend: %.5f",
                        df['close'].iloc[-1], df['SUPERT_10_3.0'].iloc[-1])

            # Lógica de generación de señales
            if self.analysis_mode == "estado":
                if df['close'].iloc[-1] > df['SUPERT_10_3.0'].iloc[-1]:
                    return 'alcista'
                elif df['close'].iloc[-1] < df['SUPERT_10_3.0'].iloc[-1]:
                    return 'bajista'
                return 'neutral'

            elif self.analysis_mode == "cruce":
                if (df['close'].iloc[-2] <= df['SUPERT_10_3.0'].iloc[-2] and
                        df['close'].iloc[-1] > df['SUPERT_10_3.0'].iloc[-1]):
                    return 'alcista'
                elif (df['close'].iloc[-2] >= df['SUPERT_10_3.0'].iloc[-2] and
                      df['close'].iloc[-1] < df['SUPERT_10_3.0'].iloc[-1]):
                    return 'bajista'
                return 'neutral'

        except Exception as e:
            logger.error("Error al generar señal: %s", e, exc_info=True)
            return None

    def registrar_senal(self, pair, tipo_senal, precio_actual, timestamp):
        """Registra una señal en la base de datos."""
        query = """
            INSERT INTO senales (timestamp, par_de_divisas, tipo_senal, origen, price_signal)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (timestamp, par_de_divisas) DO NOTHING;
        """
        try:
            connection = self.obtener_conexion()
            if not connection:
                raise ConnectionError("No se pudo establecer la conexión a la base de datos.")

            with connection.cursor() as cursor:
                cursor.execute(query, (
                    timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                    pair, tipo_senal, 'ForexSignalAnalyzer', float(precio_actual)
                ))
                connection.commit()
                logger.info("Señal registrada: %s -> %s a %s con precio %.5f", pair, tipo_senal, timestamp, precio_actual)
        except Exception as e:
            logger.error("Error al registrar señal para %s: %s", pair, e, exc_info=True)
        finally:
            if connection:
                connection.close()
