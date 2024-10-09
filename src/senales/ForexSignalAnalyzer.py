import psycopg2
import pandas as pd
import pandas_ta as ta
from datetime import datetime, timezone
import pytz  # Para obtener la hora en Colombia
import logging
import threading
import time
import os
import json
import unicodedata

# Configuración del logger
logger = logging.getLogger(__name__)

class ForexSignalAnalyzer:
    def __init__(self, db_config):
        self.db_config = db_config
        self.lock = threading.Lock()
        # Ruta al archivo JSON donde se guardarán las señales
        self.SIGNALS_FILE = os.path.join(os.path.dirname(__file__), '..', 'data', 'signals.json')
        # Cargar configuración desde config.json
        CONFIG_FILE = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.json')
        with open(CONFIG_FILE, "r", encoding='utf-8') as f:
            self.config = json.load(f)
        self.pairs = self.config.get("pairs", [])

    def normalizar_string(self, valor):
        """Normaliza una cadena de texto eliminando caracteres especiales."""
        if isinstance(valor, str):
            return unicodedata.normalize('NFKD', valor).encode('ascii', 'ignore').decode('ascii')
        return valor

    def obtener_datos_bd(self, symbol, registros=150):
        """Obtiene los datos más recientes de la base de datos y los prepara para el análisis."""
        try:
            logger.info(f"Obteniendo datos para {symbol}.")
            connection = psycopg2.connect(**self.db_config)
            cursor = connection.cursor()

            # Consulta SQL para obtener los registros más recientes
            query = """
            SELECT timestamp, open, high, low, close, volume
            FROM forex_data_3m
            WHERE pair = %s
            ORDER BY timestamp DESC
            LIMIT %s;
            """
            cursor.execute(query, (symbol, registros))
            rows = cursor.fetchall()

            # Convertir a DataFrame
            df = pd.DataFrame(rows, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df.set_index('timestamp', inplace=True)

            # Ordenar el DataFrame en orden ascendente
            df = df.sort_index()

            logger.info(f"Datos obtenidos para {symbol}. Número de filas: {len(df)}")
            logger.debug(f"Datos iniciales:\n{df.head()}")

            if df.empty:
                logger.warning(f"No se encontraron resultados en la base de datos para {symbol}.")
                return pd.DataFrame()

            # Eliminar duplicados en los timestamps
            df = df[~df.index.duplicated(keep='first')]

            # Convertir los valores a float para asegurar compatibilidad con pandas_ta
            cols_to_convert = ['open', 'high', 'low', 'close', 'volume']
            df[cols_to_convert] = df[cols_to_convert].apply(pd.to_numeric, errors='coerce')

            # Eliminar filas con valores NaN
            df.dropna(subset=cols_to_convert, inplace=True)

            # Reindexar el DataFrame para asegurar timestamps uniformes cada 3 minutos
            df = df.asfreq('3min')

            # Interpolar los valores faltantes
            df[cols_to_convert] = df[cols_to_convert].interpolate(method='time')

            # Eliminar filas con valores NaN restantes después de la interpolación
            df.dropna(subset=cols_to_convert, inplace=True)

            # Verificar que el DataFrame tiene suficientes filas
            if len(df) < 22:
                logger.error(f"Datos insuficientes para {symbol}. Se requieren al menos 22 registros.")
                return pd.DataFrame()

            logger.debug(f"Datos procesados para {symbol}:\n{df.tail()}")
            return df
        except Exception as e:
            logger.error(f"Error al obtener datos de la base de datos para {symbol}: {e}", exc_info=True)
            return pd.DataFrame()
        finally:
            if 'connection' in locals() and connection:
                cursor.close()
                connection.close()

    def analizar_senales(self):
        """Analiza señales para todos los pares y guarda los resultados en un archivo JSON."""
        resultados = {}

        logger.info(f"Analizando señales para los pares: {self.pairs}")

        for symbol in self.pairs:
            resultado = self.analizar_senal_para_par(symbol)
            if resultado is not None:
                resultados[symbol] = self.normalizar_string(resultado)

        # Guardar resultados en archivo JSON
        with self.lock:
            timestamp_colombia = self.obtener_hora_colombia()
            resultados['last_update'] = self.normalizar_string(timestamp_colombia)
            with open(self.SIGNALS_FILE, 'w', encoding='utf-8') as f:
                json.dump(resultados, f, indent=4)
            logger.info(f"Señales guardadas en {self.SIGNALS_FILE} con timestamp {timestamp_colombia}.")

        return resultados

    def analizar_senal_para_par(self, symbol):
        """Analiza señal para un par específico usando Supertrend."""
        logger.info(f"Analizando señal para {symbol}")
        df = self.obtener_datos_bd(symbol)
        if df.empty:
            logger.error(f"No se pudieron obtener datos para {symbol}.")
            return None

        # Implementar la lógica de análisis de señales con Supertrend
        signal = self.generar_senal(df)
        return signal

    def generar_senal(self, df):
        """Genera una señal basada en el indicador Supertrend."""
        try:
            # Calcular Supertrend
            logger.info("Calculando el indicador Supertrend.")
            supertrend = ta.supertrend(df['high'], df['low'], df['close'], length=10, multiplier=3)
            df = df.join(supertrend)

            logger.debug(f"Supertrend calculado:\n{df[['SUPERT_10_3.0', 'SUPERTd_10_3.0']].tail()}")

            # Detectar señal de compra o venta basándose en Supertrend
            if df['SUPERTd_10_3.0'].iloc[-2] == -1 and df['SUPERTd_10_3.0'].iloc[-1] == 1:
                logger.info("Señal de Compra detectada con Supertrend.")
                return "Señal de Compra"
            elif df['SUPERTd_10_3.0'].iloc[-2] == 1 and df['SUPERTd_10_3.0'].iloc[-1] == -1:
                logger.info("Señal de Venta detectada con Supertrend.")
                return "Señal de Venta"
            else:
                logger.info("No se detectó señal (Supertrend).")
                return "Sin Señal"
        except Exception as e:
            logger.error(f"Error al generar señal con Supertrend: {e}", exc_info=True)
            return None

    def obtener_hora_colombia(self):
        """Obtiene la hora actual en la zona horaria de Colombia."""
        zona_colombia = pytz.timezone('America/Bogota')
        hora_actual_colombia = datetime.now(zona_colombia)
        return hora_actual_colombia.strftime('%Y-%m-%d %H:%M:%S')

    def tiempo_para_proxima_vela(self):
        """Calcula el tiempo restante hasta la próxima vela de 3 minutos."""
        ahora = datetime.now(timezone.utc)
        minutos_actuales = ahora.minute % 3
        segundos_actuales = ahora.second
        segundos_restantes = (2 - minutos_actuales) * 60 + (60 - segundos_actuales)
        return max(0, segundos_restantes)

    def ejecutar_analisis_cuando_nueva_vela(self):
        """Ejecuta el análisis al inicio de cada nueva vela de 3 minutos."""
        while True:
            try:
                tiempo_restante = self.tiempo_para_proxima_vela()
                logger.info(f"Esperando {tiempo_restante} segundos para la próxima vela de 3 minutos.")
                time.sleep(tiempo_restante)
                logger.info("Iniciando análisis de señales.")
                self.analizar_senales()
            except Exception as e:
                logger.error(f"Error en el bucle principal: {e}", exc_info=True)
                time.sleep(60)
