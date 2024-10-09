import sys
import os
import json
import logging
import threading
import time
from flask import Flask, jsonify
from datetime import datetime, timezone
import pytz
import psycopg2
import pandas as pd
import talib as ta

# Configuración básica de logging
log_dir = os.path.join(os.path.dirname(__file__), '..', 'logs')
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

logging.basicConfig(
    filename=os.path.join(log_dir, 'candle_pattern_server.log'),
    level=logging.INFO,  # Cambiado a INFO para reducir la verbosidad
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Cargar configuración desde config.json
CONFIG_FILE = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.json')
with open(CONFIG_FILE, "r") as f:
    config = json.load(f)

# Inicializar Flask
app = Flask(__name__)

# Definir la ubicación del archivo candle_patterns.json
CANDLE_PATTERNS_FILE = os.path.join(os.path.dirname(__file__), '..', 'data', 'candle_patterns.json')

# Función para obtener la fecha y hora en Colombia
def obtener_hora_colombia():
    zona_colombia = pytz.timezone('America/Bogota')
    hora_actual = datetime.now(zona_colombia)
    return hora_actual.strftime('%Y-%m-%d %H:%M:%S')

# Clase CandlePatternAnalyzer
class CandlePatternAnalyzer:
    def __init__(self, db_config):
        self.db_config = db_config
        self.lock = threading.Lock()
        self.pairs = config.get("pairs", [])
        # Lista de patrones de velas disponibles en TA-Lib
        self.pattern_functions = {
            'CDLDOJI': ta.CDLDOJI,
            'CDLENGULFING': ta.CDLENGULFING,
            'CDLHAMMER': ta.CDLHAMMER,
            'CDLSHOOTINGSTAR': ta.CDLSHOOTINGSTAR,
        }

    def obtener_datos_bd(self, symbol, registros=100, timeframe='3m'):
        """Obtiene los datos más recientes de la base de datos según el timeframe."""
        try:
            connection = psycopg2.connect(**self.db_config)
            cursor = connection.cursor()

            # Definir la tabla según la temporalidad
            if timeframe == '3m':
                table = 'forex_data_3m'
            elif timeframe == '15m':
                table = 'forex_data_15m'
            elif timeframe == '4h':
                table = 'forex_data_4h'
            else:
                logger.error(f"Timeframe no soportado: {timeframe}")
                return pd.DataFrame()

            # Consulta SQL para obtener los registros más recientes
            query = f"""
            SELECT timestamp, open, high, low, close, volume
            FROM {table}
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

            logger.info(f"Datos obtenidos para {symbol} en el timeframe {timeframe}. Número de filas: {len(df)}")
            logger.info(f"Rango de fechas para {symbol}: Desde {df.index.min()} hasta {df.index.max()}")

            if df.empty:
                logger.warning(f"No se encontraron resultados en la base de datos para {symbol}.")
                return pd.DataFrame()

            # Eliminar duplicados en los timestamps
            df = df[~df.index.duplicated(keep='first')]

            # Convertir los valores a float para asegurar compatibilidad con TA-Lib
            cols_to_convert = ['open', 'high', 'low', 'close', 'volume']
            df[cols_to_convert] = df[cols_to_convert].apply(pd.to_numeric, errors='coerce')

            # Eliminar filas con valores NaN
            df.dropna(subset=cols_to_convert, inplace=True)

            # Reindexar el DataFrame para asegurar timestamps uniformes
            if timeframe == '3m':
                df = df.asfreq('3min')
            elif timeframe == '15m':
                df = df.asfreq('15min')
            elif timeframe == '4h':
                df = df.asfreq('4h')

            # Interpolar los valores faltantes
            df[cols_to_convert] = df[cols_to_convert].interpolate(method='time')

            # Eliminar filas con valores NaN restantes después de la interpolación
            df.dropna(subset=cols_to_convert, inplace=True)

            # Verificar que el DataFrame tiene suficientes filas
            if len(df) < 22:
                logger.error(f"Datos insuficientes para {symbol} en {timeframe}. Se requieren al menos 22 registros.")
                return pd.DataFrame()

            return df
        except Exception as e:
            logger.error(f"Error al obtener datos de la base de datos para {symbol} en {timeframe}: {e}", exc_info=True)
            return pd.DataFrame()
        finally:
            if 'connection' in locals() and connection:
                cursor.close()
                connection.close()

    def detectar_patrones_para_par(self, symbol, timeframe='3m'):
        """Detecta patrones de velas para un par específico en un timeframe específico."""
        logger.info(f"Analizando patrones para {symbol} en el timeframe {timeframe}")
        df = self.obtener_datos_bd(symbol, timeframe=timeframe)
        if df.empty:
            logger.error(f"No se pudieron obtener datos para {symbol} en {timeframe}.")
            return None

        patrones_detectados = {}

        try:
            # Iterar sobre los patrones disponibles
            for pattern_name, pattern_function in self.pattern_functions.items():
                result = pattern_function(df['open'], df['high'], df['low'], df['close'])
                # Verificar si hay un patrón detectado en la última vela
                if result.iloc[-1] != 0:
                    patrones_detectados[pattern_name] = {
                        'tipo': 'Alcista' if result.iloc[-1] > 0 else 'Bajista',
                        'timeframe': timeframe
                    }
                    logger.info(f"Patrón {pattern_name} detectado para {symbol} en {timeframe} con valor {result.iloc[-1]}.")

            if not patrones_detectados:
                logger.info(f"No se detectaron patrones para {symbol} en {timeframe}.")

            return patrones_detectados
        except Exception as e:
            logger.error(f"Error al detectar patrones para {symbol} en {timeframe}: {e}", exc_info=True)
            return None

    def analizar_patrones(self):
        """Analiza patrones para todos los pares y guarda los resultados en un archivo JSON."""
        patrones_detectados = {}

        logger.info(f"Analizando patrones para los pares: {self.pairs}")

        for symbol in self.pairs:
            for timeframe in ['3m', '15m', '4h']:
                patrones = self.detectar_patrones_para_par(symbol, timeframe=timeframe)
                if patrones is not None:
                    patrones_detectados.setdefault(symbol, {}).update(patrones)

        # Guardar los patrones en el archivo JSON
        with self.lock:
            timestamp_colombia = obtener_hora_colombia()
            patrones_detectados['last_timestamp'] = timestamp_colombia
            with open(CANDLE_PATTERNS_FILE, 'w', encoding='utf-8') as f:
                json.dump(patrones_detectados, f, indent=4, ensure_ascii=False)
            logger.info(f"Patrones guardados en {CANDLE_PATTERNS_FILE} con timestamp {timestamp_colombia}.")

        return patrones_detectados

    def tiempo_para_proxima_vela(self):
        """Calcula el tiempo restante hasta la próxima vela de 3 minutos."""
        ahora = datetime.now(timezone.utc)
        segundos_actuales = (ahora.minute % 3) * 60 + ahora.second
        tiempo_restante = (180 - segundos_actuales) % 180
        return tiempo_restante

    def ejecutar_analisis_cuando_nueva_vela(self):
        """Ejecuta el análisis al inicio de cada nueva vela de 3 minutos."""
        while True:
            try:
                tiempo_restante = self.tiempo_para_proxima_vela()
                logger.info(f"Esperando {tiempo_restante} segundos para la próxima vela de 3 minutos.")
                time.sleep(tiempo_restante)
                logger.info("Iniciando análisis de patrones con nueva vela de 3 minutos.")
                self.analizar_patrones()
            except Exception as e:
                logger.error(f"Error en el análisis de patrones automático: {e}", exc_info=True)
                time.sleep(60)  # Esperar antes de intentar nuevamente en caso de error

# Inicializar el analizador de patrones de velas con los parámetros de la base de datos desde config.json
db_config = config["db_config"]
analyzer = CandlePatternAnalyzer(db_config)

# Iniciar el análisis de patrones en un hilo en segundo plano
def iniciar_hilo_analisis():
    hilo_analisis = threading.Thread(target=analyzer.ejecutar_analisis_cuando_nueva_vela)
    hilo_analisis.daemon = True
    hilo_analisis.start()

# Endpoint para obtener los patrones almacenados
@app.route('/patterns', methods=['GET'])
def obtener_patrones():
    try:
        with open(CANDLE_PATTERNS_FILE, 'r', encoding='utf-8') as f:
            patrones = json.load(f)
        logger.info("Patrones obtenidos correctamente.")
        return jsonify(patrones)
    except FileNotFoundError:
        logger.warning(f"Archivo {CANDLE_PATTERNS_FILE} no encontrado.")
        return jsonify({})
    except Exception as e:
        logger.error(f"Error al obtener los patrones: {e}")
        return jsonify({"error": "No se pudieron obtener los patrones"}), 500

# Iniciar el servidor
if __name__ == '__main__':
    try:
        logger.info("Iniciando el servidor de detección de patrones de velas...")
        # Iniciar el análisis en segundo plano
        iniciar_hilo_analisis()

        # Iniciar la aplicación Flask
        app.run(host='0.0.0.0', port=5004)
    except Exception as e:
        logger.error(f"Error al iniciar el servidor: {e}")
