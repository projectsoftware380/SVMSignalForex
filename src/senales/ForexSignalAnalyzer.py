import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from datetime import datetime, timedelta, timezone
import pytz
import threading
import logging
import time
import json
import traceback
import pandas_ta as ta
import os

# Configurar el logging
logs_directory = os.path.join(os.path.dirname(__file__), '..', 'logs')
if not os.path.exists(logs_directory):
    os.makedirs(logs_directory)

logging.basicConfig(
    filename=os.path.join(logs_directory, 'signal_server.log'),
    level=logging.DEBUG,  
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Cargar configuración desde config.json
CONFIG_FILE = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.json')

try:
    with open(CONFIG_FILE, "r", encoding='utf-8') as f:
        config = json.load(f)
        logging.info("Archivo de configuración cargado correctamente.")
except Exception as e:
    logging.error(f"Error al cargar el archivo de configuración: {e}")
    raise

# Definir la ubicación del archivo signals.json
SIGNALS_FILE = os.path.join(os.path.dirname(__file__), '..', 'data', 'signals.json')

class ForexSignalAnalyzer:
    def __init__(self, db_config):
        self.db_config = db_config
        self.lock = threading.Lock()
        
        # Crear la conexión a la base de datos PostgreSQL
        self.engine = create_engine(
            f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}/{db_config['database']}"
        )
    
    def obtener_datos_postgresql(self, pair, start_date, end_date):
        """
        Obtiene datos OHLC de la base de datos PostgreSQL para un par de divisas específico.
        """
        query = f"""
        SELECT timestamp, open, high, low, close
        FROM forex_data_3m
        WHERE pair = '{pair}'
        AND timestamp BETWEEN '{start_date}' AND '{end_date}'
        ORDER BY timestamp DESC;
        """
        
        try:
            df = pd.read_sql(query, self.engine)
            df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
            df.set_index('timestamp', inplace=True)
            return df
        except Exception as e:
            logging.error(f"Error al obtener datos para {pair} desde la base de datos: {str(e)}")
            return pd.DataFrame()

    def obtener_hora_colombia(self):
        """Obtiene la hora actual en la zona horaria de Colombia."""
        zona_colombia = pytz.timezone('America/Bogota')
        hora_actual = datetime.now(zona_colombia)
        return hora_actual.strftime('%Y-%m-%d %H:%M:%S')

    def calcular_indicadores(self, df):
        """Calcula el Supertrend."""
        try:
            if df.empty:
                raise ValueError("El DataFrame está vacío, no se pueden calcular los indicadores.")

            logging.info(f"Calculando Supertrend para los datos proporcionados.")
            supertrend_df = ta.supertrend(df['high'], df['low'], df['close'], length=14, multiplier=3)
            df['Supertrend'] = supertrend_df['SUPERT_14_3.0']
            logging.debug(f"Supertrend calculado exitosamente.")
            return df
        except Exception as e:
            logging.error(f"Error al calcular indicadores: {e}")
            logging.error(traceback.format_exc())
            return df

    def generar_senal_trading(self, df):
        """Genera señales de trading basadas en el Supertrend."""
        try:
            if df.empty or 'Supertrend' not in df.columns:
                raise ValueError("Datos insuficientes o falta la columna 'Supertrend'.")

            close = df['close'].iloc[-1]
            supertrend = df['Supertrend'].iloc[-1]

            logging.info(f"Valores de Indicadores - Close: {close}, Supertrend: {supertrend}")

            if close > supertrend:
                logging.info(f"Señal de Compra detectada en {df.index[-1]}")
                return "Señal de Compra"

            elif close < supertrend:
                logging.info(f"Señal de Venta detectada en {df.index[-1]}")
                return "Señal de Venta"

            logging.info(f"No se detectó señal en {df.index[-1]}.")
            return None
        except Exception as e:
            logging.error(f"Error al generar la señal de trading: {e}")
            logging.error(traceback.format_exc())
            return None

    def analizar_senal_para_par(self, pair):
        """Función que maneja el análisis de señales para cada par."""
        try:
            logging.info(f"Analizando señal para {pair}")
            now = datetime.now(timezone.utc)
            start_date = now - timedelta(hours=12)  # Últimas 12 horas
            df = self.obtener_datos_postgresql(pair, start_date, now)
            if df.empty:
                logging.warning(f"No se obtuvieron datos para {pair}.")
                return None

            df = self.calcular_indicadores(df)
            return self.generar_senal_trading(df)
        except ValueError as e:
            logging.error(f"Error en el análisis de señales para {pair}: {str(e)}")
            logging.error(traceback.format_exc())
        except Exception as e:
            logging.error(f"Error inesperado al analizar la señal para {pair}: {str(e)}")
            logging.error(traceback.format_exc())
        return None

    def analizar_senales(self):
        """Analiza todas las señales para los pares de divisas en config.json."""
        pares_a_analizar = config['pairs']
        resultados = {}

        logging.info(f"Analizando señales para los pares: {pares_a_analizar}")

        for pair in pares_a_analizar:
            senal = self.analizar_senal_para_par(pair)
            if senal:
                resultados[pair] = senal

        return resultados

    def tiempo_para_proxima_vela(self):
        """Calcula el tiempo restante hasta la próxima vela de 3 minutos."""
        ahora = datetime.now(timezone.utc)
        proxima_vela = ahora.replace(minute=(ahora.minute // 3) * 3, second=0, microsecond=0) + timedelta(minutes=3)
        return (proxima_vela - ahora).total_seconds()

    def ejecutar_analisis_cuando_nueva_vela(self):
        """Ejecuta el análisis de señales sincronizado con la creación de nuevas velas de 3 minutos."""
        while True:
            # Calcular el tiempo hasta la próxima vela de 3 minutos
            tiempo_restante = self.tiempo_para_proxima_vela()
            logging.info(f"Esperando {tiempo_restante} segundos para la próxima vela de 3 minutos.")
            time.sleep(tiempo_restante)  # Esperar hasta la nueva vela
            logging.info("Iniciando análisis de señales con nueva vela de 3 minutos.")
            
            # Ejecutar el análisis de señales
            senales = self.analizar_senales()

            # Guardar las señales en el archivo JSON
            self.guardar_senales_en_json(senales)
            logging.info("Análisis de señales completado y guardado.")

    def guardar_senales_en_json(self, senales):
        """Guarda las señales generadas en el archivo JSON, incluyendo la fecha y hora de Colombia."""
        try:
            timestamp_colombia = self.obtener_hora_colombia()  # Obtener la hora en Colombia
            senales['last_timestamp'] = timestamp_colombia  # Agregar la hora al archivo JSON

            with open(SIGNALS_FILE, 'w', encoding='utf-8') as f:
                json.dump(senales, f, indent=4, ensure_ascii=False)
            logging.info(f"Señales guardadas en {SIGNALS_FILE} con timestamp {timestamp_colombia}.")
        except Exception as e:
            logging.error(f"Error al guardar las señales en {SIGNALS_FILE}: {e}")

# Crear un hilo para ejecutar el análisis sincronizado con la creación de nuevas velas de 3 minutos
def iniciar_hilo_analisis():
    signal_analyzer = ForexSignalAnalyzer(db_config=config['db_config'])
    hilo_analisis = threading.Thread(target=signal_analyzer.ejecutar_analisis_cuando_nueva_vela)
    hilo_analisis.daemon = True  # Hilo como demonio
    hilo_analisis.start()

if __name__ == "__main__":
    iniciar_hilo_analisis()
    while True:
        time.sleep(1)  # Mantener el script corriendo
