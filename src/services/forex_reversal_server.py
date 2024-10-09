import sys
import os
import json
import logging
import threading
import time
from datetime import datetime, timezone, timedelta
import pytz
import psycopg2

# Ajustar la ruta del proyecto para que pueda importar los módulos correctamente
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

# Importar la clase ForexReversalAnalyzer desde la carpeta 'reversals'
from src.reversals.ForexReversalAnalyzer import ForexReversalAnalyzer

# Verificar si el directorio 'logs' existe, si no, crearlo
logs_directory = os.path.join(os.path.dirname(__file__), '..', 'logs')
if not os.path.exists(logs_directory):
    os.makedirs(logs_directory)

# Configuración básica de logging
logging.basicConfig(
    filename=os.path.join(logs_directory, 'reversal_server.log'),
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    encoding='utf-8'
)

# Cargar configuración desde config.json
CONFIG_FILE = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.json')
try:
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        config = json.load(f)
        logging.info("Configuración cargada correctamente.")
except Exception as e:
    logging.error(f"Error al cargar el archivo de configuración: {e}")
    raise

# Inicializar el analizador de reversiones usando la configuración de la base de datos
try:
    forex_reversal_analyzer = ForexReversalAnalyzer(db_config=config['db_config'])
    logging.info("ForexReversalAnalyzer inicializado correctamente.")
except Exception as e:
    logging.error(f"Error al inicializar ForexReversalAnalyzer: {e}")
    raise

# Definir la ubicación del archivo reversiones.json
REVERSIONS_FILE = os.path.join(os.path.dirname(__file__), '..', 'data', 'reversiones.json')

# Función para obtener la fecha y hora actual en la zona horaria de Colombia
def obtener_hora_colombia():
    zona_colombia = pytz.timezone('America/Bogota')
    hora_actual = datetime.now(zona_colombia)
    return hora_actual.strftime('%Y-%m-%d %H:%M:%S')

# Guardar reversiones en archivo JSON, incluyendo la fecha y hora del último cálculo
def guardar_reversiones_en_json(reversiones):
    try:
        timestamp_colombia = obtener_hora_colombia()  # Obtener la hora en Colombia
        reversiones['last_timestamp'] = timestamp_colombia  # Agregar la hora al archivo JSON
        
        with open(REVERSIONS_FILE, 'w', encoding='utf-8') as f:
            json.dump(reversiones, f, indent=4, ensure_ascii=False)  # Guardar el archivo sin escapar caracteres Unicode
        logging.info(f"Reversiones guardadas en {REVERSIONS_FILE} con timestamp {timestamp_colombia}.")
    except Exception as e:
        logging.error(f"Error al guardar las reversiones en {REVERSIONS_FILE}: {e}")

# Función para ejecutar el análisis de reversiones automáticamente sin necesidad de invocar los endpoints
def ejecutar_analisis_automatico():
    while True:
        try:
            # Cargar los pares de divisas desde config.json
            pares_a_analizar = config['pairs']
            reversiones = {}

            # Ejecutar análisis para cada par
            for pair in pares_a_analizar:
                reversion = forex_reversal_analyzer.analizar_reversion_para_par(pair)
                if reversion is None:
                    logging.warning(f"Análisis de reversión para {pair} no pudo completarse.")
                    reversiones[pair] = "Error en análisis"
                else:
                    reversiones[pair] = reversion

            # Guardar los resultados en el archivo JSON
            guardar_reversiones_en_json(reversiones)

            logging.info("Análisis de reversiones completado y guardado.")
            
            # Calcular el tiempo hasta la próxima vela de 15 minutos
            tiempo_restante = forex_reversal_analyzer.tiempo_para_proxima_vela()
            logging.info(f"Esperando {tiempo_restante} segundos para la próxima vela de 15 minutos.")
            time.sleep(tiempo_restante)
        except Exception as e:
            logging.error(f"Error en el análisis automático de reversiones: {e}")
            time.sleep(60)  # Esperar antes de intentar nuevamente en caso de error

# Iniciar el análisis de reversiones en un hilo en segundo plano
def iniciar_hilo_analisis():
    hilo_analisis = threading.Thread(target=ejecutar_analisis_automatico)
    hilo_analisis.daemon = True  # Hilo como demonio
    hilo_analisis.start()

# Iniciar el servidor sin necesidad de endpoints para desencadenar el análisis
if __name__ == '__main__':
    try:
        logging.info("Iniciando el servidor de reversiones en el puerto 5001...")

        # Iniciar el análisis automático en segundo plano
        iniciar_hilo_analisis()

        # Mantener el servidor activo (si hay necesidad de endpoints adicionales)
        while True:
            time.sleep(1)
    except Exception as e:
        logging.error(f"Error al iniciar el servidor: {e}")
