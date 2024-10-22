import sys
import os
import json
import logging
import threading
import time
from datetime import datetime, timezone, timedelta
import pytz
import psycopg2

# Ajustar la ruta del proyecto para importar módulos correctamente
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

# Importar la clase ForexReversalAnalyzer desde la carpeta 'reversals'
from src.reversals.ForexReversalAnalyzer import ForexReversalAnalyzer

# Verificar y crear directorio de logs
logs_directory = os.path.join(os.path.dirname(__file__), '..', 'logs')
os.makedirs(logs_directory, exist_ok=True)

# Configuración básica de logging
logging.basicConfig(
    filename=os.path.join(logs_directory, 'reversal_server.log'),
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    encoding='utf-8'
)
logger = logging.getLogger(__name__)

# Cargar configuración desde config.json
CONFIG_FILE = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.json')
try:
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        config = json.load(f)
        logger.info("Configuración cargada correctamente.")
except Exception as e:
    logger.error(f"Error al cargar el archivo de configuración: {e}")
    raise

# Inicializar el analizador de reversiones usando la configuración de la base de datos
try:
    forex_reversal_analyzer = ForexReversalAnalyzer(db_config=config['db_config'])
    logger.info("ForexReversalAnalyzer inicializado correctamente.")
except Exception as e:
    logger.error(f"Error al inicializar ForexReversalAnalyzer: {e}")
    raise

# Definir la ubicación del archivo reversiones.json
REVERSIONS_FILE = os.path.join(os.path.dirname(__file__), '..', 'data', 'reversiones.json')

# Función para obtener la fecha y hora actual en la zona horaria de Colombia
def obtener_hora_colombia():
    zona_colombia = pytz.timezone('America/Bogota')
    return datetime.now(zona_colombia).strftime('%Y-%m-%d %H:%M:%S')

# Guardar reversiones en archivo JSON con timestamp actualizado
def guardar_reversiones_en_json(reversiones):
    try:
        timestamp_colombia = obtener_hora_colombia()
        reversiones['last_timestamp'] = timestamp_colombia  # Agregar timestamp

        with open(REVERSIONS_FILE, 'w', encoding='utf-8') as f:
            json.dump(reversiones, f, indent=4, ensure_ascii=False)
        logger.info(f"Reversiones guardadas en {REVERSIONS_FILE} con timestamp {timestamp_colombia}.")
    except Exception as e:
        logger.error(f"Error al guardar las reversiones: {e}", exc_info=True)

# Función para ejecutar análisis de reversiones periódicamente
def ejecutar_analisis_automatico():
    while True:
        try:
            pares_a_analizar = config['pairs']
            reversiones = {}

            # Analizar cada par de divisas
            for pair in pares_a_analizar:
                reversion = forex_reversal_analyzer.analizar_reversion_para_par(pair)
                if reversion is None:
                    logger.warning(f"Análisis para {pair} no completado.")
                    reversiones[pair] = "Error en análisis"
                else:
                    reversiones[pair] = reversion  # Puede ser 'alcista', 'bajista' o 'neutral'

            # Guardar resultados en archivo JSON
            guardar_reversiones_en_json(reversiones)
            logger.info("Análisis de reversiones completado.")

            # Calcular tiempo hasta la próxima vela de 15 minutos
            tiempo_restante = forex_reversal_analyzer.tiempo_para_proxima_vela()
            logger.info(f"Esperando {tiempo_restante} segundos para la próxima vela.")
            time.sleep(tiempo_restante)
        except Exception as e:
            logger.error(f"Error en el análisis automático: {e}", exc_info=True)
            time.sleep(60)  # Reintentar tras 1 minuto

# Iniciar análisis en un hilo en segundo plano
def iniciar_hilo_analisis():
    hilo_analisis = threading.Thread(target=ejecutar_analisis_automatico)
    hilo_analisis.daemon = True
    hilo_analisis.start()

# Iniciar servidor y mantener activo
if __name__ == '__main__':
    try:
        logger.info("Iniciando el servidor de reversiones...")
        iniciar_hilo_analisis()  # Iniciar análisis automático en segundo plano

        # Mantener el servidor activo
        while True:
            time.sleep(1)
    except Exception as e:
        logger.error(f"Error al iniciar el servidor: {e}", exc_info=True)

