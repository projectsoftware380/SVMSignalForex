import sys
import os
import json
import logging
import threading
import time
from flask import Flask, jsonify, request
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
    format='%(asctime)s - %(levelname)s - %(message)s'
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

# Crear la aplicación Flask
app = Flask(__name__)

# Endpoint para obtener las reversiones almacenadas
@app.route('/reversals', methods=['GET'])
def obtener_reversiones():
    try:
        with open(REVERSIONS_FILE, 'r', encoding='utf-8') as f:
            reversiones = json.load(f)
        logging.info("Reversiones obtenidas correctamente.")
        return jsonify(reversiones)
    except FileNotFoundError:
        logging.warning(f"Archivo {REVERSIONS_FILE} no encontrado.")
        return jsonify({})
    except Exception as e:
        logging.error(f"Error al obtener las reversiones: {e}")
        return jsonify({"error": "No se pudieron obtener las reversiones"}), 500

# Endpoint para calcular las reversiones para todos los pares
@app.route('/analyze_reversals', methods=['GET'])
def analizar_reversiones():
    try:
        # Cargar los pares de divisas desde config.json
        pares_a_analizar = config['pairs']
        reversiones = {}

        for pair in pares_a_analizar:
            # Calcular la reversión para cada par
            reversion = forex_reversal_analyzer.analizar_reversion_para_par(pair)
            if reversion:
                reversiones[pair] = reversion

        # Guardar los resultados en el archivo JSON
        guardar_reversiones_en_json(reversiones)

        logging.info("Reversiones calculadas correctamente.")
        return jsonify(reversiones)
    except Exception as e:
        logging.error(f"Error al analizar las reversiones: {e}")
        return jsonify({"error": "Ocurrió un error al analizar las reversiones"}), 500

# Función para ejecutar el análisis sincronizado con la nueva vela de 15 minutos
def ejecutar_analisis_cuando_nueva_vela():
    while True:
        try:
            # Calcular el tiempo hasta la próxima vela de 15 minutos
            tiempo_restante = forex_reversal_analyzer.tiempo_para_proxima_vela()
            logging.info(f"Esperando {tiempo_restante} segundos para la próxima vela de 15 minutos.")
            time.sleep(tiempo_restante)

            # Ejecutar el análisis de reversiones para todos los pares cuando se genere una nueva vela
            logging.info("Iniciando análisis de reversión con nueva vela de 15 minutos.")
            reversiones = forex_reversal_analyzer.analizar_reversiones()

            # Guardar las reversiones en el archivo JSON
            guardar_reversiones_en_json(reversiones)
            logging.info("Análisis de reversiones completado y guardado.")
        except Exception as e:
            logging.error(f"Error en el análisis de reversiones sincronizado: {e}")
            time.sleep(60)  # Esperar antes de intentar nuevamente en caso de error

# Iniciar el análisis de reversiones en un hilo en segundo plano
def iniciar_hilo_analisis():
    hilo_analisis = threading.Thread(target=ejecutar_analisis_cuando_nueva_vela)
    hilo_analisis.daemon = True  # Hilo como demonio
    hilo_analisis.start()

# Iniciar el servidor
if __name__ == '__main__':
    try:
        logging.info("Iniciando el servidor de reversiones en el puerto 5001...")
        # Iniciar el análisis en segundo plano
        iniciar_hilo_analisis()

        # Iniciar la aplicación Flask
        app.run(port=5001)
    except Exception as e:
        logging.error(f"Error al iniciar el servidor: {e}")
