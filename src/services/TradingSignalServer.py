import os
import logging
from flask import Flask, jsonify
import threading
import sys
import time
import psycopg2
import json

# Ajuste para encontrar la clase SignalManager y SignalTracker
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
sys.path.append(ROOT_DIR)
from src.SignalManager.SignalManager import SignalManager
from src.SignalManager.SignalTracker import SignalTracker

# Configuración del archivo de log
LOG_DIR = os.path.join(os.path.dirname(__file__), '..', 'logs')
LOG_FILE = os.path.join(LOG_DIR, 'trading_signal_server.log')

# Verificación de la ruta del log
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

# Configuración del logging
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Configuración de Flask
app = Flask(__name__)

# Ruta del archivo config.json
CONFIG_FILE = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.json')

# Cargar la configuración de la base de datos desde el archivo config.json
def cargar_configuracion(config_file):
    with open(config_file, 'r') as f:
        return json.load(f)

config = cargar_configuracion(CONFIG_FILE)
db_config = config["db_config"]

# Conexión a la base de datos PostgreSQL
def conectar_db():
    try:
        conn = psycopg2.connect(
            host=db_config['host'],
            database=db_config['database'],
            user=db_config['user'],
            password=db_config['password']
        )
        logging.info("Conexión a la base de datos PostgreSQL exitosa.")
        return conn
    except psycopg2.Error as e:
        logging.error(f"Error al conectar a la base de datos: {e}")
        sys.exit(1)

# Conectar a la base de datos
conn = conectar_db()

# Directorio de datos (donde están los archivos JSON)
DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')

# Inicialización del SignalManager y SignalTracker con logging
signal_manager = SignalManager(DATA_DIR, CONFIG_FILE, logger=logging)  # Cambié conn a DATA_DIR aquí
signal_tracker = SignalTracker(conn, logger=logging)

# Función para generar señales automáticamente en un hilo
def generar_senales_automaticamente():
    while True:
        try:
            # Generar señales
            senales = signal_manager.generar_senales()
            if senales:
                logging.info(f"Señales generadas y guardadas automáticamente: {senales}")
            else:
                logging.warning("No se generaron señales automáticamente.")
        except Exception as e:
            logging.error(f"Error al generar señales automáticamente: {e}")
        time.sleep(180)  # Espera de 3 minutos antes de volver a generar

# Función para el seguimiento y la inactivación de señales automáticamente en un hilo
def seguimiento_senales_automatico():
    while True:
        try:
            # Replicar las señales generadas en tracked_signals y verificar su estado
            signal_tracker.replicar_logica_senal_activa()
            logging.info("Señales replicadas y estado actualizado automáticamente.")
        except Exception as e:
            logging.error(f"Error durante el seguimiento automático de señales: {e}")
        time.sleep(180)  # Espera de 3 minutos antes de volver a actualizar el seguimiento

# Ruta de Flask para generar señales de trading (manual)
@app.route('/generar_senales', methods=['GET'])
def obtener_senales():
    try:
        # Generar señales manualmente
        senales = signal_manager.generar_senales()
        if senales:
            return jsonify(senales), 200
        else:
            logging.warning("No se generaron señales.")
            return jsonify({'mensaje': 'No se generaron señales'}), 200
    except Exception as e:
        logging.error(f"Error al generar señales: {e}")
        return jsonify({'error': 'Error interno al generar las señales'}), 500

# Ruta de Flask para validar las señales generadas (manual)
@app.route('/validar_senales', methods=['GET'])
def validar_senales():
    try:
        # Validar manualmente el estado de las señales
        signal_tracker.replicar_logica_senal_activa()
        return jsonify({'mensaje': 'Señales validadas correctamente'}), 200
    except Exception as e:
        logging.error(f"Error al validar señales: {e}")
        return jsonify({'error': 'Error interno al validar las señales'}), 500

# Ruta de Flask para el seguimiento de señales activas (manual)
@app.route('/seguimiento_senales', methods=['GET'])
def seguimiento_senales():
    try:
        # Ejecutar seguimiento de señales manualmente
        signal_tracker.replicar_logica_senal_activa()
        return jsonify({'mensaje': 'Señales replicadas y estado actualizado correctamente'}), 200
    except Exception as e:
        logging.error(f"Error durante el seguimiento de señales: {e}")
        return jsonify({'error': 'Error interno durante el seguimiento de las señales'}), 500

if __name__ == '__main__': 
    logging.info("Iniciando el servidor Flask para el generador y seguidor de señales de trading...")

    # Crear hilos para la generación y seguimiento automáticos
    hilo_generar_senales = threading.Thread(target=generar_senales_automaticamente, daemon=True)
    hilo_seguimiento_senales = threading.Thread(target=seguimiento_senales_automatico, daemon=True)

    # Iniciar los hilos
    hilo_generar_senales.start()
    hilo_seguimiento_senales.start()

    # Iniciar la aplicación Flask
    app.run(host='0.0.0.0', port=5007)
