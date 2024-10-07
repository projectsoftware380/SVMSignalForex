import argparse
import os
import json
import logging
import threading
import time
from datetime import datetime, timezone
from flask import Flask, jsonify, request
import sys

# Añadir el directorio 'src' al sys.path para que Python lo encuentre
current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.abspath(os.path.join(current_dir, '..', '..'))
if src_dir not in sys.path:
    sys.path.append(src_dir)

from src.senales.ForexSignalAnalyzer import ForexSignalAnalyzer

# Verificar y crear el directorio de logs si no existe
log_dir = os.path.join(src_dir, 'src', 'logs')
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# Configurar logging para guardar en 'src/logs/forex_signal_server.log'
log_file = os.path.join(log_dir, 'forex_signal_server.log')
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Instanciar Flask
app = Flask(__name__)

# Ruta del archivo JSON que almacenará las señales
SIGNALS_FILE = os.path.join(src_dir, 'src', 'data', 'signals.json')

# Cargar configuración desde config.json (que contiene los pares y la configuración de la base de datos)
CONFIG_FILE = os.path.join(src_dir, 'src', 'config', 'config.json')
with open(CONFIG_FILE, "r") as f:
    config = json.load(f)

# Instanciar ForexSignalAnalyzer con la configuración de la base de datos
forex_signal_analyzer = ForexSignalAnalyzer(db_config=config["db_config"])

def guardar_senales(senales):
    """Guarda las señales en el archivo JSON, incluyendo el timestamp de la última actualización."""
    with forex_signal_analyzer.lock:
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        senales['last_update'] = timestamp  # Registrar el timestamp de la última actualización

        with open(SIGNALS_FILE, 'w') as f:
            json.dump(senales, f, indent=4)
        logger.info(f"Señales guardadas en el archivo JSON con timestamp {timestamp}.")

@app.route('/analyze', methods=['GET'])
def analyze_pair():
    """
    Endpoint para analizar manualmente y guardar las señales en el archivo JSON.
    """
    try:
        senales = forex_signal_analyzer.analizar_senales()
        guardar_senales(senales)
        return jsonify({"status": "success", "message": "Análisis completado y señales guardadas."})
    except Exception as e:
        logger.error(f"Error en el análisis manual: {str(e)}")
        return jsonify({"error": "Ocurrió un error durante el análisis."}), 500

@app.route('/signals', methods=['GET'])
def obtener_senales():
    """
    Endpoint para obtener todas las señales almacenadas en el archivo JSON.
    """
    try:
        # Verificar si el archivo de señales existe
        with forex_signal_analyzer.lock:
            if not os.path.exists(SIGNALS_FILE):
                return jsonify({"error": "No se ha encontrado el archivo de señales."}), 404

            # Leer y devolver las señales almacenadas en el archivo JSON
            with open(SIGNALS_FILE, 'r') as f:
                senales = json.load(f)

        return jsonify(senales)

    except Exception as e:
        logger.error(f"Error al obtener las señales: {str(e)}")
        return jsonify({"error": "Ocurrió un error al procesar la solicitud"}), 500

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Forex Signal Server')
    parser.add_argument('--port', type=int, default=5000, help='Puerto para el servidor Flask')
    args = parser.parse_args()

    # Sincronizar el cálculo de señales con la aparición de nuevas velas de 3 minutos
    def run_sincronizar():
        while True:
            try:
                # Realizar un análisis inmediato al iniciar
                senales = forex_signal_analyzer.analizar_senales()
                guardar_senales(senales)
                logger.info("Análisis y guardado de señales completado al iniciar.")

                tiempo_restante = 3 * 60  # Esperar 3 minutos para la próxima vela
                logger.info(f"Esperando {tiempo_restante} segundos para la próxima vela.")
                time.sleep(tiempo_restante)
            except Exception as e:
                logger.error(f"Error en sincronizar_con_nueva_vela: {str(e)}")
                time.sleep(60)  # Esperar antes de reintentar

    hilo_senales = threading.Thread(target=run_sincronizar)
    hilo_senales.daemon = True
    hilo_senales.start()

    # Iniciar el servidor Flask en el puerto especificado
    app.run(host='0.0.0.0', port=args.port, debug=False)
