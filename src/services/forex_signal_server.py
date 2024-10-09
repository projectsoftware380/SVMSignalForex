import os
import logging
import json
from flask import Flask, jsonify
import threading
from datetime import datetime
import pytz  # Para la zona horaria de Colombia
import argparse
import time

# Asegurarse de que Python puede encontrar los módulos
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

# Importar la clase ForexSignalAnalyzer
from src.senales.ForexSignalAnalyzer import ForexSignalAnalyzer

# Configuración del logger para el servidor
log_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'logs'))
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

log_file = os.path.join(log_dir, 'forex_signal_server.log')
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Cargar la configuración desde el archivo config.json
CONFIG_FILE = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'config', 'config.json'))
with open(CONFIG_FILE, "r", encoding='utf-8') as f:
    config = json.load(f)

# Crear la instancia de ForexSignalAnalyzer
db_config = config['db_config']
signal_analyzer = ForexSignalAnalyzer(db_config=db_config)

# Configuración de Flask
app = Flask(__name__)

def obtener_hora_colombia():
    """Obtiene el timestamp actual en la zona horaria de Colombia."""
    zona_colombia = pytz.timezone('America/Bogota')
    hora_actual_colombia = datetime.now(zona_colombia)
    return hora_actual_colombia.strftime('%Y-%m-%d %H:%M:%S')

def guardar_senales(resultados):
    """Guarda las señales en signals.json con el timestamp actual en hora colombiana."""
    try:
        signals_file = signal_analyzer.SIGNALS_FILE

        # Obtener el timestamp en hora colombiana
        timestamp_colombia = obtener_hora_colombia()

        # Agregar el timestamp al diccionario de resultados
        resultados['last_update'] = timestamp_colombia

        # Guardar las señales en signals.json
        with signal_analyzer.lock:
            with open(signals_file, 'w', encoding='utf-8') as f:
                json.dump(resultados, f, indent=4)
            logger.info(f"Señales guardadas en {signals_file} con timestamp {timestamp_colombia}.")
    except Exception as e:
        logger.error(f"Error al guardar las señales en {signals_file}: {e}")

@app.route('/get_signals', methods=['GET'])
def get_signals():
    """
    Endpoint para obtener las señales guardadas en signals.json.
    """
    try:
        signals_file = signal_analyzer.SIGNALS_FILE
        if not os.path.exists(signals_file):
            return jsonify({"error": "No se ha encontrado el archivo de señales."}), 404

        with open(signals_file, 'r', encoding='utf-8') as f:
            signals = json.load(f)

        logger.info("Señales obtenidas correctamente desde el archivo.")
        return jsonify(signals)
    except Exception as e:
        logger.error(f"Error al obtener las señales: {e}")
        return jsonify({"error": "Error al obtener las señales"}), 500

# Función para realizar el análisis inicial de señales al arrancar el servidor
def analizar_y_guardar_senales():
    try:
        # Ejecutar el análisis de señales inmediatamente al arrancar el servidor
        resultados = signal_analyzer.analizar_senales()
        logger.info("Señales analizadas al iniciar el servidor.")
        
        # Guardar los resultados de las señales en signals.json
        guardar_senales(resultados)
    except Exception as e:
        logger.error(f"Error en el análisis inicial de señales: {e}")

def ejecutar_analisis_periodico():
    """Ejecuta el análisis de señales periódicamente cada 3 minutos."""
    while True:
        try:
            tiempo_restante = signal_analyzer.tiempo_para_proxima_vela()
            logger.info(f"Esperando {tiempo_restante} segundos para la próxima vela.")
            time.sleep(tiempo_restante)

            # Ejecutar el análisis de señales
            logger.info("Iniciando análisis periódico de señales.")
            resultados = signal_analyzer.analizar_senales()

            # Guardar las señales en el archivo JSON
            guardar_senales(resultados)
        except Exception as e:
            logger.error(f"Error en el análisis periódico de señales: {e}")
            time.sleep(60)  # Esperar antes de reintentar

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Forex Signal Analyzer Server')
    parser.add_argument('--port', type=int, default=5003, help='Puerto para el servidor Flask')
    args = parser.parse_args()

    # Realizar el análisis de señales inmediatamente al iniciar
    analizar_y_guardar_senales()

    # Iniciar el hilo que ejecutará el análisis cada 3 minutos
    hilo_analisis = threading.Thread(target=ejecutar_analisis_periodico)
    hilo_analisis.daemon = True
    hilo_analisis.start()

    # Iniciar el servidor Flask
    logger.info("Iniciando el servidor de análisis de señales.")
    app.run(host='0.0.0.0', port=args.port, debug=False)
