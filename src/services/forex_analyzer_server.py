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

from src.tendencias.ForexAnalyzer import ForexAnalyzer

# Verificar y crear el directorio de logs si no existe
log_dir = os.path.join(src_dir, 'src', 'logs')
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# Configurar logging para guardar en 'src/logs/forex_analyzer_server.log'
log_file = os.path.join(log_dir, 'forex_analyzer_server.log')
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Instanciar Flask
app = Flask(__name__)

# Ruta del archivo JSON que almacenará las tendencias
TENDENCIAS_FILE = os.path.join(src_dir, 'src', 'data', 'tendencias.json')

# Cargar configuración desde config.json (que contiene los pares y la configuración de la base de datos)
CONFIG_FILE = os.path.join(src_dir, 'src', 'config', 'config.json')
with open(CONFIG_FILE, "r") as f:
    config = json.load(f)

# Instanciar ForexAnalyzer con la configuración de la base de datos y los pares
forex_analyzer = ForexAnalyzer(db_config=config["db_config"], pairs=config["pairs"])

def guardar_tendencias(tendencias):
    """Guarda las tendencias en el archivo JSON."""
    with forex_analyzer.lock:
        with open(TENDENCIAS_FILE, 'w') as f:
            json.dump(tendencias, f, indent=4)
        logger.info("Tendencias guardadas en el archivo JSON.")

@app.route('/analyze', methods=['GET'])
def analyze_pair():
    """
    Endpoint para analizar un par manualmente y guardar la tendencia en un archivo JSON.
    """
    try:
        pair = request.args.get('pair', None)
        if pair is None or pair not in config["pairs"]:
            return jsonify({"error": "Par inválido o no proporcionado"}), 400

        # Analizar el par utilizando ForexAnalyzer
        tendencia = forex_analyzer.analizar_par(pair)

        # Leer tendencias existentes o inicializar si el archivo no existe
        with forex_analyzer.lock:
            if os.path.exists(TENDENCIAS_FILE):
                with open(TENDENCIAS_FILE, 'r') as f:
                    tendencias = json.load(f)
            else:
                tendencias = {}

            # Actualizar la tendencia del par analizado
            tendencias[pair] = tendencia

            # Guardar las tendencias actualizadas en el archivo JSON
            guardar_tendencias(tendencias)

        logger.info(f"Tendencia guardada en JSON: {pair} -> {tendencia}")

        # Devolver la tendencia como respuesta
        return jsonify({pair: tendencia})

    except Exception as e:
        logger.error(f"Error al analizar el par: {str(e)}")
        return jsonify({"error": "Ocurrió un error al procesar la solicitud"}), 500

@app.route('/analyze_all', methods=['GET'])
def analyze_all_pairs():
    """
    Endpoint para analizar todos los pares del archivo config.json y guardar las tendencias.
    También devuelve las tendencias calculadas como respuesta.
    """
    try:
        pares = config.get("pairs", [])
        if not pares:
            return jsonify({"error": "No se encontraron pares en el archivo de configuración"}), 400

        # Analizar todos los pares
        tendencias_calculadas = forex_analyzer.analizar_pares()

        # Guardar todas las tendencias en el archivo JSON
        guardar_tendencias(tendencias_calculadas)

        logger.info("Todas las tendencias han sido actualizadas y guardadas.")

        # Devolver las tendencias calculadas como respuesta
        return jsonify(tendencias_calculadas)

    except Exception as e:
        logger.error(f"Error al analizar todos los pares: {str(e)}")
        return jsonify({"error": "Ocurrió un error al procesar la solicitud"}), 500

@app.route('/tendencias', methods=['GET'])
def obtener_tendencias():
    """
    Endpoint para obtener todas las tendencias almacenadas en el archivo JSON.
    """
    try:
        # Verificar si el archivo de tendencias existe
        with forex_analyzer.lock:
            if not os.path.exists(TENDENCIAS_FILE):
                return jsonify({"error": "No se ha encontrado el archivo de tendencias."}), 404

            # Leer y devolver las tendencias almacenadas en el archivo JSON
            with open(TENDENCIAS_FILE, 'r') as f:
                tendencias = json.load(f)

        return jsonify(tendencias)

    except Exception as e:
        logger.error(f"Error al obtener las tendencias: {str(e)}")
        return jsonify({"error": "Ocurrió un error al procesar la solicitud"}), 500

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Forex Analyzer Server')
    parser.add_argument('--port', type=int, default=5000, help='Puerto para el servidor Flask')
    args = parser.parse_args()

    # Sincronizar el cálculo de tendencias con la aparición de nuevas velas de 4 horas
    def run_sincronizar():
        while True:
            try:
                # Realizar un análisis inmediato al iniciar
                forex_analyzer.analizar_pares()
                guardar_tendencias(forex_analyzer.last_trend)
                logger.info("Análisis y guardado de tendencias completado al iniciar.")

                tiempo_restante = 4 * 3600  # Esperar 4 horas
                logger.info(f"Esperando {tiempo_restante} segundos para la próxima vela.")
                time.sleep(tiempo_restante)
            except Exception as e:
                logger.error(f"Error en sincronizar_con_nueva_vela: {str(e)}")
                time.sleep(60)  # Esperar antes de reintentar

    hilo_tendencias = threading.Thread(target=run_sincronizar)
    hilo_tendencias.daemon = True
    hilo_tendencias.start()

    # Iniciar el servidor Flask en el puerto especificado
    app.run(host='0.0.0.0', port=args.port, debug=False)
