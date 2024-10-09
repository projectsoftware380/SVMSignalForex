import argparse
import os
import logging
import json
from flask import Flask, jsonify, request
from datetime import datetime, timezone
import pytz  # Para obtener la hora en Colombia
import threading
import time
import sys

# Agregar la ruta al sistema para buscar los módulos en la estructura de carpetas.
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

# Importar la clase ForexAnalyzer desde su ubicación correcta
try:
    from src.tendencias.ForexAnalyzer import ForexAnalyzer
except ImportError as e:
    print(f"Error importando ForexAnalyzer: {e}")
    sys.exit(1)

# Configurar un logger específico para este servidor y guardarlo en `src/logs`
log_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'logs'))
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# Configurar logging
log_file = os.path.join(log_dir, 'forex_analyzer_server.log')
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Ruta relativa para el archivo de configuración config.json
CONFIG_FILE = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'config', 'config.json'))

# Cargar configuración desde config.json
if not os.path.exists(CONFIG_FILE):
    logger.error(f"El archivo de configuración {CONFIG_FILE} no fue encontrado.")
    raise FileNotFoundError(f"El archivo de configuración {CONFIG_FILE} no fue encontrado.")

with open(CONFIG_FILE, "r") as f:
    config = json.load(f)

# Instanciar ForexAnalyzer con la configuración de la base de datos y los pares
db_config = config["db_config"]
pairs = config["pairs"]

# Instanciar ForexAnalyzer
forex_analyzer = ForexAnalyzer(db_config=db_config, pairs=pairs)

# Instanciar Flask
app = Flask(__name__)

# Ruta del archivo JSON que almacenará las tendencias en `src/data/tendencias.json`
TENDENCIAS_FILE = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data', 'tendencias.json'))

def obtener_hora_colombia():
    """Obtiene la hora actual en la zona horaria de Colombia."""
    zona_colombia = pytz.timezone('America/Bogota')
    hora_actual_colombia = datetime.now(zona_colombia)
    return hora_actual_colombia.strftime('%Y-%m-%d %H:%M:%S')

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

        # Obtener el timestamp actual en hora colombiana
        timestamp = obtener_hora_colombia()

        # Leer tendencias existentes o inicializar si el archivo no existe
        with forex_analyzer.lock:
            if os.path.exists(TENDENCIAS_FILE):
                with open(TENDENCIAS_FILE, 'r') as f:
                    tendencias = json.load(f)
            else:
                tendencias = {}

            # Normalizar la tendencia y el par antes de guardarlos
            tendencia = forex_analyzer.normalizar_string(tendencia)
            par_normalizado = forex_analyzer.normalizar_string(pair)

            # Actualizar la tendencia y el timestamp del par analizado
            tendencias[par_normalizado] = {
                "tendencia": tendencia,
                "timestamp": timestamp
            }

            # Guardar las tendencias actualizadas en el archivo JSON
            guardar_tendencias(tendencias)

        logger.info(f"Tendencia guardada en JSON: {pair} -> {tendencia} at {timestamp}")

        # Devolver la tendencia y el timestamp como respuesta
        return jsonify({par_normalizado: tendencias[par_normalizado]})

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
        tendencias_calculadas = {}
        for pair in pares:
            tendencia = forex_analyzer.analizar_par(pair)
            timestamp = obtener_hora_colombia()
            tendencia = forex_analyzer.normalizar_string(tendencia)
            par_normalizado = forex_analyzer.normalizar_string(pair)

            tendencias_calculadas[par_normalizado] = {
                "tendencia": tendencia,
                "timestamp": timestamp
            }
            logger.info(f"Tendencia calculada: {par_normalizado} -> {tendencia} at {timestamp}")

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
                # Analizar todos los pares y guardar las tendencias con timestamp
                tendencias_calculadas = {}
                for pair in config["pairs"]:
                    tendencia = forex_analyzer.analizar_par(pair)
                    timestamp = obtener_hora_colombia()
                    tendencia = forex_analyzer.normalizar_string(tendencia)
                    par_normalizado = forex_analyzer.normalizar_string(pair)

                    tendencias_calculadas[par_normalizado] = {
                        "tendencia": tendencia,
                        "timestamp": timestamp
                    }
                    logger.info(f"Tendencia calculada: {par_normalizado} -> {tendencia} at {timestamp}")

                guardar_tendencias(tendencias_calculadas)
                logger.info("Análisis y guardado de tendencias completado.")

                # Esperar hasta la próxima vela de 4 horas
                tiempo_actual = datetime.now(timezone.utc)
                minutos_para_proxima_4h = (240 - ((tiempo_actual.hour % 4) * 60 + tiempo_actual.minute)) % 240
                segundos_para_proxima_4h = minutos_para_proxima_4h * 60 - tiempo_actual.second
                logger.info(f"Esperando {segundos_para_proxima_4h} segundos para la próxima vela de 4 horas.")
                time.sleep(segundos_para_proxima_4h)
            except Exception as e:
                logger.error(f"Error en sincronizar_con_nueva_vela: {str(e)}")
                time.sleep(60)  # Esperar antes de reintentar

    hilo_tendencias = threading.Thread(target=run_sincronizar)
    hilo_tendencias.daemon = True
    hilo_tendencias.start()

    # Iniciar el servidor Flask en el puerto especificado
    app.run(host='0.0.0.0', port=args.port, debug=False)
