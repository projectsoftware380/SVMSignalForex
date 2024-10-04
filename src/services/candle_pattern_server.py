import sys
import os
import json
import logging
import threading
import time
from flask import Flask, jsonify
from datetime import datetime, timedelta, timezone
import pytz

# Ajustar la ruta de sys.path para asegurar que los módulos correctos sean accesibles
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'src', 'CandlePattern')))

# Importar la clase CandlePatternAnalyzer correctamente
from CandlePatternDetector import CandlePatternAnalyzer

# Configuración básica de logging
log_dir = os.path.join(os.path.dirname(__file__), '..', 'logs')
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

logging.basicConfig(
    filename=os.path.join(log_dir, 'candle_pattern_server.log'),
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Cargar configuración desde config.json
CONFIG_FILE = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.json')
with open(CONFIG_FILE, "r") as f:
    config = json.load(f)

# Inicializar Flask
app = Flask(__name__)

# Inicializar el analizador de patrones de velas con los parámetros de la base de datos desde config.json
db_config = config["db_config"]
analyzer = CandlePatternAnalyzer(db_config)

# Definir la ubicación del archivo candle_patterns.json
CANDLE_PATTERNS_FILE = os.path.join(os.path.dirname(__file__), '..', 'data', 'candle_patterns.json')

# Función para obtener la fecha y hora en Colombia
def obtener_hora_colombia():
    zona_colombia = pytz.timezone('America/Bogota')
    hora_actual = datetime.now(zona_colombia)
    return hora_actual.strftime('%Y-%m-%d %H:%M:%S')

# Guardar los patrones detectados en un archivo JSON, incluyendo la fecha y hora del último análisis
def guardar_patrones_en_json(patrones):
    try:
        timestamp_colombia = obtener_hora_colombia()  # Obtener la hora en Colombia
        patrones['last_timestamp'] = timestamp_colombia  # Agregar la hora al archivo JSON

        # Guardar el archivo JSON
        with open(CANDLE_PATTERNS_FILE, 'w', encoding='utf-8') as f:
            json.dump(patrones, f, indent=4, ensure_ascii=False)
        logging.info(f"Patrones guardados en {CANDLE_PATTERNS_FILE} con timestamp {timestamp_colombia}.")
    except Exception as e:
        logging.error(f"Error al guardar los patrones en {CANDLE_PATTERNS_FILE}: {e}")

# Función para calcular el tiempo restante hasta la próxima vela de 3 minutos
def tiempo_para_proxima_vela():
    ahora = datetime.now(timezone.utc)  # Usar datetime.now con timezone-aware UTC
    proxima_vela = ahora.replace(second=0, microsecond=0, minute=(ahora.minute // 3) * 3) + timedelta(minutes=3)
    return (proxima_vela - ahora).total_seconds()

# Función para ejecutar el análisis de patrones de velas automáticamente cada 3 minutos
def ejecutar_analisis_automatico():
    while True:
        try:
            # Calcular el tiempo hasta la próxima vela de 3 minutos
            tiempo_restante = tiempo_para_proxima_vela()
            logging.info(f"Esperando {tiempo_restante} segundos para la próxima vela de 3 minutos.")
            time.sleep(tiempo_restante)

            # Ejecutar el análisis de patrones de velas
            logging.info("Iniciando análisis de patrones con nueva vela de 3 minutos.")
            pairs = config['pairs']
            patrones_detectados = {}

            for pair in pairs:
                patrones = analyzer.detectar_patrones_para_par(pair)
                patrones_detectados[pair] = patrones if patrones else "No se detectaron patrones"

            # Guardar los patrones en el archivo JSON
            guardar_patrones_en_json(patrones_detectados)
            logging.info("Análisis de patrones completado y guardado.")
        except Exception as e:
            logging.error(f"Error en el análisis de patrones automático: {e}")
            time.sleep(60)  # Esperar antes de intentar nuevamente en caso de error

# Iniciar el análisis de patrones en un hilo en segundo plano
def iniciar_hilo_analisis():
    hilo_analisis = threading.Thread(target=ejecutar_analisis_automatico)
    hilo_analisis.daemon = True
    hilo_analisis.start()

# Endpoint para obtener los patrones almacenados
@app.route('/patterns', methods=['GET'])
def obtener_patrones():
    try:
        with open(CANDLE_PATTERNS_FILE, 'r', encoding='utf-8') as f:
            patrones = json.load(f)
        logging.info("Patrones obtenidos correctamente.")
        return jsonify(patrones)
    except FileNotFoundError:
        logging.warning(f"Archivo {CANDLE_PATTERNS_FILE} no encontrado.")
        return jsonify({})
    except Exception as e:
        logging.error(f"Error al obtener los patrones: {e}")
        return jsonify({"error": "No se pudieron obtener los patrones"}), 500

# Iniciar el servidor
if __name__ == '__main__':
    try:
        logging.info("Iniciando el servidor de detección de patrones de velas...")
        # Iniciar el análisis en segundo plano
        iniciar_hilo_analisis()

        # Iniciar la aplicación Flask
        app.run(host='0.0.0.0', port=5004)
    except Exception as e:
        logging.error(f"Error al iniciar el servidor: {e}")
