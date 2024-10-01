import sys
import os
import json
import logging
import threading
import time
from flask import Flask, jsonify
from datetime import datetime, timedelta
import pytz

# Agrega la ruta del proyecto a sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

# Importa la clase ForexSignalAnalyzer desde la carpeta 'senales'
from src.senales.ForexSignalAnalyzer import ForexSignalAnalyzer

# Configuración básica de logging
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
    with open(CONFIG_FILE, "r") as f:
        config = json.load(f)
        logging.info("Configuración cargada correctamente.")
except Exception as e:
    logging.error(f"Error al cargar el archivo de configuración: {e}")
    raise

# Inicializar el analizador de señales
try:
    forex_signal_analyzer = ForexSignalAnalyzer(api_key_polygon=config['api_key_polygon'])
    logging.info("ForexSignalAnalyzer inicializado correctamente.")
except Exception as e:
    logging.error(f"Error al inicializar ForexSignalAnalyzer: {e}")
    raise

# Definir la ubicación del archivo signals.json
SIGNALS_FILE = os.path.join(os.path.dirname(__file__), '..', 'data', 'signals.json')

# Guardar señales en archivo JSON con la hora de Colombia
def guardar_senales_en_json(senales):
    try:
        # Obtener la hora actual en Colombia
        zona_colombia = pytz.timezone('America/Bogota')
        hora_colombia = datetime.now(zona_colombia).strftime('%Y-%m-%d %H:%M:%S')
        senales['last_timestamp'] = hora_colombia

        # Guardar las señales en el archivo JSON
        with open(SIGNALS_FILE, 'w') as f:
            json.dump(senales, f, indent=4, ensure_ascii=False)
        logging.info(f"Señales guardadas en {SIGNALS_FILE} con timestamp {hora_colombia}")
    except Exception as e:
        logging.error(f"Error al guardar las señales en {SIGNALS_FILE}: {e}")

# Crear la aplicación Flask
app = Flask(__name__)

# Endpoint para obtener las señales almacenadas
@app.route('/signals', methods=['GET'])
def obtener_senales():
    try:
        with open(SIGNALS_FILE, 'r') as f:
            senales = json.load(f)
        logging.info("Señales obtenidas correctamente.")
        return jsonify(senales)
    except FileNotFoundError:
        logging.warning(f"Archivo {SIGNALS_FILE} no encontrado.")
        return jsonify({})
    except Exception as e:
        logging.error(f"Error al obtener las señales: {e}")
        return jsonify({"error": "No se pudieron obtener las señales"}), 500

# Endpoint para calcular las señales manualmente para todos los pares
@app.route('/analyze_signals', methods=['GET'])
def analizar_senales():
    try:
        pares_a_analizar = config['pairs']
        senales = {}

        for pair in pares_a_analizar:
            senal = forex_signal_analyzer.analizar_senal_para_par(pair)
            if senal:
                senales[pair] = senal

        guardar_senales_en_json(senales)
        logging.info("Señales calculadas correctamente.")
        return jsonify(senales)
    except Exception as e:
        logging.error(f"Error al analizar las señales: {e}")
        return jsonify({"error": "Ocurrió un error al analizar las señales"}), 500

# Función para calcular el tiempo restante hasta la próxima vela de 3 minutos
def tiempo_para_proxima_vela():
    ahora = datetime.now(pytz.UTC)  # Usar datetime.now(pytz.UTC) en lugar de utcnow()
    proxima_vela = ahora.replace(second=0, microsecond=0, minute=(ahora.minute // 3) * 3) + timedelta(minutes=3)
    return (proxima_vela - ahora).total_seconds()

# Función para ejecutar el análisis de señales cada 3 minutos
def ejecutar_analisis_automatico():
    while True:
        try:
            # Calcular el tiempo hasta la próxima vela de 3 minutos
            tiempo_restante = tiempo_para_proxima_vela()
            logging.info(f"Esperando {tiempo_restante} segundos para la próxima vela de 3 minutos.")
            time.sleep(tiempo_restante)

            # Ejecutar el análisis de señales
            logging.info("Iniciando análisis de señales con nueva vela de 3 minutos.")
            pares_a_analizar = config['pairs']
            senales = {}

            for pair in pares_a_analizar:
                senal = forex_signal_analyzer.analizar_senal_para_par(pair)
                if senal:
                    senales[pair] = senal

            # Guardar las señales en el archivo JSON
            guardar_senales_en_json(senales)
            logging.info("Análisis de señales completado y guardado.")
        except Exception as e:
            logging.error(f"Error en el análisis de señales automático: {e}")
            time.sleep(60)  # Esperar antes de intentar nuevamente en caso de error

# Iniciar el análisis de señales en un hilo en segundo plano
def iniciar_hilo_analisis():
    hilo_analisis = threading.Thread(target=ejecutar_analisis_automatico)
    hilo_analisis.daemon = True
    hilo_analisis.start()

# Iniciar el servidor
if __name__ == '__main__':
    try:
        logging.info("Iniciando el servidor de señales en el puerto 5002...")
        # Iniciar el análisis en segundo plano
        iniciar_hilo_analisis()

        # Iniciar la aplicación Flask
        app.run(port=5002)
    except Exception as e:
        logging.error(f"Error al iniciar el servidor: {e}")
