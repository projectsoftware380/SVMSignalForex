import os
import logging
import json
from flask import Flask, jsonify
import threading
from datetime import datetime, timezone, timedelta
import argparse
import time
import sys
import psycopg2
from threading import Lock

# Asegurar que Python encuentre los módulos necesarios
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

# Importar la clase ForexSignalAnalyzer
from src.senales.ForexSignalAnalyzer import ForexSignalAnalyzer

# Configuración del logger para el servidor
log_dir = os.path.join(os.path.dirname(__file__), '..', 'logs')
os.makedirs(log_dir, exist_ok=True)

try:
    # Crear el logger
    logger = logging.getLogger('ForexSignalAnalyzer')
    logger.setLevel(logging.INFO)

    # Configurar manejador para guardar los logs
    file_handler = logging.FileHandler(os.path.join(log_dir, 'forex_signal_server.log'), encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    file_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_format)

    # Agregar el manejador al logger
    logger.addHandler(file_handler)

    logger.info("Logger configurado correctamente.")
except Exception as e:
    print(f"Error al configurar el logger: {e}")
    sys.exit(1)

# Cargar la configuración desde config.json
CONFIG_FILE = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.json')
with open(CONFIG_FILE, "r", encoding='utf-8') as f:
    config = json.load(f)

# Crear la instancia de ForexSignalAnalyzer
db_config = config['db_config']
signal_analyzer = ForexSignalAnalyzer(db_config=db_config)

# Configuración de Flask
app = Flask(__name__)

# Semáforo para evitar problemas de concurrencia
lock = Lock()

def obtener_hora_utc():
    """Obtiene el timestamp actual en UTC."""
    return datetime.now(timezone.utc)

@app.route('/get_signals', methods=['GET'])
def get_signals():
    """Endpoint para obtener las señales más recientes de la base de datos."""
    try:
        connection = signal_analyzer.obtener_conexion()
        if connection is None:
            raise ConnectionError("No se pudo conectar a la base de datos.")

        query = """
            SELECT timestamp, par_de_divisas, tipo_senal, origen, price_signal
            FROM senales
            ORDER BY timestamp DESC
            LIMIT 10;
        """
        with connection.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()

        signals = [
            {
                "timestamp": row[0].strftime('%Y-%m-%d %H:%M:%S'),
                "par_de_divisas": row[1],
                "tipo_senal": row[2],
                "origen": row[3],
                "price_signal": float(row[4])
            }
            for row in rows
        ]

        logger.info("Señales obtenidas correctamente.")
        return jsonify(signals)
    except Exception as e:
        logger.error(f"Error al obtener las señales: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500
    finally:
        if connection:
            connection.close()

def ejecutar_analisis_periodico():
    """Ejecuta el análisis de señales periódicamente cada 3 minutos."""
    while True:
        try:
            tiempo_restante = signal_analyzer.tiempo_para_proxima_vela()
            logger.info(f"Esperando {tiempo_restante:.2f} segundos para la próxima vela.")
            time.sleep(tiempo_restante)

            logger.info("Iniciando análisis periódico de señales.")
            resultados = signal_analyzer.analizar_senales()
            logger.info(f"Señales analizadas: {resultados}")

        except Exception as e:
            logger.error(f"Error en el análisis periódico: {e}", exc_info=True)
            time.sleep(60)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Forex Signal Analyzer Server')
    parser.add_argument('--port', type=int, default=5003, help='Puerto para el servidor Flask')
    args = parser.parse_args()

    # Verificar si los logs se están generando correctamente
    logger.info("Iniciando el servidor de análisis de señales.")

    # Iniciar el hilo para el análisis periódico
    hilo_analisis = threading.Thread(target=ejecutar_analisis_periodico, daemon=True)
    hilo_analisis.start()

    # Iniciar el servidor Flask
    try:
        app.run(host='0.0.0.0', port=args.port, debug=False)
    except Exception as e:
        logger.error(f"Error al iniciar el servidor Flask: {e}", exc_info=True)
        sys.exit(1)
