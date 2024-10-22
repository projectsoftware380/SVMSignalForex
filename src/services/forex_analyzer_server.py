import argparse
import os
import logging
import json
import pandas as pd
from flask import Flask, jsonify, request
from sqlalchemy.sql import text
from datetime import datetime, timezone, timedelta
import pytz
import threading
import time
import sys

# Agregar la ruta al sistema para los módulos
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

try:
    from src.tendencias.ForexAnalyzer import ForexAnalyzer
except ImportError as e:
    print(f"Error importando ForexAnalyzer: {e}")
    sys.exit(1)

# Configuración del logger
log_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'logs'))
os.makedirs(log_dir, exist_ok=True)

log_file = os.path.join(log_dir, 'forex_analyzer_server.log')
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Cargar configuración
CONFIG_FILE = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'config', 'config.json'))
if not os.path.exists(CONFIG_FILE):
    logger.error(f"Archivo de configuración no encontrado: {CONFIG_FILE}")
    raise FileNotFoundError(f"No se encontró el archivo de configuración: {CONFIG_FILE}")

with open(CONFIG_FILE, "r") as f:
    config = json.load(f)

db_config = config["db_config"]
pairs = config["pairs"]

# Instancia de ForexAnalyzer
forex_analyzer = ForexAnalyzer(db_config=db_config, pairs=pairs)

app = Flask(__name__)

def obtener_hora_colombia():
    """Obtiene la hora actual en la zona horaria de Colombia."""
    zona_colombia = pytz.timezone('America/Bogota')
    return datetime.now(zona_colombia).strftime('%Y-%m-%d %H:%M:%S')

def calcular_segundos_para_proxima_4h(timestamp):
    """Calcula los segundos restantes para la próxima vela de 4 horas."""
    tiempo_actual = datetime.now(timezone.utc)
    proxima_4h = (tiempo_actual + timedelta(hours=4 - tiempo_actual.hour % 4)).replace(
        minute=0, second=0, microsecond=0
    )
    segundos_restantes = (proxima_4h - tiempo_actual).total_seconds()
    return max(int(segundos_restantes), 0)

@app.route('/tendencias', methods=['GET'])
def get_tendencias():
    """Endpoint para obtener las últimas tendencias registradas."""
    try:
        limite = int(request.args.get('limit', 10))  # Límite por defecto: 10

        query = text("""
            SELECT timestamp, par_de_divisas, tipo_tendencia, precio_actual
            FROM tendencias
            ORDER BY timestamp DESC
            LIMIT :limite;
        """)

        with forex_analyzer.engine.connect() as conn:
            result = conn.execute(query, {"limite": limite}).fetchall()

        tendencias = [
            {
                "timestamp": row[0].strftime('%Y-%m-%d %H:%M:%S'),
                "par_de_divisas": row[1],
                "tipo_tendencia": row[2],
                "precio_actual": float(row[3]),
            }
            for row in result
        ]

        return jsonify(tendencias), 200

    except Exception as e:
        logger.error(f"Error al obtener tendencias: {str(e)}", exc_info=True)
        return jsonify({"error": "Error al obtener las tendencias"}), 500

@app.route('/analyze', methods=['GET'])
def analyze_pair():
    """Endpoint para analizar un par de divisas y registrar su tendencia."""
    try:
        pair = request.args.get('pair')
        if not pair or pair not in pairs:
            return jsonify({"error": "Par inválido o no proporcionado"}), 400

        tendencia = forex_analyzer.analizar_par(pair)
        if not tendencia:
            return jsonify({"error": "No se pudo determinar la tendencia"}), 204

        timestamp = obtener_hora_colombia()
        precio_cierre, _ = forex_analyzer.obtener_precio_cierre(pair)

        if tendencia not in [None, "Neutral"]:
            forex_analyzer.registrar_tendencia(pair, tendencia, precio_cierre, timestamp)
            return jsonify({pair: {"tendencia": tendencia, "timestamp": timestamp}})
        else:
            return jsonify({"message": f"Tendencia neutral para {pair}."}), 204

    except Exception as e:
        logger.error(f"Error al analizar el par {pair}: {str(e)}")
        return jsonify({"error": "Ocurrió un error al procesar la solicitud"}), 500

def calcular_y_guardar_tendencia_inicial():
    """Calcula y registra la tendencia inicial para el último timestamp."""
    timestamp, close_price = forex_analyzer.obtener_ultimo_timestamp_y_close()
    if timestamp and close_price:
        for pair in pairs:
            try:
                forex_analyzer.analizar_par(pair)
                logger.info(f"Tendencia inicial registrada para {pair} en {timestamp}.")
            except Exception as e:
                logger.error(f"Error al analizar {pair} en la inicialización: {str(e)}")

def sincronizar_con_nuevas_velas():
    """Sincroniza las tendencias con las nuevas velas de 4 horas."""
    while True:
        try:
            timestamp_reciente, _ = forex_analyzer.obtener_ultimo_timestamp_y_close()
            segundos_para_proxima_4h = calcular_segundos_para_proxima_4h(timestamp_reciente)

            if segundos_para_proxima_4h == 0:
                logger.info("Nueva vela detectada. Calculando tendencias.")
                forex_analyzer.analizar_pares()
                time.sleep(10)
            else:
                logger.info(f"Esperando {segundos_para_proxima_4h} segundos para la próxima vela.")
                time.sleep(segundos_para_proxima_4h)
        except Exception as e:
            logger.error(f"Error en sincronización: {str(e)}")
            time.sleep(60)

def iniciar_proceso():
    """Inicia el proceso de cálculo inicial y sincronización de tendencias."""
    calcular_y_guardar_tendencia_inicial()
    sincronizar_con_nuevas_velas()

# Iniciar hilo de sincronización en segundo plano
hilo_tendencias = threading.Thread(target=iniciar_proceso, daemon=True)
hilo_tendencias.start()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Forex Analyzer Server')
    parser.add_argument('--port', type=int, default=5000, help='Puerto para el servidor Flask')
    args = parser.parse_args()

    app.run(host='0.0.0.0', port=args.port, debug=False)
