import os
import logging
import threading
import sys
import psycopg2
import json
from flask import Flask, jsonify, request, abort
from functools import wraps
from time import sleep

# Configuración del directorio raíz y PATH
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
sys.path.append(ROOT_DIR)

from src.SignalManager.SignalManager import SignalManager
from src.SignalManager.SignalValidator import SignalValidator

# Configuración del logger
LOG_DIR = os.path.join(os.path.dirname(__file__), '..', 'logs')
os.makedirs(LOG_DIR, exist_ok=True)

LOG_FILE_SERVER = os.path.join(LOG_DIR, 'trading_signal_server.log')
logging.basicConfig(
    filename=LOG_FILE_SERVER,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Inicialización de la aplicación Flask
app = Flask(__name__)
CONFIG_FILE = os.path.abspath(os.path.join(ROOT_DIR, 'src', 'config', 'config.json'))

def cargar_configuracion():
    """Carga la configuración desde un archivo JSON."""
    try:
        with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        logging.error(f"Error al cargar config.json: {e}")
        sys.exit(1)

config = cargar_configuracion()
db_config = config.get("db_config", {})
loop_interval = config.get("loop_interval", 180)  # Intervalo de 3 minutos

def conectar_db():
    """Establece una conexión segura a la base de datos PostgreSQL."""
    try:
        conn = psycopg2.connect(
            host=db_config['host'],
            database=db_config['database'],
            user=db_config['user'],
            password=db_config['password'],
            options='-c client_encoding=UTF8'
        )
        logging.info("Conexión a la base de datos PostgreSQL exitosa.")
        return conn
    except psycopg2.Error as e:
        logging.error(f"Error al conectar a la base de datos: {e}")
        return None

def obtener_sentimiento_desde_db(symbol):
    """Obtiene el sentimiento del mercado desde la base de datos."""
    conn = conectar_db()
    if not conn:
        logging.error(f"No se pudo conectar a la base de datos para obtener sentimiento de {symbol}.")
        return None

    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT sentimiento 
                FROM market_sentiments 
                WHERE symbol = %s 
                ORDER BY created_at DESC LIMIT 1;
            """, (symbol,))
            result = cursor.fetchone()
            if result:
                return result[0]
            else:
                logging.info(f"No se encontraron sentimientos para {symbol}.")
                return None
    except psycopg2.Error as e:
        logging.error(f"Error al obtener sentimiento de {symbol}: {e}", exc_info=True)
        return None
    finally:
        conn.close()

# Inicialización de clases con sus respectivos loggers
logger = logging.getLogger('SignalManager')
validator = SignalValidator(db_config, logger=logger)
signal_manager = SignalManager(CONFIG_FILE, logger=logger)

# Lista de API Keys autorizadas
AUTHORIZED_API_KEYS = ["12345", "67890"]

def require_api_key(f):
    """Valida la API Key en las solicitudes."""
    @wraps(f)
    def decorated(*args, **kwargs):
        api_key = request.headers.get('X-API-Key') or request.args.get('api_key')
        if api_key and api_key in AUTHORIZED_API_KEYS:
            return f(*args, **kwargs)
        else:
            logging.warning("Acceso denegado por API Key inválida.")
            abort(401)
    return decorated

@app.route('/get_signal', methods=['GET'])
@require_api_key
def get_signal():
    """Endpoint para obtener todas las señales generadas en el último ciclo."""
    conn = conectar_db()
    if not conn:
        return jsonify({'error': 'No se pudo conectar a la base de datos'}), 500

    try:
        with conn.cursor() as cursor:
            # Obtener el timestamp del último ciclo
            cursor.execute("""
                SELECT MAX(timestamp) AS ultimo_ciclo
                FROM generated_signals;
            """)
            ultimo_ciclo = cursor.fetchone()[0]

            if not ultimo_ciclo:
                return jsonify({'mensaje': 'No se encontraron señales'}), 404

            # Obtener todas las señales generadas dentro del último ciclo
            cursor.execute("""
                SELECT id, par_de_divisas, tipo, accion, timestamp, 
                       timeframe, price_signal
                FROM generated_signals
                WHERE timestamp >= %s - INTERVAL '180 seconds'
                ORDER BY timestamp DESC, par_de_divisas;
            """, (ultimo_ciclo,))

            results = cursor.fetchall()

            if results:
                colnames = [desc[0] for desc in cursor.description]
                signals = [dict(zip(colnames, row)) for row in results]
                return jsonify(signals), 200
            else:
                return jsonify({'mensaje': 'No se encontraron señales'}), 404
    except psycopg2.Error as e:
        logging.error(f"Error al obtener señales: {e}", exc_info=True)
        return jsonify({'error': 'Error interno'}), 500
    finally:
        conn.close()

@app.route('/get_market_sentiment', methods=['GET'])
@require_api_key
def get_market_sentiment():
    """Endpoint para obtener el sentimiento del mercado desde la base de datos."""
    symbol = request.args.get('symbol')
    if not symbol:
        return jsonify({'error': 'El parámetro "symbol" es obligatorio.'}), 400

    sentimiento = obtener_sentimiento_desde_db(symbol)
    if sentimiento:
        return jsonify({'symbol': symbol, 'sentimiento': sentimiento}), 200
    else:
        return jsonify({'mensaje': f'No se encontraron sentimientos para {symbol}'}), 404

def iniciar_procesamiento():
    """Inicia el procesamiento secuencial de señales en un ciclo continuo."""
    while True:
        inicio = threading.current_thread().name  # Información de hilo para logs
        try:
            logging.info(f"Iniciando procesamiento en {inicio}.")
            signal_manager.procesar_registros()  # Procesa señales secuencialmente

            logging.info(f"Esperando {loop_interval} segundos para el próximo ciclo.")
            sleep(loop_interval)  # Espera antes del próximo ciclo

        except Exception as e:
            logging.error(f"Error en el procesamiento de señales: {e}", exc_info=True)
            sleep(10)  # Retraso breve en caso de error

if __name__ == '__main__':
    logging.info("Iniciando el servidor Flask...")

    # Iniciar el procesamiento de señales en un hilo separado
    hilo_procesar = threading.Thread(target=iniciar_procesamiento, daemon=True, name="HiloProcesamiento")
    hilo_procesar.start()

    # Ejecutar la aplicación Flask
    app.run(host='0.0.0.0', port=5007)

