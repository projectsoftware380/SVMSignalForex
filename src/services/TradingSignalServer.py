import os
import logging
from flask import Flask, jsonify, request, abort
import threading
import sys
import time
import psycopg2
import json
from functools import wraps
from datetime import datetime, timezone, timedelta

# Ajuste para encontrar las clases SignalManager y SignalTracker
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
    try:
        with open(config_file, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        logging.error(f"Archivo de configuración no encontrado: {config_file}")
        sys.exit(1)
    except json.JSONDecodeError as e:
        logging.error(f"Error al decodificar JSON en {config_file}: {e}")
        sys.exit(1)

config = cargar_configuracion(CONFIG_FILE)
db_config = config.get("db_config", {})

# Función para crear una nueva conexión a la base de datos PostgreSQL
def conectar_db():
    try:
        conn = psycopg2.connect(
            host=db_config.get('host'),
            database=db_config.get('database'),
            user=db_config.get('user'),
            password=db_config.get('password')
        )
        logging.info("Conexión a la base de datos PostgreSQL exitosa.")
        return conn
    except psycopg2.OperationalError as e:
        logging.error(f"Error al conectar a la base de datos: {e}")
        return None
    except psycopg2.Error as e:
        logging.error(f"Error de base de datos: {e}")
        return None

# Directorio de datos (donde están los archivos JSON)
DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')

# Inicialización del SignalManager con logging
signal_manager = SignalManager(DATA_DIR, CONFIG_FILE, logger=logging)

# Decorador para requerir API key en los endpoints
AUTHORIZED_API_KEYS = ["12345", "6789"]
def require_api_key(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        api_key = request.headers.get('X-API-Key') or request.args.get('api_key')
        if api_key and api_key in AUTHORIZED_API_KEYS:
            return f(*args, **kwargs)
        else:
            logging.warning("Acceso denegado debido a API Key inválida o ausente.")
            abort(401)  # No autorizado
    return decorated

# Función para generar señales automáticamente en un hilo
def generar_senales_automaticamente():
    while True:
        try:
            senales = signal_manager.generar_senales()
            if senales:
                logging.info(f"Señales generadas y guardadas automáticamente: {senales}")
            else:
                logging.warning("No se generaron señales automáticamente.")
        except Exception as e:
            logging.error(f"Error al generar señales automáticamente: {e}", exc_info=True)
        time.sleep(180)  # Espera de 3 minutos antes de volver a generar

# Función para el seguimiento y la inactivación de señales automáticamente en un hilo
def seguimiento_senales_automatico():
    while True:
        conn = conectar_db()
        if conn is None:
            logging.error("No se pudo establecer conexión con la base de datos.")
            time.sleep(180)
            continue
        try:
            signal_tracker = SignalTracker(conn, logger=logging)
            # Verifica inactivación de señales con más de 1 hora de activas
            signal_tracker.replicar_logica_senal_activa()  # Verifica inactivación
            logging.info("Señales replicadas y estado actualizado automáticamente.")
        except Exception as e:
            logging.error(f"Error durante el seguimiento automático de señales: {e}", exc_info=True)
        finally:
            conn.close()
        time.sleep(180)  # Espera de 3 minutos antes de volver a ejecutar el seguimiento

# Ruta de Flask para que los clientes obtengan solo las señales activas de tracked_signals
@app.route('/get_signal', methods=['GET'])
@require_api_key  # Requiere autenticación
def get_signal():
    conn = conectar_db()
    if conn is None:
        return jsonify({'error': 'No se pudo conectar a la base de datos'}), 500
    try:
        with conn.cursor() as cursor:
            cursor.execute("""SELECT id, par, tipo, accion, timestamp, timeframe_operacion, estado, timestamp_actual
                              FROM tracked_signals
                              WHERE estado = 'activa'
                              ORDER BY timestamp DESC;""")
            results = cursor.fetchall()
            if results:
                colnames = [desc[0] for desc in cursor.description]
                signals = {}
                active_signals = []
                for row in results:
                    signal = dict(zip(colnames, row))
                    
                    # Convertir las fechas si es necesario
                    if isinstance(signal['timestamp'], str):
                        signal['timestamp'] = datetime.fromisoformat(signal['timestamp'])
                    if isinstance(signal['timestamp_actual'], str):
                        signal['timestamp_actual'] = datetime.fromisoformat(signal['timestamp_actual'])
                    
                    # Inactivar señales activas por más de 60 minutos
                    if signal['timestamp_actual'] - signal['timestamp'] > timedelta(minutes=60):
                        cursor.execute("""UPDATE tracked_signals
                                          SET estado = 'inactiva'
                                          WHERE id = %s""", (signal['id'],))
                        conn.commit()
                        logging.info(f"Señal inactivada: {signal['id']}")
                    else:
                        # Evitar duplicados basados en el par (símbolo)
                        if signal['par'] not in signals:
                            signals[signal['par']] = signal
                            active_signals.append(signal)
                        else:
                            # Si existe una señal más reciente, inactivar la antigua
                            cursor.execute("""UPDATE tracked_signals
                                              SET estado = 'inactiva'
                                              WHERE id = %s""", (signals[signal['par']]['id'],))
                            conn.commit()
                            logging.info(f"Señal anterior inactivada para el par: {signal['par']}")

                return jsonify(active_signals), 200
            else:
                return jsonify({'mensaje': 'No se encontraron señales activas'}), 404
    except Exception as e:
        logging.error(f"Error al obtener las señales activas: {e}", exc_info=True)
        return jsonify({'error': 'Error interno al obtener las señales activas'}), 500
    finally:
        conn.close()

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
