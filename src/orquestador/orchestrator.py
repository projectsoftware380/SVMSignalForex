import subprocess
import time
import logging
import sys
import os
import json
from flask import Flask, jsonify
from threading import Thread, Event
from datetime import datetime, timedelta
import pytz

# Ajustar el sys.path para incluir el directorio base del proyecto
project_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(project_dir)

# Crear el directorio de logs en la carpeta 'logs'
log_dir = os.path.join(project_dir, 'logs')
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# Configurar logging para que el log se guarde en 'logs/orchestrator.log'
logging.basicConfig(
    filename=os.path.join(log_dir, 'orchestrator.log'),
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

app = Flask(__name__)

BUFFER_TIME = 30  # Buffer de 30 segundos antes de consultar la nueva vela
LOG_FILE = os.path.join(log_dir, 'DataBase.log')  # Archivo de log de la base de datos

class SignalOrchestrator:
    def __init__(self, config, db_sync_event):
        self.config = config
        self.servers = {}
        self.db_sync_event = db_sync_event  # Evento para sincronizar con la base de datos
        self.db_synced = False  # Flag para verificar si ya se ha detectado la sincronización

    def start_server(self, name, command):
        """Inicia un servidor en un subproceso."""
        try:
            process = subprocess.Popen(command, shell=True)
            self.servers[name] = process
            logging.info(f"Servidor {name} iniciado.")
        except Exception as e:
            logging.error(f"Error al iniciar el servidor {name}: {e}")

    def stop_servers(self):
        """Detiene todos los servidores iniciados."""
        for name, process in self.servers.items():
            process.terminate()  # Enviar señal de terminación
            logging.info(f"Servidor {name} detenido.")

    def monitor_log_file(self):
        """Monitorea el archivo de log de la base de datos y activa el evento de sincronización al detectar el mensaje."""
        logging.info("Monitoreando el archivo de log para detectar la actualización de la base de datos.")
        
        # Verificar si el archivo existe, si no, crearlo vacío
        if not os.path.exists(LOG_FILE):
            with open(LOG_FILE, 'w') as f:
                pass  # Crea el archivo vacío si no existe

        with open(LOG_FILE, 'r') as f:
            f.seek(0, os.SEEK_END)  # Mover el puntero al final del archivo

            while not self.db_synced:
                line = f.readline()
                if "Base de datos actualizada correctamente" in line:
                    logging.info("Se detectó la actualización de la base de datos en el archivo de log.")
                    self.db_sync_event.set()  # Activar el evento de sincronización
                    self.db_synced = True
                    break
                time.sleep(1)  # Esperar un segundo antes de volver a verificar

    def read_json_file(self, filepath):
        """Lee un archivo JSON y devuelve su contenido."""
        try:
            with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                data = json.load(f)
            logging.info(f"Archivo {filepath} leído correctamente.")
            return data
        except FileNotFoundError:
            logging.error(f"Archivo {filepath} no encontrado.")
            return {}
        except Exception as e:
            logging.error(f"Error al leer {filepath}: {e}")
            return {}

    def write_json_file(self, filepath, data):
        """Escribe el contenido en un archivo JSON."""
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=4, ensure_ascii=False)
            logging.info(f"Archivo {filepath} escrito correctamente.")
        except Exception as e:
            logging.error(f"Error al escribir {filepath}: {e}")

    def start_all_servers(self):
        """Inicia todos los servidores necesarios para el análisis."""
        logging.info("Esperando a que la base de datos esté completamente actualizada antes de iniciar los servidores de análisis.")
        self.db_sync_event.wait()  # Espera a que el evento de sincronización se active

        logging.info("Base de datos actualizada. Iniciando servidores de análisis.")
        # Iniciar el servidor de tendencias
        self.start_server("Tendencia", "python src/services/forex_analyzer_server.py")

        # Iniciar el resto de los servidores
        self.start_server("Reversion", "python src/services/forex_reversal_server.py")
        self.start_server("Señales", "python src/services/forex_signal_server.py")
        self.start_server("Patrones", "python src/services/candle_pattern_server.py")

    def generate_signals_every_3_minutes(self):
        """Ejecuta la generación de señales cada 3 minutos."""
        while True:
            try:
                logging.info("Iniciando la generación automática de señales cada 3 minutos.")
                self.generate_signals()
                logging.info("Señales generadas y guardadas. Esperando 3 minutos para la próxima ejecución.")
                time.sleep(180)  # Esperar 3 minutos
            except Exception as e:
                logging.error(f"Error en la generación automática de señales: {e}")
                time.sleep(60)  # Esperar 1 minuto antes de reintentar en caso de error

# Configuración de los servidores
config = {
    'tendencia_server': 'localhost:5001',
    'reversion_server': 'localhost:5002',
    'senal_server': 'localhost:5003',
    'patrones_server': 'localhost:5004'
}

# Crear el evento de sincronización
db_sync_event = Event()

# Inicializar el orquestador con el evento de sincronización
orchestrator = SignalOrchestrator(config, db_sync_event)

# Función para iniciar el orquestador de la base de datos en un subproceso
def start_database_orchestrator():
    try:
        logging.info("Iniciando el orquestador de la base de datos.")
        # Ejecutar el script Data_Base_Server.py en lugar de database_orchestrator.py
        subprocess.run("python src/services/Data_Base_Server.py", shell=True)
        logging.info("Orquestador de la base de datos finalizado correctamente.")
    except Exception as e:
        logging.error(f"Error al iniciar el orquestador de la base de datos: {e}")

# Iniciar el orquestador de la base de datos en un hilo separado
def start_database_thread():
    db_thread = Thread(target=start_database_orchestrator)
    db_thread.daemon = True
    db_thread.start()

# Función para monitorear el log de la base de datos en un hilo separado
def start_log_monitoring_thread():
    log_thread = Thread(target=orchestrator.monitor_log_file)
    log_thread.daemon = True
    log_thread.start()

start_database_thread()
start_log_monitoring_thread()

# Iniciar todos los servidores una vez que la base de datos esté lista
orchestrator.start_all_servers()

# Iniciar la generación automática de señales cada 3 minutos en un hilo separado
def start_signal_generation():
    thread = Thread(target=orchestrator.generate_signals_every_3_minutes)
    thread.daemon = True
    thread.start()

start_signal_generation()

@app.route('/generate_signals', methods=['GET'])
def generate_signals_endpoint():
    """Endpoint para generar señales bajo demanda."""
    signals = orchestrator.generate_signals()
    return jsonify(signals), 200

if __name__ == '__main__':
    # Ejecutar la aplicación Flask en el puerto 5005
    app.run(host='0.0.0.0', port=5005)
