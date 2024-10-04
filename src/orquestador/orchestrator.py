import subprocess
import time
import logging
import sys
import os
import json
from flask import Flask, jsonify
from threading import Thread, Event

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

LOG_FILE = os.path.join(log_dir, 'DataBase.log')  # Archivo de log de la base de datos

class ServerOrchestrator:
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

    def start_all_servers(self):
        """Inicia todos los servidores necesarios para el análisis."""
        logging.info("Esperando a que la base de datos esté completamente actualizada antes de iniciar los servidores.")
        self.db_sync_event.wait()  # Espera a que el evento de sincronización se active

        logging.info("Base de datos actualizada. Iniciando servidores de análisis.")
        # Iniciar el servidor de tendencias
        self.start_server("Tendencia", "python src/services/forex_analyzer_server.py")

        # Iniciar el resto de los servidores
        self.start_server("Reversion", "python src/services/forex_reversal_server.py")
        self.start_server("Señales", "python src/services/forex_signal_server.py")
        self.start_server("Patrones", "python src/services/candle_pattern_server.py")

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
orchestrator = ServerOrchestrator(config, db_sync_event)

# Función para iniciar el orquestador de la base de datos en un subproceso
def start_database_orchestrator():
    try:
        logging.info("Iniciando el orquestador de la base de datos.")
        # Ejecutar el script Data_Base_Server.py
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

if __name__ == '__main__':
    # Ejecutar la aplicación Flask en el puerto 5005
    app.run(host='0.0.0.0', port=5005)
