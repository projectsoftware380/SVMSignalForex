import subprocess
import time
import logging
import sys
import os
import json
from flask import Flask, jsonify
from threading import Thread
import requests

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

class ServerOrchestrator:
    def __init__(self, config):
        self.config = config
        self.servers = {}
        self.db_synced = False  # Flag para verificar si ya se ha detectado la sincronización
        self.actualizacion_status = None  # Estado de la actualización de la base de datos

    def start_server(self, name, command):
        """Inicia un servidor en un subproceso y muestra la salida en la consola."""
        try:
            process = subprocess.Popen(
                command,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True
            )
            self.servers[name] = process
            logging.info(f"Servidor {name} iniciado con el comando: {command}")

            # Mostrar la salida del servidor en la consola en tiempo real
            Thread(target=self._stream_output, args=(process, name)).start()

        except Exception as e:
            logging.error(f"Error al iniciar el servidor {name}: {e}")

    def _stream_output(self, process, name):
        """Captura la salida estándar y de error de un proceso y la muestra en la consola."""
        for line in iter(process.stdout.readline, ''):
            print(f"[{name} STDOUT]: {line.strip()}")
        for line in iter(process.stderr.readline, ''):
            print(f"[{name} STDERR]: {line.strip()}")

    def stop_servers(self):
        """Detiene todos los servidores iniciados."""
        for name, process in self.servers.items():
            process.terminate()  # Enviar señal de terminación
            logging.info(f"Servidor {name} detenido.")

    def consultar_estado_base_datos(self):
        """Consulta el estado de actualización de la base de datos."""
        try:
            response = requests.get("http://localhost:5005/status")
            if response.status_code == 200:
                self.actualizacion_status = response.json()
                logging.info(f"Estado de la actualización de la base de datos: {self.actualizacion_status}")
                # Verificar si todas las tablas están actualizadas para todos los pares
                for pair_status in self.actualizacion_status.values():
                    if not all(pair_status.values()):
                        return False  # Si alguna tabla no está actualizada, retornar False
                return True  # Todas las tablas están actualizadas
            else:
                logging.error(f"Error al consultar el estado de la base de datos: {response.status_code}")
                return False
        except Exception as e:
            logging.error(f"Error al realizar la consulta del estado de la base de datos: {e}")
            return False

    def start_all_servers(self):
        """Inicia todos los servidores necesarios para el análisis."""
        logging.info("Iniciando el servidor de base de datos.")
        # Iniciar el servidor de base de datos
        self.start_server("Data Base Server", "python src/services/Data_Base_Server.py")

        logging.info("Esperando a que la base de datos esté completamente actualizada antes de iniciar los servidores.")
        # Esperar a que el servidor de base de datos esté listo
        while not self.db_synced:
            time.sleep(10)  # Esperar unos segundos antes de intentar conectarse
            self.db_synced = self.consultar_estado_base_datos()

        logging.info("Base de datos actualizada. Iniciando servidores de análisis.")

        # Iniciar los demás servidores en el orden especificado
        self.start_server("Tendencia", "python src/services/forex_analyzer_server.py")  # Este es el servidor importante a agregar
        self.start_server("Reversion", "python src/services/forex_reversal_server.py")
        self.start_server("Señales", "python src/services/forex_signal_server.py")
        self.start_server("Patrones", "python src/services/candle_pattern_server.py")
        self.start_server("Servidor de Señales", "python src/services/TradingSignalServer.py")

    @app.route('/status', methods=['GET'])
    def status():
        """Devuelve el estado actual de los servidores y la base de datos."""
        return jsonify({
            'db_sync_status': self.db_synced,
            'actualizacion_status': self.actualizacion_status
        })

# Configuración de los servidores
config = {
    'tendencia_server': 'localhost:5001',
    'reversion_server': 'localhost:5002',
    'senal_server': 'localhost:5003',
    'patrones_server': 'localhost:5004',
    'trading_signal_server': 'localhost:5007'
}

# Inicializar el orquestador
orchestrator = ServerOrchestrator(config)

if __name__ == '__main__':
    logging.info("Iniciando el orquestador de servidores.")
    orchestrator.start_all_servers()
    # Ejecutar la aplicación Flask en el puerto 5006
    app.run(host='0.0.0.0', port=5006)
