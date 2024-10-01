import subprocess
import time
import logging
import sys
import os
import json
from flask import Flask, jsonify
from threading import Thread

# Ajustar el sys.path para incluir el directorio base del proyecto
project_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(project_dir)

# Crear el directorio de logs si no existe
log_dir = os.path.join(project_dir, 'src', 'logs')
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# Configurar logging para que el log se guarde en 'src/logs/orchestrator.log'
logging.basicConfig(
    filename=os.path.join(log_dir, 'orchestrator.log'),
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

app = Flask(__name__)

class SignalOrchestrator:
    def __init__(self, config):
        self.config = config
        self.servers = {}

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

    def read_json_file(self, filepath):
        """Lee un archivo JSON y devuelve su contenido."""
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
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

    def generate_signals(self):
        """Genera señales basadas en los archivos de tendencias, reversiones, señales y patrones."""
        try:
            # Leer los archivos JSON
            tendencias = self.read_json_file('src/data/tendencias.json')
            reversiones = self.read_json_file('src/data/reversiones.json')
            signals = self.read_json_file('src/data/signals.json')
            patrones = self.read_json_file('src/data/candle_patterns.json')

            # Generar las señales basadas en las coincidencias
            generated_signals = []
            for pair, tendencia in tendencias.items():
                if pair in reversiones and pair in signals and pair in patrones:
                    reversion = reversiones.get(pair, {})
                    senal = signals.get(pair, {})
                    patron = patrones.get(pair, {})

                    # Combinaciones de señales
                    if tendencia and reversion and senal:
                        generated_signals.append({
                            "pair": pair,
                            "tipo": "Tipo 1",  # Tendencia + Reversión + Señal
                            "riesgo": "100%"
                        })
                    if tendencia and reversion and '3m' in patron:
                        generated_signals.append({
                            "pair": pair,
                            "tipo": "Tipo 2",  # Tendencia + Reversión + Patrón (3 minutos)
                            "riesgo": "100%"
                        })
                    if tendencia and senal:
                        generated_signals.append({
                            "pair": pair,
                            "tipo": "Tipo 3",  # Tendencia + Señal
                            "riesgo": "50%"
                        })
                    if tendencia and ('4h' in patron or '15m' in patron):
                        generated_signals.append({
                            "pair": pair,
                            "tipo": "Tipo 4",  # Tendencia + Patrón (4h o 15m)
                            "riesgo": "50%"
                        })

            # Escribir las señales generadas en generated_signals.json
            self.write_json_file('src/data/generated_signals.json', generated_signals)
            logging.info("Señales generadas y guardadas en generated_signals.json.")

            return generated_signals

        except Exception as e:
            logging.error(f"Error al generar señales: {e}")
            return []

    def start_all_servers(self):
        """Inicia todos los servidores necesarios para el análisis."""
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

orchestrator = SignalOrchestrator(config)

# Iniciar todos los servidores
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
