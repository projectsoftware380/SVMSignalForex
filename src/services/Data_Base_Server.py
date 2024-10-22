from flask import Flask, jsonify
import threading
import logging
import os
import json
import sys
import requests
from datetime import datetime, timedelta, timezone
from threading import Lock

# Añadir el directorio que contiene ForexData3mUpdater.py y DatabaseManager.py al sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data_processing')))

# Importar las clases necesarias
from ForexData3mUpdater import ForexData3mUpdater  # Clase para la actualización de forex_data_3m
from DatabaseManager import DatabaseManager  # Clase para la gestión de la base de datos
from HistoricalDataFetcher import HistoricalDataFetcher  # Clase para timeframe de 15m y 4h

# Cargar configuración desde config.json
def cargar_config():
    ruta_config = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.json')
    with open(ruta_config, 'r') as f:
        config = json.load(f)
    return config

# Cargar configuración
config = cargar_config()

# Configuración del logger directamente en el archivo
log_dir = os.path.join(os.path.dirname(__file__), '..', 'logs')
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

logging.basicConfig(
    filename=os.path.join(log_dir, 'DataBase.log'),
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)

# Instancia de Flask
app = Flask(__name__)

class DataBaseServer:
    def __init__(self, db_manager, forex_3m_updater, historical_fetcher, interval=190):
        self.db_manager = db_manager
        self.forex_3m_updater = forex_3m_updater  # Nueva clase para forex_data_3m
        self.historical_fetcher = historical_fetcher  # Peticiones REST para timeframe 15m y 4h
        self.interval = interval  # 3 minutos y 10 segundos
        self.market_check_interval = 300  # Verificar mercado cada 5 minutos
        self._stop_event = threading.Event()
        self._running = False
        self.actualizacion_status = {}
        self.lock = Lock()  # Bloqueo para evitar conflictos de concurrencia

    def verificar_estado_mercado(self):
        """Consulta la API de Polygon para determinar si el mercado está abierto."""
        url = "https://api.polygon.io/v1/marketstatus/now"
        params = {"apiKey": config["api_key_polygon"]}
        try:
            response = requests.get(url, params=params)
            data = response.json()
            estado_mercado = data.get("currencies", {}).get("fx", "")
            return estado_mercado == "open"
        except requests.RequestException as e:
            logging.error(f"Error al consultar el estado del mercado: {e}")
            return False

    def iniciar_actualizacion_3m(self):
        """Inicia el ciclo de actualización exclusivo para la tabla forex_data_3m."""
        logging.info("Iniciando actualización de forex_data_3m.")
        thread = threading.Thread(target=self.forex_3m_updater.iniciar)
        thread.start()

    def iniciar_proceso_periodico(self, pairs):
        """Inicia el ciclo de actualización con REST para 15m y 4h."""
        self._running = True
        while not self._stop_event.is_set():
            mercado_abierto = self.verificar_estado_mercado()

            if mercado_abierto:
                logging.info("Mercado abierto, iniciando peticiones REST para timeframes 15m y 4h.")

                with self.lock:  # Bloquear el proceso para evitar conflictos
                    # REST para timeframe 15m y 4h
                    logging.info("Iniciando proceso REST para timeframes 15m y 4h.")
                    self.historical_fetcher.iniciar_proceso(pairs, {'15m': 15, '4h': 4})

                    logging.info("Base de datos actualizada correctamente.")

                    for pair in pairs:
                        conn = self.db_manager.conectar_db()
                        if conn:
                            self.verificar_actualizacion_tablas(conn, pair)
                            conn.close()

                logging.info(f"Ciclo de actualización completo. Esperando {self.interval // 60} minutos para el próximo ciclo.")
                self._stop_event.wait(self.interval)

            else:
                logging.info("Mercado cerrado. Verificación del estado del mercado en 5 minutos.")
                self._stop_event.wait(self.market_check_interval)

        logging.info("Proceso de actualización detenido.")

    def verificar_estado_actualizacion(self):
        """Verifica si hay retraso significativo en la inserción de datos."""
        for pair in self.forex_3m_updater.pairs:
            max_timestamp = self.obtener_ultimo_timestamp(pair)
            if max_timestamp and (datetime.utcnow() - max_timestamp).total_seconds() > 180:
                logging.warning(f"Retraso significativo en la inserción de datos para {pair}. Último timestamp: {max_timestamp}")

    def verificar_actualizacion_tablas(self, conn, pair):
        """Verifica el estado de actualización de las tablas para un par."""
        timeframes = ['15m', '4h']  # Excluyendo 3m ya que se gestiona por separado
        current_time = datetime.now(timezone.utc)

        estado = {}
        for timespan in timeframes:
            ultimo_timestamp = self.obtener_ultimo_timestamp(conn, pair, timespan)
            if ultimo_timestamp:
                if ultimo_timestamp.tzinfo is None:
                    ultimo_timestamp = ultimo_timestamp.replace(tzinfo=timezone.utc)

                if timespan == '15m':
                    desfase_permitido = timedelta(minutes=30)
                elif timespan == '4h':
                    desfase_permitido = timedelta(hours=5)

                estado[timespan] = current_time - ultimo_timestamp <= desfase_permitido
            else:
                estado[timespan] = False

        self.actualizacion_status[pair] = estado
        return estado

    def obtener_ultimo_timestamp(self, conn=None, pair=None, timespan=None):
        """Obtiene el último timestamp almacenado en la base de datos."""
        if conn:
            cursor = conn.cursor()
            query = f"""
                SELECT MAX(timestamp)
                FROM forex_data_{timespan}
                WHERE pair = %s
            """
            cursor.execute(query, (pair,))
            ultimo_timestamp = cursor.fetchone()[0]
            cursor.close()
            return ultimo_timestamp

    def iniciar(self, pairs):
        """Inicia el proceso completo de actualización de datos."""
        self._stop_event.clear()
        threading.Thread(target=self.iniciar_proceso_periodico, args=(pairs,)).start()
        self.iniciar_actualizacion_3m()  # Inicia el ciclo separado de forex_data_3m

    def detener(self):
        """Detiene el ciclo continuo de actualización de datos."""
        self._stop_event.set()
        self._running = False
        logging.info("Deteniendo el proceso de actualización de datos.")

    @app.route('/status', methods=['GET'])
    def status():
        """Endpoint para obtener el estado de actualización de las tablas."""
        return jsonify(data_base_server.actualizacion_status)

# Inicialización del servidor Flask
def iniciar_servidor(db_server):
    global data_base_server
    data_base_server = db_server
    data_base_server.iniciar(config.get("pairs"))
    app.run(host='0.0.0.0', port=5005)

# Inicialización del servidor de base de datos
if __name__ == '__main__':
    db_config = config["db_config"]
    api_key_polygon = config["api_key_polygon"]

    db_manager = DatabaseManager(db_config)
    db_connection = db_manager.conectar_db()

    # Clase dedicada para forex_data_3m
    forex_3m_updater = ForexData3mUpdater(api_key_polygon, db_connection, pairs=config["pairs"])

    # REST para timeframes de 15 minutos y 4 horas
    historical_fetcher = HistoricalDataFetcher(api_key=api_key_polygon, db_connection=db_connection)

    data_base_server = DataBaseServer(db_manager, forex_3m_updater, historical_fetcher, interval=190)

    iniciar_servidor(data_base_server)
