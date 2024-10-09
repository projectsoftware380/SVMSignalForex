from flask import Flask, jsonify
import threading
import logging
import os
import json
import requests
from datetime import datetime, timedelta, timezone
import sys

# Agrega el directorio base del proyecto al sys.path
project_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
sys.path.append(project_dir)

# Importar las clases necesarias
from src.data_processing.DatabaseManager import DatabaseManager
from src.data_processing.HistoricalDataFetcher import HistoricalDataFetcher

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
    def __init__(self, db_manager, historical_fetcher, db_sync_event, interval=180):
        self.db_manager = db_manager
        self.historical_fetcher = historical_fetcher
        self.db_sync_event = db_sync_event  # Evento para sincronizar la base de datos
        self.interval = interval  # Intervalo de ejecución en segundos (3 minutos = 180 segundos)
        self.market_check_interval = 300  # 5 minutos
        self._stop_event = threading.Event()
        self._running = False
        self.db_synced = False  # Bandera para activar el evento solo una vez
        self.actualizacion_status = {}  # Estado de la actualización para cada par

    def verificar_estado_mercado(self):
        """Consulta la API de Polygon para determinar si el mercado está abierto."""
        url = "https://api.polygon.io/v1/marketstatus/now"
        params = {"apiKey": config["api_key_polygon"]}
        try:
            response = requests.get(url, params=params)
            data = response.json()

            # Verifica si el mercado de Forex ("fx") está abierto o cerrado
            estado_mercado = data.get("currencies", {}).get("fx", "")
            return estado_mercado == "open"  # True si el mercado está abierto, False si está cerrado
        except requests.RequestException as e:
            logging.error(f"Error al consultar el estado del mercado: {e}")
            return False

    def obtener_ultimo_timestamp(self, conn, pair, timespan):
        """Obtiene el último timestamp almacenado en la base de datos."""
        cursor = conn.cursor()
        query = f"""
            SELECT MAX(timestamp) 
            FROM forex_data_{timespan}  -- Tabla dinámica basada en el timespan
            WHERE pair = %s
        """
        cursor.execute(query, (pair,))
        ultimo_timestamp = cursor.fetchone()[0]
        cursor.close()
        return ultimo_timestamp

    def obtener_penultimo_timestamp(self, conn, pair, timespan):
        """Obtiene el penúltimo timestamp almacenado en la base de datos."""
        cursor = conn.cursor()
        query = f"""
            SELECT timestamp 
            FROM forex_data_{timespan}  -- Tabla dinámica basada en el timespan
            WHERE pair = %s
            ORDER BY timestamp DESC
            LIMIT 2  -- Obtener los dos últimos datos
        """
        cursor.execute(query, (pair,))
        timestamps = cursor.fetchall()
        cursor.close()
        if len(timestamps) == 2:
            return timestamps[1][0]  # Devolver el penúltimo timestamp
        else:
            return None

    def verificar_actualizacion_tablas(self, conn, pair):
        """Verifica el estado de actualización de las tablas para un par."""
        timeframes = ['3m', '15m', '4h']
        current_time = datetime.now(timezone.utc)  # Usar timezone-aware datetime

        estado = {}
        for timespan in timeframes:
            ultimo_timestamp = self.obtener_ultimo_timestamp(conn, pair, timespan)
            if ultimo_timestamp:
                # Asegurarse de que `ultimo_timestamp` sea aware
                if ultimo_timestamp.tzinfo is None:
                    ultimo_timestamp = ultimo_timestamp.replace(tzinfo=timezone.utc)

                # Lógica de desfase según el timespan
                if timespan == '3m':
                    desfase_permitido = timedelta(minutes=6)
                elif timespan == '15m':
                    desfase_permitido = timedelta(minutes=30)
                elif timespan == '4h':
                    desfase_permitido = timedelta(hours=5)

                if current_time - ultimo_timestamp <= desfase_permitido:
                    estado[timespan] = True  # Tabla actualizada
                else:
                    estado[timespan] = False  # Tabla desactualizada
            else:
                estado[timespan] = False  # No hay datos en la tabla

        self.actualizacion_status[pair] = estado
        return estado

    def insertar_datos(self, conn, datos, pair, timespan):
        """Inserta los datos obtenidos en la tabla PostgreSQL en bloques de 5000 registros."""
        cursor = conn.cursor()

        # Determinar la tabla adecuada según el timespan
        table_name = f'forex_data_{timespan}'

        batch_size = 5000
        count = 0

        for result in datos.get("results", []):
            timestamp = result['t']
            open_price = result['o']
            close_price = result['c']
            high_price = result['h']
            low_price = result['l']
            volume = result['v']

            query = f"""
            INSERT INTO {table_name} (timestamp, pair, open, close, high, low, volume)
            VALUES (to_timestamp(%s / 1000.0), %s, %s, %s, %s, %s, %s)
            ON CONFLICT (timestamp, pair) DO NOTHING
            """
            cursor.execute(query, (timestamp, pair, open_price, close_price, high_price, low_price, volume))

            count += 1
            if count % batch_size == 0:
                conn.commit()

        conn.commit()
        cursor.close()

    def obtener_datos_historicos(self, pairs, timespan, retention_period_days):
        """Obtener datos históricos desde el último timestamp para un solo día."""
        multiplier, timespan_api = self.ajustar_timespan(timespan)

        for pair in pairs:
            pair_formatted = pair.replace('-', '')
            conn = self.db_manager.conectar_db()
            if not conn:
                logging.error(f"No se pudo conectar a la base de datos para {pair}")
                continue

            end_date = datetime.now(timezone.utc)
            start_date = end_date - timedelta(days=5)

            penultimo_timestamp = self.obtener_penultimo_timestamp(conn, pair, timespan)

            if penultimo_timestamp is not None and penultimo_timestamp.tzinfo is None:
                penultimo_timestamp = penultimo_timestamp.replace(tzinfo=timezone.utc)

            if penultimo_timestamp and (end_date - penultimo_timestamp).total_seconds() < self.interval:
                logging.info(f"Datos actualizados para {pair} en timespan {timespan}. No se necesita actualización.")
                conn.close()
                continue

            datos = self.historical_fetcher.obtener_datos_polygon(
                pair_formatted,
                multiplier=multiplier,
                timespan=timespan_api,
                start_date=start_date.strftime('%Y-%m-%d'),
                end_date=end_date.strftime('%Y-%m-%d')
            )

            if datos and 'results' in datos:
                logging.info(f"Cantidad de resultados obtenidos: {len(datos['results'])}")
                self.insertar_datos(conn, datos, pair, timespan)
            else:
                logging.warning(f"No se obtuvieron datos para {pair} en timespan {timespan_api}.")

            conn.close()

    def ajustar_timespan(self, timespan):
        """Ajusta el timespan y el multiplier para la API de Polygon."""
        if timespan == '3m':
            return 3, 'minute'
        elif timespan == '15m':
            return 15, 'minute'
        elif timespan == '4h':
            return 4, 'hour'
        else:
            logging.error(f"Timespan {timespan} no reconocido.")
            return None, None

    def iniciar_proceso_periodico(self, pairs):
        """
        Inicia un ciclo continuo que obtiene los datos históricos y los actualiza cada 3 minutos.
        """
        self._running = True
        while not self._stop_event.is_set():
            mercado_abierto = self.verificar_estado_mercado()

            if mercado_abierto:
                logging.info("Mercado abierto, iniciando ciclo de actualización de datos históricos.")

                # Consultas para 3M, 15M y 4H
                self.obtener_datos_historicos(pairs, '3m', retention_period_days=5)
                self.obtener_datos_historicos(pairs, '15m', retention_period_days=15)
                self.obtener_datos_historicos(pairs, '4h', retention_period_days=180)

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

    def iniciar(self, pairs):
        """Inicia el proceso completo de obtención de datos históricos."""
        self._stop_event.clear()
        threading.Thread(target=self.iniciar_proceso_periodico, args=(pairs,)).start()

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

    db_sync_event = threading.Event()

    db_manager = DatabaseManager(db_config)
    db_connection = db_manager.conectar_db()
    historical_fetcher = HistoricalDataFetcher(api_key=api_key_polygon, db_connection=db_connection)

    data_base_server = DataBaseServer(db_manager, historical_fetcher, db_sync_event, interval=180)

    iniciar_servidor(data_base_server)
