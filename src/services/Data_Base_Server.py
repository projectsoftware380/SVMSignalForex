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

    def verificar_estado_mercado(self):
        """Consulta la API de Polygon para determinar si el mercado está abierto."""
        url = "https://api.polygon.io/v1/marketstatus/now"
        params = {"apiKey": "0E6O_kbTiqLJalWtmJmlGpTztFUFmmFR"}  # Reemplaza con tu API Key
        try:
            response = requests.get(url, params=params)
            data = response.json()

            # Verifica si el mercado de Forex ("fx") está abierto o cerrado
            estado_mercado = data.get("currencies", {}).get("fx", "")
            return estado_mercado == "open"  # True si el mercado está abierto, False si está cerrado
        except requests.RequestException as e:
            logging.error(f"Error al consultar el estado del mercado: {e}")
            return False

    def obtener_ultimo_timestamp(self, conn, pair, timeframe):
        """Obtiene el último timestamp almacenado en la base de datos."""
        cursor = conn.cursor()
        query = f"""
            SELECT MAX(timestamp) 
            FROM forex_data_{timeframe}  -- Tabla dinámica basada en el timeframe
            WHERE pair = %s
        """
        cursor.execute(query, (pair,))
        ultimo_timestamp = cursor.fetchone()[0]
        cursor.close()
        return ultimo_timestamp

    def obtener_penultimo_timestamp(self, conn, pair, timeframe):
        """Obtiene el penúltimo timestamp almacenado en la base de datos."""
        cursor = conn.cursor()
        query = f"""
            SELECT timestamp 
            FROM forex_data_{timeframe}  -- Tabla dinámica basada en el timeframe
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

    def insertar_datos(self, conn, datos, pair, timeframe):
        """Inserta los datos obtenidos en la tabla PostgreSQL en bloques y evita duplicados."""
        cursor = conn.cursor()

        # Determinar la tabla adecuada según el timeframe
        table_name = f'forex_data_{timeframe}'

        for result in datos.get("results", []):
            timestamp = result['t']  # Unix timestamp en milisegundos
            open_price = result['o']
            close_price = result['c']
            high_price = result['h']
            low_price = result['l']
            volume = result['v']

            # Inserta con manejo de duplicados, si ya existe no hace nada
            query = f"""
            INSERT INTO {table_name} (timestamp, pair, open, close, high, low, volume)
            VALUES (to_timestamp(%s / 1000.0), %s, %s, %s, %s, %s, %s)
            ON CONFLICT (timestamp, pair) DO NOTHING
            """
            cursor.execute(query, (timestamp, pair, open_price, close_price, high_price, low_price, volume))

        conn.commit()
        cursor.close()

    def obtener_datos_historicos(self, pairs, timeframe, retention_period_days):
        """Obtener datos históricos desde el último timestamp para un solo día."""
        multiplier, timeframe_api = self.ajustar_timeframe(timeframe)

        for pair in pairs:
            pair_formatted = pair.replace('-', '')
            conn = self.db_manager.conectar_db()
            if not conn:
                logging.error(f"No se pudo conectar a la base de datos para {pair}")
                continue

            # Solo un día de datos (el actual)
            end_date = datetime.now(timezone.utc)  # Aware datetime
            start_date = end_date - timedelta(days=retention_period_days)

            # Obtener el penúltimo timestamp en la base de datos
            penultimo_timestamp = self.obtener_penultimo_timestamp(conn, pair, timeframe)

            # Convertir 'penultimo_timestamp' a aware datetime si es naive
            if penultimo_timestamp is not None and penultimo_timestamp.tzinfo is None:
                # Asumimos que el timestamp almacenado está en UTC
                penultimo_timestamp = penultimo_timestamp.replace(tzinfo=timezone.utc)

            # Comparación entre ambos aware datetime
            if penultimo_timestamp and (end_date - penultimo_timestamp).total_seconds() < self.interval:
                logging.info(f"Datos actualizados para {pair} en timeframe {timeframe}. No se necesita actualización.")
                conn.close()
                continue

            # Obtener datos históricos de la API de Polygon
            datos = self.historical_fetcher.obtener_datos_polygon(
                pair_formatted,
                multiplier=multiplier,
                timeframe=timeframe_api,
                start_date=start_date.strftime('%Y-%m-%d'),
                end_date=end_date.strftime('%Y-%m-%d')
            )

            if datos and 'results' in datos:
                self.insertar_datos(conn, datos, pair, timeframe)
            else:
                logging.warning(f"No se obtuvieron datos para {pair} en timeframe {timeframe_api}.")

            conn.close()

    def ajustar_timeframe(self, timeframe):
        """Ajusta el timeframe y el multiplier para la API de Polygon."""
        if timeframe == '3m':
            return 3, 'minute'
        elif timeframe == '15m':
            return 15, 'minute'
        elif timeframe == '4h':
            return 4, 'hour'
        else:
            logging.error(f"Timeframe {timeframe} no reconocido.")
            return None, None

    def iniciar_proceso_periodico(self, pairs):
        """
        Inicia un ciclo continuo que obtiene los datos históricos y los actualiza cada 3 minutos.
        """
        self._running = True
        while not self._stop_event.is_set():
            # Verificar si el mercado está abierto
            mercado_abierto = self.verificar_estado_mercado()

            if mercado_abierto:
                logging.info("Mercado abierto, iniciando ciclo de actualización de datos históricos.")

                # Consultas para 3M, 15M y 4H
                self.obtener_datos_historicos(pairs, '3m', retention_period_days=1)  # 3 minutos, retención de 1 día
                self.obtener_datos_historicos(pairs, '15m', retention_period_days=15)  # 15 minutos, retención de 15 días
                self.obtener_datos_historicos(pairs, '4h', retention_period_days=180)  # 4 horas, retención de 6 meses

                logging.info("Base de datos actualizada correctamente.")

                # Esperar el tiempo normal del ciclo (3 minutos)
                logging.info(f"Ciclo de actualización completo. Esperando {self.interval // 60} minutos para el próximo ciclo.")
                self._stop_event.wait(self.interval)

            else:
                logging.info("Mercado cerrado. Verificación del estado del mercado en 5 minutos.")
                # Esperar 5 minutos y volver a verificar si el mercado se ha abierto
                self._stop_event.wait(self.market_check_interval)

        logging.info("Proceso de actualización detenido.")

    def iniciar(self, pairs):
        """
        Inicia el proceso completo de obtención de datos históricos.
        Ejecuta la actualización de los datos históricos en un ciclo continuo.
        """
        # Iniciar la obtención de datos históricos y eliminación en un hilo separado
        self._stop_event.clear()
        threading.Thread(target=self.iniciar_proceso_periodico, args=(pairs,)).start()

    def detener(self):
        """
        Detiene el ciclo continuo de actualización de datos.
        """
        self._stop_event.set()
        self._running = False
        logging.info("Deteniendo el proceso de actualización de datos.")

# Inicialización del servidor Flask
def iniciar_servidor(db_server):
    global data_base_server
    data_base_server = db_server
    # Iniciar automáticamente el servidor al arrancar
    data_base_server.iniciar(config.get("pairs"))
    app.run(host='0.0.0.0', port=5005)

# Inicialización del servidor de base de datos
if __name__ == '__main__':
    db_config = config["db_config"]
    api_key_polygon = config["api_key_polygon"]

    # Crear evento de sincronización
    db_sync_event = threading.Event()

    # Instancias necesarias
    db_manager = DatabaseManager(db_config)
    historical_fetcher = HistoricalDataFetcher(api_key=api_key_polygon)

    # Crear el servidor de base de datos con el intervalo de 3 minutos y el evento de sincronización
    data_base_server = DataBaseServer(db_manager, historical_fetcher, db_sync_event, interval=180)

    # Iniciar el servidor Flask con la instancia de base de datos
    iniciar_servidor(data_base_server)
