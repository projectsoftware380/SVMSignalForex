import requests
import logging
import psycopg2
import time
from datetime import datetime, timedelta, timezone
import json
import websocket
import threading
import os

# Configuración del logger
log_dir = os.path.join(os.path.dirname(__file__), '..', 'logs')
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

logging.basicConfig(
    filename=os.path.join(log_dir, 'DataBase.log'),
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)

class WebSocketDataFetcher:
    def __init__(self, db_config, api_key, pairs, websocket_url="wss://socket.polygon.io/forex", retention_days=30):
        self.db_config = db_config
        self.api_key = api_key
        self.pairs = pairs  # Lista de pares a seguir
        self.websocket_url = websocket_url
        self.retention_days = retention_days
        self.db_connection = None
        self.ws = None  # Mantener la conexión del WebSocket activa
        self.stop_event = threading.Event()

    def conectar_db(self):
        """Conectar a la base de datos con reintentos."""
        max_retries = 3
        retry_delay = 5
        for attempt in range(max_retries):
            try:
                conn = psycopg2.connect(
                    host=self.db_config["host"],
                    database=self.db_config["database"],
                    user=self.db_config["user"],
                    password=self.db_config["password"]
                )
                logging.info("Conexión exitosa a la base de datos.")
                return conn
            except Exception as error:
                logging.error(f"Intento {attempt + 1} - Error al conectar a la base de datos: {error}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                else:
                    logging.error("No se pudo establecer conexión después de varios intentos.")
                    return None

    def obtener_ultimo_timestamp(self):
        """Obtener el último timestamp registrado en la tabla forex_data_3m."""
        conn = self.conectar_db()
        if conn:
            cursor = conn.cursor()
            query = f"SELECT MAX(timestamp) FROM forex_data_3m WHERE pair = %s;"
            cursor.execute(query, (self.pairs[0],))  # Cambiado para trabajar con la lista de pares
            ultimo_timestamp = cursor.fetchone()[0]
            cursor.close()
            conn.close()
            return ultimo_timestamp
        return None

    def obtener_datos_faltantes(self):
        """Obtener los datos faltantes desde el último timestamp usando la API REST."""
        ultimo_timestamp = self.obtener_ultimo_timestamp()
        if not ultimo_timestamp:
            # Si no hay datos, solicitar los últimos 60 días
            start_date = (datetime.utcnow() - timedelta(days=60)).strftime('%Y-%m-%d')
        else:
            start_date = ultimo_timestamp.strftime('%Y-%m-%d')

        end_date = datetime.utcnow().strftime('%Y-%m-%d')
        base_url = f"https://api.polygon.io/v2/aggs/ticker/C:{self.pairs[0]}/range/3/minute/{start_date}/{end_date}?apiKey={self.api_key}"

        response = requests.get(base_url)
        if response.status_code == 200:
            return response.json().get('results', [])
        else:
            logging.error(f"Error al obtener datos faltantes: {response.text}")
            return []

    def insertar_datos(self, datos):
        """Insertar los datos obtenidos en la tabla forex_data_3m."""
        conn = self.conectar_db()
        if conn:
            cursor = conn.cursor()
            query = f"""
            INSERT INTO forex_data_3m (timestamp, pair, open, close, high, low, volume)
            VALUES (to_timestamp(%s), %s, %s, %s, %s, %s, %s)
            ON CONFLICT (timestamp, pair) DO NOTHING;
            """
            for record in datos:
                cursor.execute(query, (
                    record['t'] / 1000,  # Convertir a segundos
                    self.pairs[0],  # Usar el primer par
                    record['o'],
                    record['c'],
                    record['h'],
                    record['l'],
                    record['v']
                ))
            conn.commit()
            cursor.close()
            conn.close()
            logging.info(f"Datos insertados correctamente para {self.pairs[0]}.")

    def procesar_mensaje(self, ws, mensaje):
        """Procesar los datos recibidos en tiempo real por el WebSocket y almacenarlos."""
        datos = json.loads(mensaje)
        if 'ev' in datos and datos['ev'] == 'CA':  # 'CA' es el evento de agregados de tiempo real
            self.insertar_datos([datos])

    def eliminar_datos_antiguos(self):
        """Eliminar datos más antiguos que el tiempo de retención (30 días por defecto)."""
        conn = self.conectar_db()
        if conn:
            try:
                cursor = conn.cursor()
                query = f"""
                DELETE FROM forex_data_3m
                WHERE timestamp < NOW() - INTERVAL '{self.retention_days} days';
                """
                cursor.execute(query)
                conn.commit()
                logging.info(f"Datos antiguos eliminados (más de {self.retention_days} días).")
            except psycopg2.Error as e:
                logging.error(f"Error al eliminar datos antiguos: {e}")
                conn.rollback()
            finally:
                cursor.close()
                conn.close()

    def iniciar_websocket(self):
        """Iniciar conexión WebSocket para recibir datos en tiempo real."""
        def on_message(ws, mensaje):
            self.procesar_mensaje(ws, mensaje)

        def on_error(ws, error):
            logging.error(f"Error en WebSocket: {error}")

        def on_close(ws, close_status_code, close_msg):
            logging.info(f"Conexión WebSocket cerrada con código: {close_status_code}, motivo: {close_msg}")
            self.reconnect_websocket()  # Intentar reconectar automáticamente

        def on_open(ws):
            # Suscribir a todos los pares definidos en el archivo config.json
            subscribe_message = {
                "action": "subscribe",
                "params": ",".join([f"C.C:{pair}" for pair in self.pairs])  # Suscribirse a todos los pares
            }
            ws.send(json.dumps(subscribe_message))
            logging.info(f"Suscrito a los pares: {self.pairs}")

        self.ws = websocket.WebSocketApp(
            self.websocket_url,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
            on_open=on_open
        )

        self.ws.run_forever()

    def reconnect_websocket(self):
        """Intentar reconectar al WebSocket."""
        logging.info("Intentando reconectar al WebSocket...")
        time.sleep(5)  # Esperar antes de reconectar
        self.iniciar_websocket()

    def sincronizar_datos_faltantes(self):
        """Sincronizar datos faltantes antes de activar el WebSocket."""
        datos_faltantes = self.obtener_datos_faltantes()
        if datos_faltantes:
            self.insertar_datos(datos_faltantes)
        else:
            logging.info(f"No se encontraron datos faltantes para {self.pairs[0]}.")

    def iniciar(self):
        """Iniciar la sincronización, eliminar datos antiguos y activar el WebSocket."""
        self.eliminar_datos_antiguos()
        self.sincronizar_datos_faltantes()
        logging.info(f"Iniciando WebSocket para {self.pairs}.")
        self.iniciar_websocket()
