import requests
import logging
import time
from datetime import datetime, timedelta
import psycopg2

class ForexData3mUpdater:
    def __init__(self, api_key, db_connection, pairs, interval=120, max_retries=3, retry_delay=5):
        self.api_key = api_key
        self.db_connection = db_connection
        self.pairs = pairs  # Lista de pares para actualizar
        self.interval = interval  # Intervalo de actualización en segundos
        self.max_retries = max_retries
        self.retry_delay = retry_delay

    def market_is_open(self):
        """Verifica si el mercado Forex está abierto."""
        # Lógica para verificar si el mercado está abierto
        # Esto podría ser una consulta a la API de Polygon o simplemente usando horarios conocidos de mercado.
        current_time = datetime.utcnow().time()
        # Ajustar el horario de apertura/cierre según las reglas del mercado Forex
        return current_time >= datetime.strptime('00:00', '%H:%M').time() and current_time <= datetime.strptime('23:59', '%H:%M').time()

    def obtener_ultimo_timestamp(self, pair):
        """Obtiene el último timestamp registrado en la base de datos para forex_data_3m."""
        cursor = self.db_connection.cursor()
        query = "SELECT MAX(timestamp) FROM forex_data_3m WHERE pair = %s"
        cursor.execute(query, (pair,))
        ultimo_timestamp = cursor.fetchone()[0]
        cursor.close()

        if not ultimo_timestamp:
            # Verificar si el mercado está abierto antes de usar un timestamp predeterminado
            return datetime.utcnow() - timedelta(minutes=5) if self.market_is_open() else None
        return ultimo_timestamp

    def obtener_datos_polygon(self, pair, start_time, end_time):
        """Obtiene los datos históricos de 3 minutos desde la API REST de Polygon.io."""
        # Asegurar que los tiempos se convierten a formato Unix timestamp
        start_time_unix = int(start_time.timestamp() * 1000)  # Convertir a Unix MS Timestamp
        end_time_unix = int(end_time.timestamp() * 1000)  # Convertir a Unix MS Timestamp

        url = f"https://api.polygon.io/v2/aggs/ticker/C:{pair}/range/3/minute/{start_time_unix}/{end_time_unix}?adjusted=true&sort=asc&apiKey={self.api_key}"
        
        for attempt in range(self.max_retries):
            try:
                response = requests.get(url)
                response.raise_for_status()
                data = response.json()
                return data.get('results', [])
            except requests.exceptions.RequestException as e:
                logging.error(f"Error al obtener datos para {pair} (intento {attempt + 1}): {e}")
                time.sleep(self.retry_delay)
        return []

    def insertar_datos(self, pair, datos):
        """Inserta los datos obtenidos en la tabla forex_data_3m."""
        if not datos:
            logging.info(f"No hay datos para insertar para {pair}")
            return

        cursor = self.db_connection.cursor()
        query = """
        INSERT INTO forex_data_3m (timestamp, pair, open, high, low, close, volume)
        VALUES (to_timestamp(%s), %s, %s, %s, %s, %s, %s)
        ON CONFLICT (timestamp, pair) DO NOTHING
        """
        
        for dato in datos:
            cursor.execute(query, (
                dato['t'] / 1000,  # Convertir timestamp a segundos
                pair,
                dato['o'],  # open
                dato['h'],  # high
                dato['l'],  # low
                dato['c'],  # close
                dato['v']   # volume
            ))
        self.db_connection.commit()
        cursor.close()
        logging.info(f"Datos insertados correctamente para {pair}.")

    def actualizar_tabla(self):
        """Actualiza la tabla forex_data_3m para todos los pares de divisas."""
        while True:
            for pair in self.pairs:
                ultimo_timestamp = self.obtener_ultimo_timestamp(pair)
                
                if not ultimo_timestamp:
                    logging.warning(f"No se pudo obtener un timestamp válido para {pair}. O el mercado está cerrado.")
                    continue

                end_time = datetime.utcnow()

                # Verificar si han pasado 3 minutos desde la última vela
                diferencia_minutos = (end_time - ultimo_timestamp).total_seconds() / 60

                if diferencia_minutos >= 3:
                    # Obtener y actualizar datos
                    datos = self.obtener_datos_polygon(pair, ultimo_timestamp, end_time)
                    self.insertar_datos(pair, datos)
                else:
                    logging.info(f"La vela para {pair} aún no ha cerrado. Última vela fue hace {diferencia_minutos:.2f} minutos.")

            logging.info(f"Actualización completada. Esperando {self.interval // 60} minutos para la próxima actualización.")
            time.sleep(self.interval)

    def iniciar(self):
        """Iniciar el proceso de actualización continua."""
        logging.info("Iniciando la actualización de forex_data_3m.")
        self.actualizar_tabla()
