import requests
import logging
import os
import time
from datetime import datetime, timedelta
import psycopg2

# Configuración del logger directamente en el archivo
log_dir = os.path.join(os.path.dirname(__file__), '..', 'logs')
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

logging.basicConfig(
    filename=os.path.join(log_dir, 'DataBase.log'),
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)

class HistoricalDataFetcher:
    def __init__(self, api_key, db_connection, max_retries=3, retry_delay=5, batch_size=500):
        self.api_key = api_key
        self.db_connection = db_connection
        self.max_retries = max_retries  # Número máximo de reintentos
        self.retry_delay = retry_delay  # Intervalo entre reintentos (segundos)
        self.batch_size = batch_size  # Tamaño de los lotes de inserción en la base de datos

    def obtener_ultimo_registro(self, pair):
        """Obtiene el timestamp más reciente de la base de datos para el par específico."""
        query = "SELECT MAX(timestamp) FROM forex_data_3m WHERE pair = %s;"
        cursor = self.db_connection.cursor()
        cursor.execute(query, (pair,))
        resultado = cursor.fetchone()[0]
        cursor.close()
        return resultado

    def obtener_datos_polygon(self, pair, multiplier=3, timespan='minute', start_date=None, end_date=None, adjusted=True, sort='desc', limit=5000):
        """Obtiene datos históricos desde la API de Polygon.io para un rango de fechas de 5 días con paginación si es necesario."""
        if not start_date:
            end_date = datetime.utcnow().strftime('%Y-%m-%d')
            start_date = (datetime.utcnow() - timedelta(days=5)).strftime('%Y-%m-%d')

        base_url = f"https://api.polygon.io/v2/aggs/ticker/C:{pair}/range/{multiplier}/{timespan}/{start_date}/{end_date}?apiKey={self.api_key}"
        params = {
            "adjusted": str(adjusted).lower(),
            "sort": sort,
            "limit": limit
        }

        logging.info(f"Haciendo solicitud a: {base_url} con params {params}")
        all_data = []

        for attempt in range(self.max_retries):
            try:
                start_time = time.time()

                response = requests.get(base_url, params=params)
                response.raise_for_status()

                response_time = time.time() - start_time
                logging.info(f"Solicitud completada en {response_time:.2f} segundos.")

                data = response.json()
                if 'results' in data and data['results']:
                    all_data.extend(data['results'])
                    logging.info(f"Datos obtenidos de Polygon.io para {pair} en timespan {timespan} desde {start_date} hasta {end_date}.")

                    return {'results': all_data}
                else:
                    logging.warning(f"No se encontraron resultados en la respuesta para {pair}. Datos: {data}")
                    return None

            except requests.exceptions.RequestException as e:
                logging.error(f"Intento {attempt + 1} - Error al obtener datos de Polygon.io: {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)
                else:
                    logging.error("No se pudo obtener datos de Polygon.io después de varios intentos.")
                    return None

    def insertar_datos(self, datos, pair, timespan):
        """Procesa los datos y los inserta en la base de datos."""
        if not datos or not datos.get('results'):
            logging.warning(f"No hay datos para insertar para {pair} en timespan {timespan}.")
            return
        
        ultimo_timestamp = self.obtener_ultimo_registro(pair)
        if ultimo_timestamp:
            logging.info(f"El último registro para {pair} es {ultimo_timestamp}")
            nuevo_timestamp = datos['results'][-1]['t'] / 1000
            if nuevo_timestamp <= ultimo_timestamp:
                logging.info(f"Los datos más recientes ya están en la base de datos para {pair}. No se insertan nuevos datos.")
                return

        try:
            cursor = self.db_connection.cursor()
            batch = []
            for record in datos['results']:
                timestamp = record['t'] / 1000
                query = """
                INSERT INTO forex_data_3m (pair, timestamp, open, high, low, close, volume)
                VALUES (%s, to_timestamp(%s), %s, %s, %s, %s, %s)
                ON CONFLICT (pair, timestamp) DO NOTHING
                """
                batch.append((pair, timestamp, record['o'], record['h'], record['l'], record['c'], record['v']))

                if len(batch) >= self.batch_size:
                    cursor.executemany(query, batch)
                    self.db_connection.commit()
                    batch = []

            if batch:
                cursor.executemany(query, batch)
                self.db_connection.commit()

            logging.info(f"Datos insertados correctamente en la base de datos para {pair}.")
        except psycopg2.Error as e:
            logging.error(f"Error al insertar datos en la base de datos para {pair}: {e}")
            self.db_connection.rollback()
        finally:
            cursor.close()

    def iniciar_proceso(self, pairs, timespan):
        """Proceso principal para obtener y guardar los datos."""
        for pair in pairs:
            datos = self.obtener_datos_polygon(pair, timespan=timespan)
            if datos:
                self.insertar_datos(datos, pair, timespan)
            else:
                logging.warning(f"No se obtuvieron datos para {pair} en timespan {timespan}.")
