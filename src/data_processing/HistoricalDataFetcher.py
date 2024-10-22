import requests
import logging
import time
from datetime import datetime, timedelta
import psycopg2
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

class HistoricalDataFetcher:
    def __init__(self, api_key, db_connection, max_retries=3, retry_delay=5, batch_size=500):
        self.api_key = api_key
        self.db_connection = db_connection
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.batch_size = batch_size

    def obtener_ultimo_registro(self, pair, timeframe):
        """Obtiene el timestamp más reciente de la base de datos para el par específico."""
        table_name = f'forex_data_{timeframe}'
        query = f"SELECT MAX(timestamp) FROM {table_name} WHERE pair = %s;"
        cursor = self.db_connection.cursor()
        cursor.execute(query, (pair,))
        resultado = cursor.fetchone()[0]
        cursor.close()

        if resultado is None:
            logging.info(f"No se encontraron registros previos para {pair} en {timeframe}.")
        return resultado

    def mapear_timespan(self, timeframe):
        """Mapea los timeframes como '15m', '4h' a los valores aceptados por Polygon.io."""
        if timeframe == '15m':
            return ('minute', 15)
        elif timeframe == '4h':
            return ('hour', 4)
        else:
            logging.error(f"Timespan no válido: {timeframe}")
            return None, None

    def obtener_datos_polygon_batch(self, pairs, timespan, start_date=None, end_date=None, adjusted=True, sort='desc', limit=5000):
        """Obtiene datos históricos desde la API de Polygon.io en un rango de fechas para múltiples pares."""
        if not start_date:
            end_date = datetime.utcnow().strftime('%Y-%m-%d')
            start_date = (datetime.utcnow() - timedelta(days=30)).strftime('%Y-%m-%d')

        timespan_str, multiplier = self.mapear_timespan(timespan)
        if not timespan_str:
            return None

        datos_por_par = {}
        for pair in pairs:
            base_url = f"https://api.polygon.io/v2/aggs/ticker/C:{pair}/range/{multiplier}/{timespan_str}/{start_date}/{end_date}?adjusted={str(adjusted).lower()}&sort={sort}&limit={limit}&apiKey={self.api_key}"
            logging.info(f"Haciendo solicitud para {pair} a: {base_url}")

            all_data = []
            next_url = base_url

            while next_url:
                for attempt in range(self.max_retries):
                    try:
                        start_time = time.time()
                        response = requests.get(next_url)
                        response.raise_for_status()

                        response_time = time.time() - start_time
                        logging.info(f"Solicitud completada para {pair} en {response_time:.2f} segundos.")

                        data = response.json()
                        if 'results' in data and data['results']:
                            all_data.extend(data['results'])
                            logging.info(f"Datos obtenidos de Polygon.io para {pair} en timespan {timespan} desde {start_date} hasta {end_date}.")

                            next_url = data.get('next_url')
                            if next_url:
                                next_url += f"&apiKey={self.api_key}"
                            else:
                                break
                        else:
                            logging.warning(f"No se encontraron más resultados en la respuesta para {pair}.")
                            break

                    except requests.exceptions.RequestException as e:
                        logging.error(f"Intento {attempt + 1} - Error al obtener datos de Polygon.io para {pair}: {e}")
                        if attempt < self.max_retries - 1:
                            time.sleep(self.retry_delay)
                        else:
                            logging.error(f"No se pudo obtener datos de Polygon.io para {pair} después de varios intentos.")
                            break

            datos_por_par[pair] = all_data

        return datos_por_par

    def insertar_datos(self, datos, pair, timeframe):
        """Inserta los datos obtenidos en la tabla correspondiente de la base de datos."""
        if not datos:
            logging.warning(f"No hay datos para insertar para {pair} en timeframe {timeframe}.")
            return
        
        table_name = f'forex_data_{timeframe}'
        ultimo_timestamp = self.obtener_ultimo_registro(pair, timeframe)

        if ultimo_timestamp is None:
            logging.info(f"No hay registros previos para {pair} en {timeframe}. Insertando todos los datos.")
            ultimo_timestamp = datetime.utcfromtimestamp(0)  # Definir como epoch en caso de no tener timestamp

        try:
            cursor = self.db_connection.cursor()
            batch = []
            for record in datos:
                timestamp = record['t'] / 1000  # Convertir a segundos
                timestamp_dt = datetime.utcfromtimestamp(timestamp)
                if timestamp_dt > ultimo_timestamp:  # Comparar con datetime
                    query = f"""
                    INSERT INTO {table_name} (pair, timestamp, open, high, low, close, volume)
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

            logging.info(f"Datos insertados correctamente en la base de datos para {pair} en timeframe {timeframe}.")
        except psycopg2.Error as e:
            logging.error(f"Error al insertar datos en la base de datos para {pair}: {e}")
            self.db_connection.rollback()
        finally:
            cursor.close()

    def iniciar_proceso(self, pairs, timeframes):
        """Proceso principal para obtener y guardar los datos según los timeframes."""
        for timeframe in timeframes:
            datos_por_par = self.obtener_datos_polygon_batch(pairs, timeframe)

            for pair, datos in datos_por_par.items():
                if datos:
                    self.insertar_datos(datos, pair, timeframe)
                else:
                    logging.warning(f"No se obtuvieron datos para {pair} en timeframe {timeframe}.")
