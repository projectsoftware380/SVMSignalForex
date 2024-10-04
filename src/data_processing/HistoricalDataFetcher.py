import requests
import logging
import os
import time
from datetime import datetime, timedelta

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
    def __init__(self, api_key, max_retries=3, retry_delay=5):
        self.api_key = api_key
        self.max_retries = max_retries  # Número máximo de reintentos
        self.retry_delay = retry_delay  # Intervalo entre reintentos (segundos)

    def obtener_datos_polygon(self, pair, multiplier=1, timeframe='minute', start_date=None, end_date=None):
        """
        Obtiene datos históricos desde la API de Polygon.io para un solo día o un rango con paginación.

        :param pair: Par de divisas en formato EURUSD, GBPUSD, etc.
        :param multiplier: Número de unidades de la temporalidad (ej. 1, 15, 3).
        :param timeframe: Temporalidad de la consulta ('minute', 'hour', 'day').
        :param start_date: Fecha de inicio en formato YYYY-MM-DD (opcional).
        :param end_date: Fecha de fin en formato YYYY-MM-DD (opcional).
        :return: Diccionario con los datos o None en caso de error.
        """
        # Validar formato de fechas y usar el día actual si no se proporcionan fechas
        if not start_date or not end_date:
            start_date = end_date = datetime.utcnow().strftime('%Y-%m-%d')

        # Construir la URL base con los parámetros requeridos
        base_url = f"https://api.polygon.io/v2/aggs/ticker/C:{pair}/range/{multiplier}/{timeframe}/{start_date}/{end_date}"
        params = {
            "adjusted": "true",  # Ajustado por splits
            "sort": "desc",      # Obtener las velas más recientes primero
            "apiKey": self.api_key
        }

        logging.info(f"Haciendo solicitud a: {base_url}")
        all_data = []

        # Implementar reintentos en caso de fallo
        for attempt in range(self.max_retries):
            try:
                start_time = time.time()  # Inicio del monitoreo del tiempo de respuesta

                # Realizar la solicitud HTTP a la API de Polygon
                response = requests.get(base_url, params=params)
                response.raise_for_status()  # Levanta una excepción si hay un error HTTP

                # Registrar el tiempo de respuesta de la API
                response_time = time.time() - start_time
                logging.info(f"Solicitud completada en {response_time:.2f} segundos.")

                data = response.json()

                # Verificar si la respuesta incluye resultados
                if 'results' in data and len(data['results']) >= 2:
                    all_data.extend(data['results'])
                    logging.info(f"Datos obtenidos de Polygon.io para {pair} en timeframe {timeframe} desde {start_date} hasta {end_date}.")

                    # Obtener el penúltimo timestamp de los datos recibidos
                    penultimo_timestamp_api = data['results'][1]['t'] / 1000  # El timestamp del penúltimo dato en milisegundos
                    tiempo_actual = time.time()
                    diferencia_tiempo = tiempo_actual - penultimo_timestamp_api

                    if diferencia_tiempo > self.retry_delay:
                        logging.warning(f"Los datos de {pair} están desactualizados por {diferencia_tiempo:.2f} segundos. Intentando de nuevo.")
                        continue  # Volver a intentar si el dato está desfasado
                else:
                    logging.warning(f"No se encontraron suficientes resultados para {pair} en timeframe {timeframe} desde {start_date} hasta {end_date}.")
                    return None  # No hay datos disponibles, retornar None

                # Manejar la paginación si hay más datos
                next_url = data.get('next_url')
                while next_url:
                    logging.info(f"Paginating: fetching next URL {next_url}")
                    response = requests.get(next_url, params={"apiKey": self.api_key})
                    response.raise_for_status()
                    data = response.json()

                    if 'results' in data:
                        all_data.extend(data['results'])
                        logging.info(f"Datos adicionales obtenidos de la siguiente página de Polygon.io para {pair}.")
                    else:
                        logging.warning(f"No se encontraron más resultados en la siguiente página para {pair}.")
                        break

                    next_url = data.get('next_url')

                # Devolver todos los datos obtenidos
                return {'results': all_data}

            except requests.exceptions.RequestException as e:
                # En caso de error, registrar el mensaje de error y reintentar si es necesario
                logging.error(f"Intento {attempt + 1} - Error al obtener datos de Polygon.io: {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)  # Espera antes de reintentar
                else:
                    logging.error("No se pudo obtener datos de Polygon.io después de varios intentos.")
                    return None

    def insertar_datos(self, datos, pair, timeframe):
        """Procesa los datos y los inserta en la base de datos."""
        if not datos or not datos.get('results'):
            logging.warning(f"No hay datos para insertar para {pair} en timeframe {timeframe}.")
            return
        
        # Aquí iría la lógica para insertar los datos en la base de datos, con la implementación actual.
        logging.info(f"Datos listos para ser insertados en la base de datos para {pair} en timeframe {timeframe}.")
