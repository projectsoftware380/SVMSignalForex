import requests
import logging
import os
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
    def __init__(self, api_key):
        self.api_key = api_key

    def obtener_datos_polygon(self, pair, multiplier=1, timeframe='minute', start_date=None, end_date=None):
        """
        Obtiene datos históricos desde la API de Polygon.io para un solo día.
        
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
        
        try:
            # Realizar la solicitud HTTP a la API de Polygon
            response = requests.get(base_url, params=params)
            response.raise_for_status()  # Levanta una excepción si hay un error HTTP
            data = response.json()

            # Verificar si los datos fueron obtenidos correctamente
            if 'results' in data:
                logging.info(f"Datos obtenidos de Polygon.io para {pair} en timeframe {timeframe} desde {start_date} hasta {end_date}.")
            else:
                logging.warning(f"No se encontraron resultados para {pair} en timeframe {timeframe} desde {start_date} hasta {end_date}.")

            return data

        except requests.exceptions.RequestException as e:
            # En caso de error, registrar el mensaje de error y devolver None
            logging.error(f"Error al obtener datos de Polygon.io: {e}")
            return None

    def insertar_datos(self, datos, pair, timeframe):
        """Procesa los datos y los inserta en la base de datos"""
        # Aquí iría la lógica para insertar los datos en la base de datos, con la implementación actual
        pass
