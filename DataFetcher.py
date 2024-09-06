import requests
import json
import os
from datetime import datetime, timedelta, timezone
import pandas as pd

class DataFetcher:
    def __init__(self, api_key_polygon, api_token_forexnews):
        self.api_key_polygon = api_key_polygon
        self.api_token_forexnews = api_token_forexnews
        self.data_file = "forex_news_data.json"  # Archivo para almacenar los datos de Forex News

    def obtener_estado_mercado(self):
        """
        Verifica si el mercado de divisas (forex) está abierto a través de la API de Polygon.io.
        """
        url = f"https://api.polygon.io/v1/marketstatus/now?apiKey={self.api_key_polygon}"
        response = requests.get(url)
        if response.status_code == 401:
            raise ValueError("Error 401: No autorizado. Verifica tu API Key.")
        elif response.status_code != 200:
            raise ValueError(f"Error al obtener el estado del mercado: {response.status_code}")
        
        market_status = response.json()

        # Asegurarse de que la clave 'fx' esté presente en la respuesta
        if 'currencies' in market_status and 'fx' in market_status['currencies']:
            # Verificar si el mercado de divisas está abierto
            return market_status['currencies']['fx'] == "open"
        else:
            raise ValueError("No se pudo determinar el estado del mercado Forex a partir de la respuesta de la API.")

    def obtener_datos(self, symbol, timeframe='hour', range='1', days=1):
        """
        Obtiene los datos históricos para un símbolo específico en un timeframe dado.

        :param symbol: Símbolo del par de divisas o activo (ej. 'EURUSD').
        :param timeframe: Timeframe de las velas (ej. 'hour', 'minute').
        :param range: Rango de tiempo entre velas (ej. '1' para velas de 1 hora).
        :param days: Número de días de historia a recuperar.
        :return: DataFrame con los datos obtenidos.
        """
        fecha_actual = datetime.now(timezone.utc)
        fecha_inicio = fecha_actual - timedelta(days=days)

        url = f"https://api.polygon.io/v2/aggs/ticker/C:{symbol}/range/{range}/{timeframe}/{fecha_inicio.strftime('%Y-%m-%d')}/{fecha_actual.strftime('%Y-%m-%d')}"
        params = {
            "adjusted": "true",
            "sort": "desc",
            "apiKey": self.api_key_polygon
        }

        response = requests.get(url, params=params)
        if response.status_code == 401:
            raise ValueError("Error 401: No autorizado. Verifica tu API Key.")
        elif response.status_code != 200:
            raise ValueError(f"Error al obtener datos de la API para {symbol}: {response.status_code}")
        
        data = response.json()
        if 'results' in data:
            df = pd.DataFrame(data['results'])
            df['timestamp'] = pd.to_datetime(df['t'], unit='ms', utc=True)
            df.set_index('timestamp', inplace=True)
            df.rename(columns={'o': 'Open', 'h': 'High', 'l': 'Low', 'c': 'Close', 'v': 'Volume'}, inplace=True)
            return df[['Open', 'High', 'Low', 'Close', 'Volume']]
        else:
            raise ValueError(f"No se pudieron obtener datos de la API para {symbol}.")

    def obtener_datos_almacenados(self):
        """
        Lee los datos de la API de Forex News almacenados en el archivo, si existen.
        """
        if os.path.exists(self.data_file):
            with open(self.data_file, 'r') as file:
                data = json.load(file)
                return data
        return None

    def guardar_datos_almacenados(self, data):
        """
        Guarda los datos de Forex News en un archivo JSON.
        """
        with open(self.data_file, 'w') as file:
            json.dump(data, file)

    def solicitar_datos_forex_news(self, pair):
        """
        Solicita datos de la API de Forex News si es necesario (una vez al día).
        """
        # Verificar si ya hemos hecho una solicitud hoy
        datos_almacenados = self.obtener_datos_almacenados()
        fecha_actual = datetime.now().strftime('%Y-%m-%d')

        # Si ya hay datos almacenados de la fecha actual, no hacemos una nueva petición
        if datos_almacenados and datos_almacenados.get('fecha') == fecha_actual:
            print("Usando datos almacenados del día.")
            return datos_almacenados['datos']

        # Si no hay datos o son de un día anterior, hacemos una nueva petición
        url = f"https://forexnewsapi.com/api/v1/stat?currencypair={pair}&date=last30days&page=1&token={self.api_token_forexnews}"
        try:
            response = requests.get(url)
            if response.status_code == 200:
                datos_api = response.json()
                # Guardar los datos junto con la fecha actual
                self.guardar_datos_almacenados({'fecha': fecha_actual, 'datos': datos_api})
                return datos_api
            else:
                print(f"Error en la petición: {response.status_code}")
                return None
        except requests.RequestException as e:
            print(f"Error al conectar con la API: {e}")
            return None

# Ejemplo de uso
data_fetcher = DataFetcher("0E6O_kbTiqLJalWtmJmlGpTztFUFmmFR", "tu_forexnews_api_token")
estado_mercado = data_fetcher.obtener_estado_mercado()
if estado_mercado:
    datos_forex_news = data_fetcher.solicitar_datos_forex_news("EUR-USD")
    if datos_forex_news:
        print("Datos obtenidos de Forex News")
else:
    print("Mercado cerrado")

