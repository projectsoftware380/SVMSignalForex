import requests
import pandas as pd
from datetime import datetime, timedelta, timezone

class DataFetcher:
    def __init__(self, api_key_polygon):
        self.api_key_polygon = api_key_polygon

    def obtener_estado_mercado(self):
        """
        Verifica si el mercado de divisas (forex) está abierto a través de la API de Polygon.io.
        """
        url = f"https://api.polygon.io/v1/marketstatus/now?apiKey={self.api_key_polygon}"
        response = requests.get(url)
        if response.status_code != 200:
            raise ValueError(f"Error al obtener el estado del mercado: {response.status_code}")
        
        market_status = response.json()
        return 'currencies' in market_status and market_status['currencies']['fx'] == "open"

    def obtener_datos(self, symbol, timeframe='hour', range='1', days=1):
        """
        Obtiene los datos históricos para un símbolo específico en un timeframe dado.

        :param symbol: Símbolo del par de divisas o activo (ej. 'EURUSD').
        :param timeframe: Timeframe de las velas (ej. 'hour', 'minute').
        :param range: Rango de tiempo entre velas (ej. '1' para velas de 1 hora).
        :param days: Número de días de historia a recuperar.
        :return: DataFrame con los datos obtenidos.
        """
        if not self.obtener_estado_mercado():
            raise ValueError("El mercado está cerrado. No se pueden obtener datos.")
        
        fecha_actual = datetime.now(timezone.utc)
        fecha_inicio = fecha_actual - timedelta(days=days)

        url = f"https://api.polygon.io/v2/aggs/ticker/C:{symbol}/range/{range}/{timeframe}/{fecha_inicio.strftime('%Y-%m-%d')}/{fecha_actual.strftime('%Y-%m-%d')}"
        params = {
            "adjusted": "true",
            "sort": "desc",
            "apiKey": self.api_key_polygon
        }

        response = requests.get(url, params=params)
        if response.status_code != 200:
            raise ValueError(f"Error al obtener datos de la API para {symbol}: {response.status_code}")
        
        data = response.json()

        if 'results' in data:
            df = pd.DataFrame(data['results'])
            df['timestamp'] = pd.to_datetime(df['t'], unit='ms', utc=True)
            df.set_index('timestamp', inplace=True)
            df.rename(columns={'o': 'Open', 'h': 'High', 'l': 'Low', 'c': 'Close', 'v': 'Volume'}, inplace=True)

            # Filtrar los datos recientes en función de los días solicitados
            df_recientes = df[df.index >= fecha_inicio]

            # Verificar si los datos obtenidos son los más recientes
            timestamp_reciente = df_recientes.index.max()
            if timestamp_reciente is None or fecha_actual - timestamp_reciente > timedelta(hours=1):
                raise ValueError(f"Los datos obtenidos para {symbol} no son los más recientes. Última fecha: {timestamp_reciente}")
            
            return df_recientes[['Open', 'High', 'Low', 'Close', 'Volume']]
        else:
            raise ValueError(f"No se pudieron obtener datos de la API para {symbol}.")
