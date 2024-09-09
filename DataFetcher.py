import requests
from datetime import datetime, timedelta, timezone
import pandas as pd

class DataFetcher:
    def __init__(self, api_key_polygon):
        self.api_key_polygon = api_key_polygon

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

        # Ajustar la URL para obtener los datos desde la fecha actual hasta el número de días especificado
        url = f"https://api.polygon.io/v2/aggs/ticker/C:{symbol}/range/{range}/{timeframe}/{fecha_inicio.strftime('%Y-%m-%d')}/{fecha_actual.strftime('%Y-%m-%d')}"
        params = {
            "adjusted": "true",
            "sort": "asc",  # Asegurarnos de que los datos estén en orden cronológico
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
            
            # Imprimir el último precio recibido para verificar
            # print(f"Par: {symbol}, Último dato: {df.index[-1]}, Precio de cierre más reciente: {df['Close'].iloc[-1]}")
            
            return df[['Open', 'High', 'Low', 'Close', 'Volume']]
        else:
            raise ValueError(f"No se pudieron obtener datos de la API para {symbol}.")

# Ejemplo de uso
data_fetcher = DataFetcher("0E6O_kbTiqLJalWtmJmlGpTztFUFmmFR")
estado_mercado = data_fetcher.obtener_estado_mercado()
if estado_mercado:
    print("El mercado está abierto.")
    # Puedes agregar la lógica aquí para obtener datos históricos
    symbol = "EURUSD"  # Ejemplo de símbolo
    datos = data_fetcher.obtener_datos(symbol, 'hour', '1', 1)  # Último día
else:
    print("Mercado cerrado")


