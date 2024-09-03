import requests
import pandas as pd
from datetime import datetime, timedelta, timezone

from .DataFetcher import DataFetcher

class DataFetcher:
    def __init__(self, api_key_polygon):
        self.api_key_polygon = api_key_polygon

    def obtener_datos(self, symbol, timeframe='minute', range='1', days=1):
        # Obtener fechas para la solicitud usando datetime.now con timezone.utc
        fecha_actual = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        fecha_inicio = (datetime.now(timezone.utc) - timedelta(days=days)).strftime('%Y-%m-%d')
        
        # Construir la URL de la API
        url = f"https://api.polygon.io/v2/aggs/ticker/C:{symbol}/range/{range}/{timeframe}/{fecha_inicio}/{fecha_actual}"
        params = {
            "adjusted": "true",
            "sort": "asc",
            "apiKey": self.api_key_polygon
        }
        
        # Hacer la solicitud
        response = requests.get(url, params=params)
        if response.status_code != 200:
            raise ValueError(f"Error al obtener datos de la API para {symbol}: {response.status_code}")
        
        data = response.json()
        if 'results' in data:
            df = pd.DataFrame(data['results'])
            df['timestamp'] = pd.to_datetime(df['t'], unit='ms', utc=True)
            df.set_index('timestamp', inplace=True)
            df.rename(columns={'o': 'Open', 'h': 'High', 'l': 'Low', 'c': 'Close', 'v': 'Volume'}, inplace=True)
            print(f"Datos obtenidos para {symbol}:")
            print(df.tail())  # Mostrar los últimos registros para validación
            return df[['Open', 'High', 'Low', 'Close', 'Volume']]
        else:
            raise ValueError(f"No se pudieron obtener datos de la API para {symbol}.")

if __name__ == "__main__":
    # Instancia de DataFetcher con tu API key
    api_key_polygon = "0E6O_kbTiqLJalWtmJmlGpTztFUFmmFR"  # Usa tu clave API aquí
    data_fetcher = DataFetcher(api_key_polygon)
    
    # Prueba con un símbolo específico
    try:
        # Puedes ajustar el símbolo, timeframe, rango y días según sea necesario
        df = data_fetcher.obtener_datos(symbol="EURUSD", timeframe='minute', range='1', days=1)
        print("Datos obtenidos con éxito:")
        print(df)  # Muestra el DataFrame completo para inspección
    except Exception as e:
        print(f"Error al obtener datos: {e}")
