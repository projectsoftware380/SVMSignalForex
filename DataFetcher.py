import requests
from time import sleep
from datetime import datetime, timedelta

class DataFetcher:
    def __init__(self, api_key_polygon, max_retries=3, retry_wait=5):
        self.api_key_polygon = api_key_polygon
        self.max_retries = max_retries
        self.retry_wait = retry_wait

    def obtener_datos(self, symbol, timeframe='hour', range='1', days=1):
        """
        Obtiene los datos históricos para un símbolo específico en un timeframe dado.
        """
        fecha_actual = datetime.now(timezone.utc)
        fecha_inicio = fecha_actual - timedelta(days=days)

        url = f"https://api.polygon.io/v2/aggs/ticker/C:{symbol}/range/{range}/{timeframe}/{fecha_inicio.strftime('%Y-%m-%d')}/{fecha_actual.strftime('%Y-%m-%d')}"
        params = {
            "adjusted": "true",
            "sort": "desc",
            "apiKey": self.api_key_polygon
        }

        retries = 0
        while retries < self.max_retries:
            response = requests.get(url, params=params)
            if response.status_code == 200:
                data = response.json()
                if 'results' in data:
                    # Procesar los datos
                    df = pd.DataFrame(data['results'])
                    df['timestamp'] = pd.to_datetime(df['t'], unit='ms', utc=True)
                    df.set_index('timestamp', inplace=True)
                    df.rename(columns={'o': 'Open', 'h': 'High', 'l': 'Low', 'c': 'Close', 'v': 'Volume'}, inplace=True)
                    df_recientes = df[df.index >= fecha_inicio]
                    return df_recientes[['Open', 'High', 'Low', 'Close', 'Volume']]
                else:
                    raise ValueError(f"No se encontraron datos para {symbol}.")
            else:
                print(f"Error al obtener datos para {symbol}. Intento {retries + 1}/{self.max_retries}. Retrying in {self.retry_wait} seconds...")
                retries += 1
                sleep(self.retry_wait)

        raise ValueError(f"Max retries exceeded for {symbol}.")
