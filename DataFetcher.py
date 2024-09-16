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

    def normalizar_par(self, symbol):
        """
        Elimina los guiones del símbolo del par de divisas (ej. convierte 'EUR-USD' a 'EURUSD').
        """
        return symbol.replace("-", "")

    def obtener_datos(self, symbol, timeframe='hour', range='1', days=1):
        """
        Obtiene los datos históricos para un símbolo específico en un timeframe dado.

        :param symbol: Símbolo del par de divisas o activo (ej. 'EURUSD').
        :param timeframe: Timeframe de las velas (ej. 'hour', 'minute').
        :param range: Rango de tiempo entre velas (ej. '1' para velas de 1 hora).
        :param days: Número de días de historia a recuperar.
        :return: DataFrame con los datos obtenidos.
        """
        # Normalizar el símbolo eliminando guiones
        symbol = self.normalizar_par(symbol)

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
            
            return df[['Open', 'High', 'Low', 'Close', 'Volume']]
        else:
            raise ValueError(f"No se pudieron obtener datos de la API para {symbol}.")

    def obtener_precio_cierre_anterior(self, symbol):
        """
        Obtiene el precio de cierre anterior para un par de divisas.
        :param symbol: Símbolo del par de divisas (ej. 'EURUSD').
        :return: Precio de cierre anterior.
        """
        # Obtenemos los datos históricos más recientes (último día por ejemplo)
        df = self.obtener_datos(symbol, 'hour', '1', 1)
        
        if df.empty or len(df) < 2:
            raise ValueError(f"No se encontraron suficientes datos para el par {symbol}")
        
        # Obtener el penúltimo precio de cierre
        precio_cierre_anterior = df['Close'].iloc[-2]
        return precio_cierre_anterior

# Ejemplo de uso
if __name__ == "__main__":
    data_fetcher = DataFetcher("TU_API_KEY")
    estado_mercado = data_fetcher.obtener_estado_mercado()
    if estado_mercado:
        print("El mercado está abierto.")
        # Puedes agregar la lógica aquí para obtener datos históricos
        symbol = "EUR-USD"  # Ejemplo de símbolo
        try:
            precio_cierre_anterior = data_fetcher.obtener_precio_cierre_anterior(symbol)
            print(f"El precio de cierre anterior para {symbol} es {precio_cierre_anterior}")
        except ValueError as e:
            print(e)
    else:
        print("Mercado cerrado")


