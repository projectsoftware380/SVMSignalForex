import pandas as pd
import requests
from datetime import datetime, timedelta
import pytz
import threading

class ForexAnalyzer:
    def __init__(self, api_key_polygon, pairs):
        self.api_key_polygon = api_key_polygon
        self.pairs = pairs  # Lista de pares de divisas para analizar
        self.last_trend = {}  # Almacena las tendencias de cada par (incluso los neutrales)
        self.lock = threading.Lock()  # Añadir el lock para proteger el acceso desde múltiples hilos

    def obtener_hora_servidor(self):
        """Obtiene la hora actual del servidor desde la API de Polygon.io."""
        url = f"https://api.polygon.io/v1/marketstatus/now?apiKey={self.api_key_polygon}"
        response = requests.get(url)
        if response.status_code == 200:
            server_time = response.json().get("serverTime", None)
            if server_time:
                try:
                    return datetime.strptime(server_time, '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=pytz.UTC)
                except ValueError:
                    return datetime.fromisoformat(server_time).astimezone(pytz.UTC)
        return datetime.utcnow().replace(tzinfo=pytz.UTC)

    def obtener_datos_polygon(self, symbol, timeframe='minute', multiplier=4, start_date=None, end_date=None):
        """
        Obtiene datos de mercado de Polygon.io en el intervalo de tiempo solicitado y maneja la paginación.
        Args:
            symbol: Símbolo a consultar.
            timeframe: 'minute', 'hour', etc.
            multiplier: Multiplicador del intervalo (4 para 4 horas).
            start_date: Fecha de inicio en formato YYYY-MM-DD.
            end_date: Fecha de fin en formato YYYY-MM-DD.
        """
        url = f"https://api.polygon.io/v2/aggs/ticker/C:{symbol}/range/{multiplier}/{timeframe}/{start_date}/{end_date}"
        params = {
            'apiKey': self.api_key_polygon,
            'limit': 50000,  # Máximo permitido por la API.
            'sort': 'asc'
        }

        all_data = []  # Para almacenar todos los datos obtenidos.
        next_url = url  # La URL inicial.
        
        while next_url:
            response = requests.get(next_url, params=params)
            if response.status_code == 200:
                data = response.json()
                results = data.get('results', [])
                
                if results:
                    all_data.extend(results)
                else:
                    print(f"No se encontraron resultados adicionales para {symbol}.")
                    break

                # Verificar si hay una página siguiente en los resultados
                next_url = data.get('next_url', None)  # Maneja la paginación.
                if next_url:
                    print(f"Paginando resultados, siguiente URL: {next_url}")
                else:
                    break  # Si no hay `next_url`, no hay más páginas.

            else:
                print(f"Error: No se pudieron obtener los datos para {symbol}. Código de estado {response.status_code}")
                break

        if len(all_data) == 0:
            print(f"Advertencia: No se obtuvieron suficientes datos para {symbol}.")
            return pd.DataFrame()

        # Convertir los datos en DataFrame
        df = pd.DataFrame(all_data)
        df['timestamp'] = pd.to_datetime(df['t'], unit='ms', utc=True)
        df.set_index('timestamp', inplace=True)
        df.rename(columns={'h': 'High', 'l': 'Low', 'c': 'Close', 'o': 'Open'}, inplace=True)

        print(f"Datos obtenidos correctamente para {symbol}: {df.shape[0]} filas.")
        return df[['High', 'Low', 'Close', 'Open']]

    def calcular_ichimoku(self, df):
        """Calcula los componentes del indicador Ichimoku."""
        if len(df) < 78:
            print(f"Advertencia: Se requieren al menos 78 períodos para calcular Ichimoku. Solo se obtuvieron {len(df)}.")
            return pd.DataFrame()

        df['Tenkan-sen'] = (df['High'].rolling(window=9).max() + df['Low'].rolling(window=9).min()) / 2
        df['Kijun-sen'] = (df['High'].rolling(window=26).max() + df['Low'].rolling(window=26).min()) / 2
        df['Senkou Span A'] = ((df['Tenkan-sen'] + df['Kijun-sen']) / 2).shift(26)
        df['Senkou Span B'] = ((df['High'].rolling(window=52).max() + df['Low'].rolling(window=52).min()) / 2).shift(26)
        df['Chikou Span'] = df['Close'].shift(26)

        return df

    def obtener_datos_validos(self, symbol_polygon, timeframe='hour', periodos_necesarios=104):
        """Obtiene datos de mercado asegurando que cumplan con el número de períodos necesarios."""
        fecha_actual_servidor = self.obtener_hora_servidor()
        fecha_inicio_utc = fecha_actual_servidor - timedelta(days=60)  # Extiende el rango de días para asegurar datos.
        start_date = fecha_inicio_utc.strftime('%Y-%m-%d')
        end_date = fecha_actual_servidor.strftime('%Y-%m-%d')

        print(f"Solicitando datos desde {start_date} hasta {end_date} para {symbol_polygon}...")
        df = self.obtener_datos_polygon(symbol_polygon, timeframe, 4, start_date, end_date)  # Solicitar datos en intervalos de 4 horas.

        if df.empty:
            print(f"No se obtuvieron datos válidos para {symbol_polygon}.")
            return pd.DataFrame()

        if len(df) >= periodos_necesarios:
            df = df.tail(periodos_necesarios)
            return df
        else:
            print(f"No se pudieron obtener suficientes datos válidos para {symbol_polygon}. Solo se obtuvieron {len(df)}.")
            return pd.DataFrame()

    def analizar_par(self, pair):
        """Realiza el análisis del par de divisas utilizando el indicador Ichimoku."""
        print(f"Iniciando análisis para {pair}")
        symbol_polygon = pair.replace("-", "")
        df = self.obtener_datos_validos(symbol_polygon, 'hour', 104)

        if df.empty:
            print(f"No se obtuvieron datos válidos para {pair}")
            tendencia = "Neutral"
            with self.lock:
                self.last_trend[pair] = tendencia
            return tendencia

        df = self.calcular_ichimoku(df)

        if df.empty:
            print(f"No se pudieron calcular los valores de Ichimoku para {pair}")
            tendencia = "Neutral"
            with self.lock:
                self.last_trend[pair] = tendencia
            return tendencia

        ultimo_valor = df.iloc[-2]  # Usamos el penúltimo valor para evitar errores con datos en tiempo real.
        fecha_ultimo_valor = df.index[-2]

        if len(df) >= 26:
            precio_hace_26_periodos = df['Close'].iloc[-26]
        else:
            print(f"No se pudo obtener el precio de hace 26 periodos para {pair}")
            tendencia = "Neutral"
            with self.lock:
                self.last_trend[pair] = tendencia
            return tendencia

        print(f"Valores de Ichimoku para {pair} (Fecha: {fecha_ultimo_valor}):")
        print(ultimo_valor[['Close', 'Tenkan-sen', 'Kijun-sen', 'Senkou Span A', 'Senkou Span B', 'Chikou Span']])

        # Verificación de tendencia alcista
        if (ultimo_valor['Senkou Span A'] > ultimo_valor['Senkou Span B'] and
            ultimo_valor['Close'] > ultimo_valor['Senkou Span A'] and
            ultimo_valor['Tenkan-sen'] > ultimo_valor['Kijun-sen'] and
            ultimo_valor['Chikou Span'] > precio_hace_26_periodos):
            tendencia = "Tendencia Alcista"
        # Verificación de tendencia bajista
        elif (ultimo_valor['Senkou Span B'] > ultimo_valor['Senkou Span A'] and
              ultimo_valor['Close'] < ultimo_valor['Senkou Span B'] and
              ultimo_valor['Tenkan-sen'] < ultimo_valor['Kijun-sen'] and
              ultimo_valor['Chikou Span'] < precio_hace_26_periodos):
            tendencia = "Tendencia Bajista"
        else:
            tendencia = "Neutral"

        with self.lock:
            self.last_trend[pair] = tendencia

        return tendencia
