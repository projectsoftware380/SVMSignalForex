import requests
import pandas as pd
import pandas_ta as ta
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
import pytz
import threading

class ForexReversalAnalyzer:
    def __init__(self, mt5_executor, api_key_polygon):
        self.mt5_executor = mt5_executor
        self.api_key_polygon = api_key_polygon
        self.resultados = {}
        self.lock = threading.Lock()

    def obtener_hora_servidor(self):
        """
        Obtiene la hora actual del servidor de Polygon.io (en UTC o con zona horaria).
        """
        url = "https://api.polygon.io/v1/marketstatus/now?apiKey=" + self.api_key_polygon
        response = requests.get(url)
        if response.status_code == 200:
            server_time = response.json().get("serverTime", None)
            if server_time:
                try:
                    # Intentar primero con el formato UTC (con 'Z')
                    return datetime.strptime(server_time, '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=pytz.UTC)
                except ValueError:
                    # Si falla, intentar con formato que incluye zona horaria
                    return datetime.fromisoformat(server_time).astimezone(pytz.UTC)
        return datetime.utcnow().replace(tzinfo=pytz.UTC)

    def obtener_datos_api(self, symbol, timeframe='minute', multiplier=15, horas=75):
        """
        Solicita datos directamente a la API de Polygon.io para el símbolo dado.
        """
        try:
            fecha_fin = self.obtener_hora_servidor()
            fecha_inicio = fecha_fin - timedelta(hours=horas)

            start_date = fecha_inicio.strftime('%Y-%m-%d')
            end_date = fecha_fin.strftime('%Y-%m-%d')

            # Asegurar que el símbolo esté en el formato correcto para Polygon.io
            symbol_polygon = symbol.replace("/", "").replace("-", "").upper()

            url = f"https://api.polygon.io/v2/aggs/ticker/C:{symbol_polygon}/range/{multiplier}/{timeframe}/{start_date}/{end_date}?apiKey={self.api_key_polygon}&sort=asc"
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()
                if 'results' in data:
                    df = pd.DataFrame(data['results'])
                    df['timestamp'] = pd.to_datetime(df['t'], unit='ms', utc=True)
                    df.set_index('timestamp', inplace=True)

                    if df.empty:
                        print(f"Advertencia: No se obtuvieron suficientes datos para {symbol}.")
                        return pd.DataFrame()

                    print(f"Datos obtenidos correctamente para {symbol}: {df.shape[0]} filas.")
                    return df[['o', 'h', 'l', 'c']]
                else:
                    print(f"No se encontraron resultados en la respuesta para {symbol}.")
                    return pd.DataFrame()
            else:
                print(f"Error en la solicitud para {symbol}: {response.status_code}")
                return pd.DataFrame()
        except Exception as e:
            print(f"Error al obtener datos de la API para {symbol}: {e}")
            return pd.DataFrame()

    def obtener_datos_bollinger(self, symbol):
        """
        Obtiene los datos más recientes para calcular las Bandas de Bollinger.
        """
        df = self.obtener_datos_api(symbol)
        if df.empty:
            raise ValueError(f"Los datos obtenidos para {symbol} no son suficientes o están vacíos.")

        df.columns = ['Open', 'High', 'Low', 'Close']
        for col in ['Open', 'High', 'Low', 'Close']:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        df = df.dropna()

        if len(df) < 22:
            raise ValueError(f"No hay suficientes datos para calcular las Bandas de Bollinger para {symbol}.")

        # Cálculo de las Bandas de Bollinger
        bollinger = ta.bbands(df['Close'], length=20, std=2)
        df['mid'] = bollinger['BBM_20_2.0']
        df['upper'] = bollinger['BBU_20_2.0']
        df['lower'] = bollinger['BBL_20_2.0']

        # Tomar la penúltima vela (la última completa)
        precio_cierre = df['Close'].iloc[-2]
        banda_central = df['mid'].iloc[-2]
        print(f"Precio de cierre para {symbol}: {precio_cierre}, Banda central de Bollinger: {banda_central}")

        return df

    def detectar_reversion(self, df, tendencia):
        """
        Detecta una posible reversión basada en las Bandas de Bollinger y la tendencia actual.
        """
        try:
            precio_actual = df['Close'].iloc[-2]  # Usar la penúltima vela completa
            linea_central = df['mid'].iloc[-2]

            if tendencia == "Tendencia Alcista" and precio_actual < linea_central:
                print(f"Reversión Alcista detectada en {df.index[-2]}")
                return "Reversión Alcista"
            elif tendencia == "Tendencia Bajista" and precio_actual > linea_central:
                print(f"Reversión Bajista detectada en {df.index[-2]}")
                return "Reversión Bajista"
        except Exception as e:
            print(f"Error al detectar reversión: {e}")

        return None

    def verificar_estado_mercado(self):
        """
        Verifica si el mercado de Forex (fx) está abierto consultando la API de Polygon.io.
        """
        url = f"https://api.polygon.io/v1/marketstatus/now?apiKey={self.api_key_polygon}"
        try:
            response = requests.get(url)
            if response.status_code == 200:
                # Obtener la parte de "currencies" y verificar el estado de "fx"
                currencies = response.json().get("currencies", {})
                fx_status = currencies.get("fx", None)
                
                if fx_status == "open":
                    print("El mercado de Forex está abierto.")
                    return True
                else:
                    print(f"El mercado de Forex está cerrado. Estado actual: {fx_status}")
                    return False
            else:
                print(f"Error al consultar el estado del mercado en Polygon: {response.status_code}")
                return False
        except Exception as e:
            print(f"Error al verificar el estado del mercado: {e}")
            return False

    def analizar_reversiones(self, pares_tendencia):
        """
        Analiza los pares de divisas en tendencia para detectar posibles reversiones.
        """
        if not self.verificar_estado_mercado():
            return {}

        with self.lock:
            self.resultados.clear()

        pares_validos = {pair: tendencia for pair, tendencia in pares_tendencia.items() if tendencia != "Neutral"}
        print(f"Pares válidos para analizar reversiones: {pares_validos}")

        if not pares_validos:
            print("No hay pares válidos para analizar reversiones.")
            return {}

        max_workers = max(1, min(10, len(pares_validos)))

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            for pair, tendencia in pares_validos.items():
                future = executor.submit(self.analizar_reversion_para_par, pair, tendencia)
                futures.append(future)

            for future in futures:
                future.result()

        with self.lock:
            if self.resultados:
                print("\nReversiones detectadas:")
                for pair, reversion in self.resultados.items():
                    print(f"{pair}: {reversion}")
            else:
                print("No se detectaron reversiones.")

        return self.resultados

    def analizar_reversion_para_par(self, symbol, tendencia):
        """
        Función que maneja el análisis de reversiones para cada par en paralelo.
        """
        try:
            print(f"Analizando reversión para {symbol} con tendencia {tendencia}")
            df = self.obtener_datos_bollinger(symbol)
            resultado_reversion = self.detectar_reversion(df, tendencia)
            if resultado_reversion:
                with self.lock:
                    self.resultados[symbol] = resultado_reversion
        except ValueError as e:
            print(f"Error en el análisis para {symbol}: {str(e)}")
        except TypeError as e:
            print(f"Error de tipo en {symbol}: {str(e)}")
