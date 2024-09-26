import requests
import pandas as pd
import pandas_ta as ta
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
import pytz
import threading
import time
import logging

# Configurar logging
logging.basicConfig(filename='logs/trading_system.log', level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

class ForexReversalAnalyzer:
    def __init__(self, mt5_executor, api_key_polygon):
        self.mt5_executor = mt5_executor
        self.api_key_polygon = api_key_polygon
        self.resultados = {}
        self.lock = threading.Lock()

    def obtener_hora_servidor(self):
        """Obtiene la hora actual del servidor de Polygon.io (en UTC o con zona horaria) con reintentos."""
        url = f"https://api.polygon.io/v1/marketstatus/now?apiKey={self.api_key_polygon}"
        intentos = 0
        max_reintentos = 3

        while intentos < max_reintentos:
            try:
                response = requests.get(url)
                response.raise_for_status()
                server_time = response.json().get("serverTime", None)
                if server_time:
                    try:
                        return datetime.strptime(server_time, '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=pytz.UTC)
                    except ValueError:
                        return datetime.fromisoformat(server_time).astimezone(pytz.UTC)
            except requests.exceptions.RequestException as e:
                intentos += 1
                logging.warning(f"Intento {intentos}/{max_reintentos} fallido al obtener la hora del servidor: {e}")
                time.sleep(5)
        logging.error("Error persistente al obtener la hora del servidor tras múltiples intentos.")
        return datetime.utcnow().replace(tzinfo=pytz.UTC)

    def obtener_datos_api(self, symbol, timeframe='minute', multiplier=15, horas=75):
        """Solicita datos directamente a la API de Polygon.io para el símbolo dado con manejo de errores y reintentos."""
        try:
            fecha_fin = self.obtener_hora_servidor()
            fecha_inicio = fecha_fin - timedelta(hours=horas)

            start_date = fecha_inicio.strftime('%Y-%m-%d')
            end_date = fecha_fin.strftime('%Y-%m-%d')

            symbol_polygon = symbol.replace("/", "").replace("-", "").upper()

            url = f"https://api.polygon.io/v2/aggs/ticker/C:{symbol_polygon}/range/{multiplier}/{timeframe}/{start_date}/{end_date}?apiKey={self.api_key_polygon}&sort=asc"
            intentos = 0
            max_reintentos = 3

            while intentos < max_reintentos:
                try:
                    response = requests.get(url)
                    response.raise_for_status()
                    data = response.json()
                    if 'results' in data:
                        df = pd.DataFrame(data['results'])
                        df['timestamp'] = pd.to_datetime(df['t'], unit='ms', utc=True)
                        df.set_index('timestamp', inplace=True)

                        if df.empty:
                            logging.warning(f"Advertencia: No se obtuvieron suficientes datos para {symbol}.")
                            return pd.DataFrame()

                        logging.info(f"Datos obtenidos correctamente para {symbol}: {df.shape[0]} filas.")
                        return df[['o', 'h', 'l', 'c']]
                    else:
                        logging.warning(f"No se encontraron resultados en la respuesta para {symbol}.")
                        return pd.DataFrame()
                except requests.exceptions.RequestException as e:
                    intentos += 1
                    logging.warning(f"Intento {intentos}/{max_reintentos} fallido al obtener datos para {symbol}: {e}")
                    time.sleep(5)

            logging.error(f"Error persistente al obtener datos para {symbol} tras múltiples intentos.")
            return pd.DataFrame()

        except Exception as e:
            logging.error(f"Error al obtener datos de la API para {symbol}: {e}")
            return pd.DataFrame()

    def obtener_niveles_fibonacci(self, df):
        """Calcula los niveles de retroceso de Fibonacci basados en el máximo y mínimo del rango de precios reciente."""
        max_precio = df['High'].max()
        min_precio = df['Low'].min()

        niveles_fibonacci = {
            'nivel_23.6': max_precio - 0.236 * (max_precio - min_precio),
            'nivel_38.2': max_precio - 0.382 * (max_precio - min_precio),
            'nivel_50.0': max_precio - 0.5 * (max_precio - min_precio),
            'nivel_61.8': max_precio - 0.618 * (max_precio - min_precio),
            'nivel_78.6': max_precio - 0.786 * (max_precio - min_precio),
        }
        return niveles_fibonacci

    def obtener_datos_bollinger(self, symbol):
        """Obtiene los datos más recientes para calcular las Bandas de Bollinger."""
        df = self.obtener_datos_api(symbol)
        if df.empty:
            raise ValueError(f"Los datos obtenidos para {symbol} no son suficientes o están vacíos.")

        df.columns = ['Open', 'High', 'Low', 'Close']
        for col in ['Open', 'High', 'Low', 'Close']:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        df = df.dropna()

        if len(df) < 22:
            raise ValueError(f"No hay suficientes datos para calcular las Bandas de Bollinger para {symbol}.")

        bollinger = ta.bbands(df['Close'], length=20, std=2)
        df['mid'] = bollinger['BBM_20_2.0']
        df['upper'] = bollinger['BBU_20_2.0']
        df['lower'] = bollinger['BBL_20_2.0']

        return df

    def detectar_retroceso(self, df, tendencia):
        """Detecta un retroceso basado tanto en las Bandas de Bollinger como en los niveles de Fibonacci."""
        try:
            # Usar la penúltima vela completa
            precio_open = df['Open'].iloc[-2]
            precio_high = df['High'].iloc[-2]
            precio_low = df['Low'].iloc[-2]
            precio_close = df['Close'].iloc[-2]
            banda_media = df['mid'].iloc[-2]

            precios = [precio_open, precio_high, precio_low, precio_close]

            # Revisamos retrocesos de Fibonacci
            niveles_fibonacci = self.obtener_niveles_fibonacci(df)

            # Retroceso Alcista: Si cualquier precio está por debajo de la banda media o alcanza un nivel de Fibonacci
            if tendencia == "Tendencia Alcista" and (any(precio < banda_media for precio in precios) or any(precio <= niveles_fibonacci['nivel_61.8'] for precio in precios)):
                logging.info(f"Retroceso Alcista detectado en {df.index[-2]}")
                return "Retroceso Alcista"

            # Retroceso Bajista: Si cualquier precio está por encima de la banda media o alcanza un nivel de Fibonacci
            elif tendencia == "Tendencia Bajista" and (any(precio > banda_media for precio in precios) or any(precio >= niveles_fibonacci['nivel_61.8'] for precio in precios)):
                logging.info(f"Retroceso Bajista detectado en {df.index[-2]}")
                return "Retroceso Bajista"

        except Exception as e:
            logging.error(f"Error al detectar retroceso: {e}")

        return None

    def verificar_estado_mercado(self):
        """Verifica si el mercado de Forex (fx) está abierto consultando la API de Polygon.io."""
        url = f"https://api.polygon.io/v1/marketstatus/now?apiKey={self.api_key_polygon}"
        try:
            response = requests.get(url)
            response.raise_for_status()
            currencies = response.json().get("currencies", {})
            fx_status = currencies.get("fx", None)

            if fx_status == "open":
                logging.info("El mercado de Forex está abierto.")
                return True
            else:
                logging.info(f"El mercado de Forex está cerrado. Estado actual: {fx_status}")
                return False
        except requests.exceptions.RequestException as e:
            logging.error(f"Error al consultar el estado del mercado: {e}")
            return False

    def analizar_reversiones(self, pares_tendencia):
        """Analiza los pares de divisas en tendencia para detectar posibles retrocesos."""
        if not self.verificar_estado_mercado():
            return {}

        with self.lock:
            self.resultados.clear()

        pares_validos = {pair: tendencia for pair, tendencia in pares_tendencia.items() if tendencia != "Neutral"}
        logging.info(f"Pares válidos para analizar reversiones: {pares_validos}")

        if not pares_validos:
            logging.info("No hay pares válidos para analizar reversiones.")
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
                logging.info("\nReversiones detectadas:")
                for pair, reversion in self.resultados.items():
                    logging.info(f"{pair}: {reversion}")
            else:
                logging.info("No se detectaron reversiones.")

        return self.resultados

    def analizar_reversion_para_par(self, symbol, tendencia):
        """Función que maneja el análisis de retrocesos para cada par en paralelo."""
        try:
            logging.info(f"Analizando reversión para {symbol} con tendencia {tendencia}")
            df = self.obtener_datos_bollinger(symbol)
            resultado_reversion = self.detectar_retroceso(df, tendencia)
            if resultado_reversion:
                with self.lock:
                    self.resultados[symbol] = resultado_reversion
        except ValueError as e:
            logging.error(f"Error en el análisis para {symbol}: {str(e)}")
        except TypeError as e:
            logging.error(f"Error de tipo en {symbol}: {str(e)}")
        except Exception as e:
            logging.error(f"Error inesperado en {symbol}: {str(e)}")
