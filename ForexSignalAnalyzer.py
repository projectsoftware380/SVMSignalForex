import requests
import pandas as pd
import pandas_ta as ta
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
import pytz
import threading
import MetaTrader5 as mt5
import logging
import time

# Configurar logging
logging.basicConfig(filename='logs/trading_system.log', level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

class ForexSignalAnalyzer:
    def __init__(self, mt5_executor, api_key_polygon):
        self.mt5_executor = mt5_executor
        self.api_key_polygon = api_key_polygon
        self.lock = threading.Lock()

    def obtener_datos_api(self, symbol, timeframe='minute', multiplier=1, horas=24):
        """
        Solicita datos directamente a la API de Polygon.io para el símbolo dado con manejo de errores y reintentos.
        """
        try:
            fecha_fin = datetime.utcnow().replace(tzinfo=pytz.UTC)
            fecha_inicio = fecha_fin - timedelta(hours=horas)

            start_date = fecha_inicio.strftime('%Y-%m-%d')
            end_date = fecha_fin.strftime('%Y-%m-%d')

            # Asegurar que el símbolo esté en el formato correcto para Polygon.io
            symbol_polygon = symbol.replace("/", "").replace("-", "").upper()

            url = f"https://api.polygon.io/v2/aggs/ticker/C:{symbol_polygon}/range/{multiplier}/{timeframe}/{start_date}/{end_date}?apiKey={self.api_key_polygon}&sort=asc"
            intentos = 0
            max_reintentos = 3

            while intentos < max_reintentos:
                try:
                    response = requests.get(url)
                    response.raise_for_status()  # Lanza excepción si hay error HTTP
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
                    time.sleep(5)  # Esperar antes de reintentar

            logging.error(f"Error persistente al obtener datos para {symbol} tras múltiples intentos.")
            return pd.DataFrame()

        except Exception as e:
            logging.error(f"Error al obtener datos de la API para {symbol}: {e}")
            return pd.DataFrame()

    def obtener_datos_rsi(self, symbol):
        """
        Obtiene datos para calcular el RSI.
        """
        df = self.obtener_datos_api(symbol, timeframe='minute', multiplier=1, horas=24)
        if df.empty:
            raise ValueError(f"Los datos obtenidos para {symbol} no son suficientes o están vacíos.")

        df.columns = ['Open', 'High', 'Low', 'Close']
        for col in ['Open', 'High', 'Low', 'Close']:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        df = df.dropna()

        return df

    def generar_senal_trading(self, df, reverso_tendencia):
        """
        Genera señales de trading basadas en el RSI y las reversiones detectadas.
        """
        try:
            rsi = ta.rsi(df['Close'], length=2)
            if rsi is None or rsi.empty:
                raise ValueError("No se pudo calcular el RSI.")

            ultimo_rsi = rsi.iloc[-2]  # Usar la penúltima vela para el cálculo del RSI
            logging.info(f"RSI para {df.index[-2]}: {ultimo_rsi}")

            if ultimo_rsi > 80 and reverso_tendencia == "Reversión Bajista":
                logging.info(f"Señal de Venta detectada en {df.index[-2]}")
                return "Señal de Venta"
            elif ultimo_rsi < 20 and reverso_tendencia == "Reversión Alcista":
                logging.info(f"Señal de Compra detectada en {df.index[-2]}")
                return "Señal de Compra"
            return None
        except Exception as e:
            raise ValueError(f"Error al generar la señal de trading: {e}")

    def analizar_senales(self, pares_reversiones, imprimir_senales):
        """
        Analiza las señales de trading para los pares en los que se detectaron reversiones de tendencia.
        """
        if not self.verificar_estado_mercado():
            logging.info("El mercado no está abierto. No se procesarán señales.")
            return {}

        resultados = {}
        num_pares = len(pares_reversiones)
        max_workers = min(10, num_pares)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            for pair, reverso_tendencia in pares_reversiones.items():
                future = executor.submit(self.analizar_senal_para_par, pair, reverso_tendencia, resultados, imprimir_senales)
                futures.append(future)

            for future in futures:
                future.result()

        # Imprimir el diccionario de señales detectadas
        if resultados:
            logging.info("\nSeñales detectadas:")
            for pair, senal in resultados.items():
                logging.info(f"{pair}: {senal}")
        else:
            logging.info("No se detectaron señales.")

        return resultados

    def analizar_senal_para_par(self, pair, reverso_tendencia, resultados, imprimir_senales):
        """
        Función que maneja el análisis de señales para cada par en paralelo.
        """
        try:
            logging.info(f"Analizando señal para {pair} con reversión {reverso_tendencia}")
            
            # Obtener los datos para calcular RSI
            df = self.obtener_datos_rsi(pair)
            
            # Generar la señal de trading basada en RSI y reversión
            resultado_senal = self.generar_senal_trading(df, reverso_tendencia)
            
            # Si se genera una señal válida de compra o venta
            if resultado_senal and ("Compra" in resultado_senal or "Venta" in resultado_senal):
                with self.lock:
                    resultados[pair] = resultado_senal
                
                if imprimir_senales:
                    logging.info(f"Señal detectada para {pair}: {resultado_senal}")
                
                # Determinar el tipo de orden (buy/sell)
                order_type = "buy" if "Compra" in resultado_senal else "sell"
                
                # Asegurarse de que el símbolo está disponible antes de ejecutar la orden
                symbol_info = mt5.symbol_info(pair)
                if symbol_info is None or not symbol_info.visible:
                    logging.warning(f"{pair} no encontrado o no visible. Intentando habilitarlo.")
                    if not mt5.symbol_select(pair, True):
                        logging.error(f"No se pudo habilitar el símbolo {pair}. Orden no ejecutada.")
                        return

                # Ejecutar la orden a través del ejecutor de MetaTrader 5
                logging.info(f"Intentando ejecutar orden {order_type} para {pair}")
                self.mt5_executor.ejecutar_orden(pair, order_type)

        except ValueError as e:
            logging.error(f"Error en el análisis de señales para {pair}: {str(e)}")
        except TypeError as e:
            logging.error(f"Error de tipo en {pair}: {str(e)}")
        except Exception as e:
            logging.error(f"Error inesperado al analizar la señal para {pair}: {str(e)}")

    def verificar_estado_mercado(self):
        """
        Verifica si el mercado de Forex (fx) está abierto consultando la API de Polygon.io.
        """
        url = f"https://api.polygon.io/v1/marketstatus/now?apiKey={self.api_key_polygon}"
        try:
            response = requests.get(url)
            response.raise_for_status()  # Lanza excepción si hay error HTTP
            # Obtener la parte de "currencies" y verificar el estado de "fx"
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
