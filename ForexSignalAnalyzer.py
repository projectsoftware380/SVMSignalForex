# ForexSignalAnalyzer.py

import requests
import pandas as pd
import pandas_ta as ta
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
import pytz
import threading

class ForexSignalAnalyzer:
    def __init__(self, mt5_executor, api_key_polygon):
        self.mt5_executor = mt5_executor
        self.api_key_polygon = api_key_polygon
        self.lock = threading.Lock()

    def obtener_datos_api(self, symbol, timeframe='minute', multiplier=1, horas=24):
        """
        Solicita datos directamente a la API de Polygon.io para el símbolo dado.
        """
        try:
            fecha_fin = datetime.utcnow().replace(tzinfo=pytz.UTC)
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
            print(f"RSI para {df.index[-2]}: {ultimo_rsi}")

            if ultimo_rsi > 80 and reverso_tendencia == "Reversión Bajista":
                print(f"Señal de Venta detectada en {df.index[-2]}")
                return "Señal de Venta"
            elif ultimo_rsi < 20 and reverso_tendencia == "Reversión Alcista":
                print(f"Señal de Compra detectada en {df.index[-2]}")
                return "Señal de Compra"
            return None
        except Exception as e:
            raise ValueError(f"Error al generar la señal de trading: {e}")

    def analizar_senales(self, pares_reversiones, imprimir_senales):
        """
        Analiza las señales de trading para los pares en los que se detectaron reversiones de tendencia.
        """
        if not self.verificar_estado_mercado():
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
            print("\nSeñales detectadas:")
            for pair, senal in resultados.items():
                print(f"{pair}: {senal}")
        else:
            print("No se detectaron señales.")

        return resultados

    def analizar_senal_para_par(self, pair, reverso_tendencia, resultados, imprimir_senales):
        """
        Función que maneja el análisis de señales para cada par en paralelo.
        """
        try:
            print(f"Analizando señal para {pair} con reversión {reverso_tendencia}")
            df = self.obtener_datos_rsi(pair)
            resultado_senal = self.generar_senal_trading(df, reverso_tendencia)
            if resultado_senal and ("Compra" in resultado_senal or "Venta" in resultado_senal):
                with self.lock:
                    resultados[pair] = resultado_senal
                if imprimir_senales:
                    print(f"Señal detectada para {pair}: {resultado_senal}")
                # Ejecutar una orden en MetaTrader 5 según la señal detectada
                order_type = "buy" if "Compra" in resultado_senal else "sell"
                self.mt5_executor.ejecutar_orden(pair, order_type)
        except ValueError as e:
            print(f"Error en el análisis de señales para {pair}: {str(e)}")
        except TypeError as e:
            print(f"Error de tipo en {pair}: {str(e)}")

    def verificar_estado_mercado(self):
        return True  # Placeholder para la verificación real
