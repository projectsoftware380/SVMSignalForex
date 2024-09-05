import requests
import pandas as pd
import pandas_ta as ta
from DataFetcher import DataFetcher
import MetaTrader5 as mt5
from concurrent.futures import ThreadPoolExecutor

class ForexSignalAnalyzer:
    def __init__(self, data_fetcher, mt5_executor, api_key_polygon):
        self.data_fetcher = data_fetcher
        self.mt5_executor = mt5_executor  # Instancia del ejecutor de MetaTrader 5
        self.api_key_polygon = api_key_polygon
        self.executor = ThreadPoolExecutor(max_workers=5)

    def verificar_estado_mercado(self):
        """
        Verifica si el mercado Forex está abierto utilizando la API de Polygon.io proporcionada por DataFetcher.
        """
        return self.data_fetcher.obtener_estado_mercado()

    def obtener_datos_rsi(self, symbol):
        """
        Obtiene datos para calcular el RSI, usando el DataFetcher para solicitar datos históricos.
        """
        return self.data_fetcher.obtener_datos(symbol=symbol, timeframe='hour', range='1', days=14)

    def generar_senal_trading(self, df, reverso_tendencia):
        """
        Genera señales de trading basadas en el RSI y las reversiones detectadas.
        """
        rsi = ta.rsi(df['Close'], length=14)
        if rsi.iloc[-1] > 70 and reverso_tendencia == "Reversión Bajista Detectada":
            return "Señal de Venta Detectada"
        elif rsi.iloc[-1] < 30 and reverso_tendencia == "Reversión Alcista Detectada":
            return "Señal de Compra Detectada"
        return "No hay señal"

    def analizar_senales(self, pares_reversiones, imprimir_senales):
        """
        Analiza las señales de trading para los pares en los que se detectaron reversiones de tendencia.
        """
        if not self.verificar_estado_mercado():
            return {}  # Detener si el mercado está cerrado

        resultados = {}
        futures = []
        for pair, reverso_tendencia in pares_reversiones.items():
            symbol_polygon = pair.replace("-", "")
            future = self.executor.submit(self.analizar_senal_para_par, symbol_polygon, reverso_tendencia, resultados, pair, imprimir_senales)
            futures.append(future)

        for future in futures:
            future.result()

        return resultados

    def analizar_senal_para_par(self, symbol_polygon, reverso_tendencia, resultados, pair, imprimir_senales):
        """
        Función que maneja el análisis de señales para cada par en paralelo.
        """
        try:
            df = self.obtener_datos_rsi(symbol_polygon)
            resultado_senal = self.generar_senal_trading(df, reverso_tendencia)
            if resultado_senal and ("Compra" in resultado_senal or "Venta" in resultado_senal):
                resultados[pair] = resultado_senal
                if imprimir_senales:
                    print(f"Señal detectada para {pair}: {resultado_senal}")
                # Ejecutar una orden en MetaTrader 5 según la señal detectada
                order_type = "buy" if "Compra" in resultado_senal else "sell"
                self.mt5_executor.ejecutar_orden(symbol_polygon, order_type)
        except ValueError as e:
            print(f"Error en el análisis de señales para {pair}: {str(e)}")
        except TypeError as e:
            print(f"Error de tipo en {pair}: {str(e)}")
