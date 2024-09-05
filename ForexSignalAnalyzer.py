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

    def analizar_senales(self, pares_reversiones):
        """
        Analiza las señales de trading para los pares en los que se detectaron reversiones de tendencia.
        """
        if not self.verificar_estado_mercado():
            return {}  # Detener si el mercado está cerrado

        resultados = {}  # Asegurarse que resultados sea un diccionario
        print(f"Inicializando 'resultados' como diccionario: {resultados}")  # Debug print

        futures = []  # Lista para almacenar las tareas de los hilos
        for pair, reverso_tendencia in pares_reversiones.items():
            print(f"Procesando par: {pair} con reverso {reverso_tendencia}")  # Debug print
            symbol_polygon = pair.replace("-", "")
            future = self.executor.submit(self.analizar_senal_para_par, symbol_polygon, reverso_tendencia, resultados, pair)
            futures.append(future)

        # Esperar a que todas las señales terminen de analizarse
        for future in futures:
            future.result()

        print(f"Contenido final de 'resultados': {resultados}")  # Debug print
        return resultados

    def analizar_senal_para_par(self, symbol_polygon, reverso_tendencia, resultados, pair):
        """
        Función que maneja el análisis de señales para cada par en paralelo.
        """
        try:
            df = self.obtener_datos_rsi(symbol_polygon)
            resultado_senal = self.generar_senal_trading(df, reverso_tendencia, symbol_polygon)
            if isinstance(resultados, dict):
                resultados[pair] = resultado_senal
                print(f"Señal generada para {pair}: {resultado_senal}")  # Debug print
            else:
                raise TypeError(f"Error: Se esperaba un diccionario en 'resultados', pero se recibió: {type(resultados)}")

            if "Señal de Compra Detectada" in resultado_senal or "Señal de Venta Detectada" in resultado_senal:
                self.mt5_executor.ejecutar_orden(symbol_polygon, "buy" if "Compra" in resultado_senal else "sell")
        except ValueError as e:
            print(f"Error en el análisis de señales para {pair}: {str(e)}")
        except TypeError as e:
            print(f"Error de tipo en {pair}: {str(e)}")
