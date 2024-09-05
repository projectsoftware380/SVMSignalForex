import requests
import pandas as pd
import pandas_ta as ta
from concurrent.futures import ThreadPoolExecutor
from DataFetcher import DataFetcher  # Importar solo lo necesario
from MetaTrader5Executor import MetaTrader5Executor  # Asegurarse de importar correctamente

class ForexReversalAnalyzer:
    def __init__(self, data_fetcher, mt5_executor, api_key_polygon):
        self.data_fetcher = data_fetcher
        self.mt5_executor = mt5_executor
        self.api_key_polygon = api_key_polygon
        self.executor = ThreadPoolExecutor(max_workers=5)  # Para manejar el procesamiento paralelo

    def obtener_datos_bollinger(self, symbol):
        """
        Solicita los datos más recientes para calcular las Bandas de Bollinger.
        """
        df = self.data_fetcher.obtener_datos(symbol=symbol, timeframe='minute', range='15', days=5)
        if df.empty:
            raise ValueError(f"Los datos obtenidos para {symbol} no son suficientes o están vacíos.")
        return df

    def detectar_reversion(self, df, tendencia):
        """
        Detecta una posible reversión basada en las Bandas de Bollinger y la tendencia actual.
        """
        bollinger = ta.bbands(df['Close'], length=20, std=2)
        if bollinger is None or 'BBM_20_2.0' not in bollinger:
            raise ValueError("No se pudo calcular las Bandas de Bollinger.")

        df['mid'] = bollinger['BBM_20_2.0']  # Línea central de las Bandas de Bollinger
        precio_actual = df['Close'].iloc[-1]
        linea_central = df['mid'].iloc[-1]

        if tendencia == "Tendencia Alcista" and precio_actual < linea_central:
            return "Reversión Alcista Detectada"
        elif tendencia == "Tendencia Bajista" and precio_actual > linea_central:
            return "Reversión Bajista Detectada"
        else:
            return None  # Si no se detecta reversión

    def verificar_estado_mercado(self):
        """
        Verifica si el mercado Forex está abierto utilizando la API de Polygon.io.
        """
        return self.data_fetcher.obtener_estado_mercado()

    def analizar_reversiones(self, pares_tendencia):
        """
        Analiza los pares de divisas en tendencia para detectar posibles reversiones.
        """
        if not self.verificar_estado_mercado():
            return {}  # Detener la ejecución si el mercado está cerrado

        resultados = {}  # Verificar que sea un diccionario
        futures = []  # Almacenar las tareas de los hilos

        for pair, tendencia in pares_tendencia.items():
            if tendencia != "Neutral":
                tendencia_simple = "Tendencia Alcista" if "Tendencia Alcista" in tendencia else "Tendencia Bajista"
                symbol_polygon = pair.replace("-", "")
                future = self.executor.submit(self.analizar_reversion_para_par, symbol_polygon, tendencia_simple, resultados, pair)
                futures.append(future)

        # Esperar a que todos los hilos terminen
        for future in futures:
            future.result()

        return resultados

    def analizar_reversion_para_par(self, symbol_polygon, tendencia_simple, resultados, pair):
        """
        Función que maneja el análisis de reversiones para cada par en paralelo.
        """
        try:
            df = self.obtener_datos_bollinger(symbol_polygon)
            resultado_reversion = self.detectar_reversion(df, tendencia_simple)
            if resultado_reversion:
                # Verificar que 'resultados' sea un diccionario antes de agregarle contenido
                if isinstance(resultados, dict):
                    resultados[pair] = resultado_reversion
                else:
                    raise TypeError(f"Error: Se esperaba un diccionario en 'resultados', pero se recibió: {type(resultados)}")
                # Si se detecta una reversión, ejecutar la lógica
                self.mt5_executor.procesar_reversion(pair, resultado_reversion)
        except ValueError as e:
            print(f"Error en el análisis para {pair}: {str(e)}")
        except TypeError as e:
            print(f"Error de tipo en {pair}: {str(e)}")

