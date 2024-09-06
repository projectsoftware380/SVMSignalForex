import requests
import pandas as pd
import pandas_ta as ta
from concurrent.futures import ThreadPoolExecutor
from DataFetcher import DataFetcher
from MetaTrader5Executor import MetaTrader5Executor

class ForexReversalAnalyzer:
    def __init__(self, data_fetcher, mt5_executor, api_key_polygon):
        self.data_fetcher = data_fetcher
        self.mt5_executor = mt5_executor
        self.api_key_polygon = api_key_polygon
        self.executor = ThreadPoolExecutor(max_workers=5)  # Maneja procesamiento paralelo

    def normalizar_par(self, pair):
        """
        Normaliza el formato del par de divisas, eliminando guiones.
        """
        return pair.replace("-", "")

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

        # Agregar las Bandas de Bollinger al dataframe
        df['mid'] = bollinger['BBM_20_2.0']  # Línea central de las Bandas de Bollinger
        df['upper'] = bollinger['BBU_20_2.0']  # Banda superior
        df['lower'] = bollinger['BBL_20_2.0']  # Banda inferior

        precio_actual = df['Close'].iloc[-1]
        linea_central = df['mid'].iloc[-1]

        print(f"Precio actual: {precio_actual}, Línea central: {linea_central}, Tendencia: {tendencia}")

        # Reversión alcista si el precio está por debajo de la línea central
        if tendencia == "Tendencia Alcista" and precio_actual < linea_central:
            print(f"Reversión Alcista Detectada para {df.name}")
            return "Reversión Alcista Detectada"
        # Reversión bajista si el precio está por encima de la línea central
        elif tendencia == "Tendencia Bajista" and precio_actual > linea_central:
            print(f"Reversión Bajista Detectada para {df.name}")
            return "Reversión Bajista Detectada"
        
        return None  # Si no se detecta ninguna reversión

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

        resultados = {}
        # Filtrar solo los pares con tendencia válida (alcista o bajista)
        pares_validos = {self.normalizar_par(pair): tendencia for pair, tendencia in pares_tendencia.items() if tendencia != "No tendencia"}
        
        # Asegurar que siempre haya al menos un hilo disponible
        num_pares_validos = len(pares_validos)
        max_workers = max(1, min(10, num_pares_validos))  # Siempre al menos 1 hilo y máximo 10

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            for pair, tendencia in pares_validos.items():
                symbol_polygon = self.normalizar_par(pair)  # Normalizar símbolo
                future = executor.submit(self.analizar_reversion_para_par, symbol_polygon, tendencia, resultados, pair)
                futures.append(future)

            for future in futures:
                future.result()

        return resultados

    def analizar_reversion_para_par(self, symbol_polygon, tendencia, resultados, pair):
        """
        Función que maneja el análisis de reversiones para cada par en paralelo.
        """
        try:
            df = self.obtener_datos_bollinger(symbol_polygon)
            resultado_reversion = self.detectar_reversion(df, tendencia)
            if resultado_reversion:
                if isinstance(resultados, dict):
                    resultados[self.normalizar_par(pair)] = resultado_reversion  # Normalizar el par aquí también
                    # Imprimir solo cuando se detecta una reversión válida
                    print(f"Reversión detectada para {pair}: {resultado_reversion}")
        except ValueError as e:
            print(f"Error en el análisis para {pair}: {str(e)}")
        except TypeError as e:
            print(f"Error de tipo en {pair}: {str(e)}")
