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

        # Reversión alcista si el precio está por debajo de la línea central
        if tendencia == "Tendencia Alcista" and precio_actual < linea_central:
            return "Reversión Alcista Detectada"
        # Reversión bajista si el precio está por encima de la línea central
        elif tendencia == "Tendencia Bajista" and precio_actual > linea_central:
            return "Reversión Bajista Detectada"
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

        resultados = {}
        futures = []

        for pair, tendencia in pares_tendencia.items():
            if tendencia != "Neutral":
                symbol_polygon = pair.replace("-", "")
                future = self.executor.submit(self.analizar_reversion_para_par, symbol_polygon, tendencia, resultados, pair)
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
                    resultados[pair] = resultado_reversion
                    # Imprimir solo cuando se detecta una reversión válida
                    print(f"Reversión detectada para {pair}: {resultado_reversion}")
                    # Interactuar con MetaTrader5Executor para procesar la reversión
                    self.mt5_executor.procesar_reversion(pair, resultado_reversion)
        except ValueError as e:
            print(f"Error en el análisis para {pair}: {str(e)}")
        except TypeError as e:
            print(f"Error de tipo en {pair}: {str(e)}")

# Uso del programa
if __name__ == "__main__":
    data_fetcher = DataFetcher("0E6O_kbTiqLJalWtmJmlGpTztFUFmmFR")
    mt5_executor = MetaTrader5Executor(None)  # Asegúrate de tener esta clase disponible
    reversal_analyzer = ForexReversalAnalyzer(data_fetcher, mt5_executor, "0E6O_kbTiqLJalWtmJmlGpTztFUFmmFR")
    
    # Simulación de datos de tendencia para pares de divisas
    pares_tendencia_simulada = {
        "GBP-USD": "Tendencia Alcista",
        "USD-JPY": "Tendencia Bajista",
        "EUR-USD": "Tendencia Alcista"
    }
    
    resultado_reversiones = reversal_analyzer.analizar_reversiones(pares_tendencia_simulada)
    print(f"Reversiones detectadas: {resultado_reversiones}")
