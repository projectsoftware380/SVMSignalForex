import pandas as pd
import pandas_ta as ta
from DataFetcher import DataFetcher
from concurrent.futures import ThreadPoolExecutor

class ForexReversalAnalyzer:
    def __init__(self, data_fetcher, mt5_executor):
        self.data_fetcher = data_fetcher
        self.mt5_executor = mt5_executor
        self.executor = ThreadPoolExecutor(max_workers=5)  # Para manejar el procesamiento paralelo

    def obtener_datos_bollinger(self, symbol):
        """
        Solicita los datos más recientes para calcular las Bandas de Bollinger.
        Se obtienen datos de los últimos 5 días con velas de 15 minutos.
        """
        df = self.data_fetcher.obtener_datos(symbol=symbol, timeframe='minute', range='15', days=5)
        return df

    def detectar_reversion(self, df, tendencia):
        """
        Detecta una posible reversión basada en las Bandas de Bollinger y la tendencia actual.
        """
        bollinger = ta.bbands(df['Close'], length=20, std=2)
        df['mid'] = bollinger['BBM_20_2.0']  # Línea central de las Bandas de Bollinger
        precio_actual = df['Close'].iloc[-1]
        linea_central = df['mid'].iloc[-1]

        if tendencia == "Tendencia Alcista" and precio_actual < linea_central:
            return "Reversión Alcista Detectada"
        elif tendencia == "Tendencia Bajista" and precio_actual > linea_central:
            return "Reversión Bajista Detectada"
        else:
            return None  # Si no se detecta reversión

    def analizar_reversiones(self, pares_tendencia):
        """
        Analiza los pares de divisas en tendencia para detectar posibles reversiones.
        Ahora se ejecuta de forma multihilo para manejar el análisis de varias divisas en paralelo.
        """
        resultados = {}
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

        # Imprimir los resultados de las reversiones detectadas
        if resultados:
            print("Reversiones detectadas:")
            for pair, resultado in resultados.items():
                print(f"{pair}: {resultado}")
        else:
            print("No se detectaron reversiones.")

        return resultados

    def analizar_reversion_para_par(self, symbol_polygon, tendencia_simple, resultados, pair):
        """
        Función que maneja el análisis de reversiones para cada par en paralelo.
        """
        try:
            df = self.obtener_datos_bollinger(symbol_polygon)
            resultado_reversion = self.detectar_reversion(df, tendencia_simple)
            if resultado_reversion:
                resultados[pair] = resultado_reversion
                # Si se detecta una reversión, enviar una solicitud al MetaTrader5Executor
                self.mt5_executor.procesar_reversion(pair, resultado_reversion)  # Nuevo método en MetaTrader5Executor
        except ValueError as e:
            print(f"Error en el análisis para {pair} - {str(e)}")

# Ejemplo de uso
if __name__ == "__main__":
    api_key_polygon = "0E6O_kbTiqLJalWtmJmlGpTztFUFmmFR"  # Reemplaza con tu clave API

    # Instancia de DataFetcher
    data_fetcher = DataFetcher(api_key_polygon)

    # Instancia de MetaTrader5Executor (debería ser multihilo)
    from MetaTrader5Executor import MetaTrader5Executor
    mt5_executor = MetaTrader5Executor()

    # Instancia de ForexReversalAnalyzer utilizando DataFetcher y MetaTrader5Executor
    reversal_analyzer = ForexReversalAnalyzer(data_fetcher, mt5_executor)

    # Ejemplo de pares en tendencia desde ForexAnalyzer (Simulado)
    pares_en_tendencia = {
        "AUD-USD": "AUDUSD Tendencia Bajista",
        "NZD-USD": "NZDUSD Tendencia Bajista",
        "USD-CHF": "USDCHF Tendencia Alcista",
        "USD-INR": "USDINR Tendencia Bajista",
        "EUR-CHF": "EURCHF Tendencia Alcista",
        "GBP-EUR": "GBPEUR Tendencia Bajista",
        "GBP-CHF": "GBPCHF Tendencia Alcista",
        "GBP-INR": "GBPINR Tendencia Bajista",
        "CAD-JPY": "CADJPY Tendencia Alcista",
        "USD-SGD": "USDSGD Tendencia Alcista"
    }

    # Analizar reversiones en paralelo
    resultados_reversiones = reversal_analyzer.analizar_reversiones(pares_en_tendencia)

