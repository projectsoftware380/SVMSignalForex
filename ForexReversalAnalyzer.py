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
        Se obtienen datos de los últimos 5 días con velas de 15 minutos.
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
        url = f"https://api.polygon.io/v1/marketstatus/now?apiKey={self.api_key_polygon}"
        response = requests.get(url)
        data = response.json()

        if response.status_code != 200:
            print(f"Error al verificar el estado del mercado: {response.status_code}")
            return False

        # Verificar si el mercado de Forex está abierto
        if data.get('currencies', {}).get('fx') == "open":
            return True
        else:
            print("El mercado Forex está cerrado. No se realizarán análisis.")
            return False

    def analizar_reversiones(self, pares_tendencia):
        """
        Analiza los pares de divisas en tendencia para detectar posibles reversiones.
        Solo se ejecuta si el mercado está abierto.
        """
        if not self.verificar_estado_mercado():
            return {}  # Detener la ejecución si el mercado está cerrado

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
                if isinstance(resultados, dict):
                    resultados[pair] = resultado_reversion
                else:
                    raise TypeError(f"Se esperaba un diccionario para almacenar los resultados, pero se recibió: {type(resultados)}")
                # Si se detecta una reversión, enviar una solicitud al MetaTrader5Executor
                self.mt5_executor.procesar_reversion(pair, resultado_reversion)
        except ValueError as e:
            print(f"Error en el análisis para {pair}: {str(e)}")
        except TypeError as e:
            print(f"Error de tipo en {pair}: {str(e)}")

# Ejemplo de uso
if __name__ == "__main__":
    api_key_polygon = "0E6O_kbTiqLJalWtmJmlGpTztFUFmmFR"  # Reemplaza con tu clave API

    # Instancia de DataFetcher
    data_fetcher = DataFetcher(api_key_polygon)

    # Instancia de MetaTrader5Executor (debería ser multihilo)
    mt5_executor = MetaTrader5Executor()

    # Instancia de ForexReversalAnalyzer utilizando DataFetcher y MetaTrader5Executor
    reversal_analyzer = ForexReversalAnalyzer(data_fetcher, mt5_executor, api_key_polygon)

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
