import requests
import pandas as pd
import pandas_ta as ta
from DataFetcher import DataFetcher
import MetaTrader5 as mt5
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta

class ForexSignalAnalyzer:
    def __init__(self, data_fetcher, mt5_executor, api_key_polygon):
        self.data_fetcher = data_fetcher
        self.mt5_executor = mt5_executor  # Instancia del ejecutor de MetaTrader 5
        self.api_key_polygon = api_key_polygon  # API Key para Polygon.io
        self.executor = ThreadPoolExecutor(max_workers=5)  # Manejar señales de múltiples pares en paralelo

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

        # Verificar si el mercado Forex (fx) está abierto
        if data.get('currencies', {}).get('fx') == "open":
            return True
        else:
            print("El mercado Forex está cerrado. No se realizarán análisis.")
            return False

    def obtener_datos_rsi(self, symbol):
        """
        Obtiene datos en un intervalo de 3 minutos desde 5 días antes hasta la fecha actual para calcular el RSI.
        """
        df = self.data_fetcher.obtener_datos(symbol=symbol, timeframe='minute', range='3', days=5)
        if df.empty:
            raise ValueError(f"Los datos obtenidos para {symbol} no son suficientes o están vacíos.")
        return df

    def verificar_operacion_abierta(self, symbol, tipo):
        """
        Verifica si ya hay una operación abierta con el mismo símbolo y tipo (compra/venta).
        Esto incluye operaciones abiertas antes de la ejecución del programa.
        """
        positions = mt5.positions_get(symbol=symbol)
        if positions is None:
            print(f"No se encontraron posiciones abiertas para {symbol}, error code={mt5.last_error()}")
            return False
        for position in positions:
            if (position.type == mt5.ORDER_TYPE_BUY and tipo == "compra") or \
               (position.type == mt5.ORDER_TYPE_SELL and tipo == "venta"):
                print(f"Ya existe una operación abierta para {symbol} tipo {tipo}.")
                return True
        return False

    def generar_senal_trading(self, df, reverso_tendencia, symbol):
        """
        Genera señales de trading basadas en el RSI y la reversión de tendencia detectada.
        """
        rsi = ta.rsi(df['Close'], length=2)
        rsi_actual = rsi.iloc[-1]  # Valor del RSI actual
        print(f"{symbol}: RSI actual = {rsi_actual:.2f}")

        if reverso_tendencia == "Reversión Alcista Detectada" and rsi_actual < 20:
            if not self.verificar_operacion_abierta(symbol, "compra"):
                print(f"{symbol}: Señal de Compra Detectada")
                return "Señal de Compra Detectada"
            return "No hay Señal de Compra"
        elif reverso_tendencia == "Reversión Bajista Detectada" and rsi_actual > 80:
            if not self.verificar_operacion_abierta(symbol, "venta"):
                print(f"{symbol}: Señal de Venta Detectada")
                return "Señal de Venta Detectada"
            return "No hay Señal de Venta"
        return "Neutral - No se analiza señal"

    def analizar_senales(self, pares_reversiones):
        """
        Analiza las señales de trading para los pares en los que se detectaron reversiones de tendencia.
        Ahora funciona en paralelo para manejar la ejecución simultánea de señales.
        Solo se ejecuta si el mercado Forex está abierto.
        """
        if not self.verificar_estado_mercado():
            return {}  # Detener si el mercado está cerrado

        resultados = {}
        futures = []  # Lista para almacenar las tareas de los hilos

        for pair, reverso_tendencia in pares_reversiones.items():
            symbol_polygon = pair.replace("-", "")
            future = self.executor.submit(self.analizar_senal_para_par, symbol_polygon, reverso_tendencia, resultados, pair)
            futures.append(future)

        # Esperar a que todas las señales terminen de analizarse
        for future in futures:
            future.result()

        return resultados

    def analizar_senal_para_par(self, symbol_polygon, reverso_tendencia, resultados, pair):
        """
        Función que maneja el análisis de señales para cada par en paralelo.
        """
        try:
            df = self.obtener_datos_rsi(symbol_polygon)
            resultado_senal = self.generar_senal_trading(df, reverso_tendencia, symbol_polygon)
            resultados[pair] = resultado_senal

            # Si se detecta una señal de compra o venta, ejecutar la orden correspondiente
            if "Señal de Compra Detectada" in resultado_senal or "Señal de Venta Detectada" in resultado_senal:
                self.mt5_executor.ejecutar_orden(symbol_polygon, "buy" if "Compra" in resultado_senal else "sell")
        except ValueError as e:
            print(f"Error en el análisis de señales para {pair}: {str(e)}")
