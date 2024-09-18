import requests
import pandas as pd
import pandas_ta as ta
from MetaTrader5Executor import MetaTrader5Executor
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta

class ForexSignalAnalyzer:
    def __init__(self, mt5_executor, api_key_polygon):
        self.mt5_executor = mt5_executor  # Instancia del ejecutor de MetaTrader 5
        self.api_key_polygon = api_key_polygon

    def verificar_estado_mercado(self):
        """
        Verifica si el mercado Forex está abierto.
        Para simplificar, esta función puede devolver siempre True, o puedes implementar lógica adicional
        para verificar el horario de mercado.
        """
        return True  # O la lógica que decidas implementar

    def obtener_datos_api(self, symbol, timeframe='minute', days=1):
        """
        Obtiene datos de la API de Polygon.io para el símbolo dado.
        """
        try:
            # Calcular fechas de inicio y fin
            fecha_final = datetime.now().strftime('%Y-%m-%d')
            fecha_inicio = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')

            url = f"https://api.polygon.io/v2/aggs/ticker/C:{symbol}/range/1/{timeframe}/{fecha_inicio}/{fecha_final}"
            params = {
                'apiKey': self.api_key_polygon,
                'limit': 50000,
                'sort': 'asc'
            }
            response = requests.get(url, params=params)

            if response.status_code == 200:
                data = response.json().get('results', [])
                if len(data) == 0:
                    print(f"Advertencia: No se obtuvieron datos para {symbol}.")
                    return pd.DataFrame()

                # Crear DataFrame con los valores de "High", "Low", "Close" y "Open"
                df = pd.DataFrame(data)
                df['timestamp'] = pd.to_datetime(df['t'], unit='ms')
                df.set_index('timestamp', inplace=True)
                df.rename(columns={'h': 'High', 'l': 'Low', 'c': 'Close', 'o': 'Open'}, inplace=True)

                return df[['High', 'Low', 'Close', 'Open']]  # Devolver solo columnas relevantes
            else:
                print(f"Error al obtener datos: {response.status_code}")
                return pd.DataFrame()
        except Exception as e:
            print(f"Error al obtener datos de la API para {symbol}: {e}")
            return pd.DataFrame()

    def obtener_datos_rsi(self, symbol):
        """
        Obtiene datos para calcular el RSI.
        """
        df = self.obtener_datos_api(symbol, timeframe='minute', days=1)  # Obtener datos de 1 día
        if df.empty:
            raise ValueError(f"Los datos obtenidos para {symbol} no son suficientes o están vacíos.")
        
        # Convertir columnas a tipo numérico y eliminar NaN
        for col in ['Open', 'High', 'Low', 'Close']:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        df = df.dropna()

        return df

    def generar_senal_trading(self, df, reverso_tendencia):
        """
        Genera señales de trading basadas en el RSI y las reversiones detectadas.
        """
        try:
            rsi = ta.rsi(df['Close'], length=14)  # Ajusta el período según necesites
            if rsi is None or rsi.empty:
                raise ValueError("No se pudo calcular el RSI.")
            
            ultimo_rsi = rsi.iloc[-1]
            print(f"RSI para {df.index[-1]}: {ultimo_rsi}")

            if ultimo_rsi > 80 and reverso_tendencia == "Reversión Bajista":
                return "Señal de Venta"
            elif ultimo_rsi < 20 and reverso_tendencia == "Reversión Alcista":
                return "Señal de Compra"
            return "No hay señal"
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

        self.imprimir_diccionario_senales(resultados)
        return resultados

    def analizar_senal_para_par(self, pair, reverso_tendencia, resultados, imprimir_senales):
        """
        Función que maneja el análisis de señales para cada par en paralelo.
        """
        try:
            df = self.obtener_datos_rsi(pair)
            resultado_senal = self.generar_senal_trading(df, reverso_tendencia)
            if resultado_senal and ("Compra" in resultado_senal or "Venta" in resultado_senal):
                resultados[pair] = resultado_senal
                if imprimir_senales:
                    print(f"Señal detectada para {pair}: {resultado_senal}")
                # Ejecutar una orden en MetaTrader 5 según la señal detectada
                order_type = "buy" if "Compra" in resultado_senal else "sell"
                self.mt5_executor.ejecutar_orden(pair.replace("-", ""), order_type)
        except ValueError as e:
            print(f"Error en el análisis de señales para {pair}: {str(e)}")
        except TypeError as e:
            print(f"Error de tipo en {pair}: {str(e)}")

    def imprimir_diccionario_senales(self, resultados):
        """
        Imprime el diccionario de señales detectadas.
        """
        if not resultados:
            print("No se detectaron señales para ningún par.")
        else:
            print("\nDiccionario de señales detectadas:")
            for pair, senal in resultados.items():
                print(f"{pair}: {senal}")
