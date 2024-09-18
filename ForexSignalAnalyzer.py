import requests
import pandas as pd
import pandas_ta as ta
from MetaTrader5Executor import MetaTrader5Executor
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
import pytz

class ForexSignalAnalyzer:
    def __init__(self, mt5_executor, api_key_polygon):
        self.mt5_executor = mt5_executor  # Instancia del ejecutor de MetaTrader 5
        self.api_key_polygon = api_key_polygon

    def obtener_hora_servidor(self):
        """
        Obtiene la hora actual del servidor de Polygon.io (en UTC o con zona horaria).
        """
        url = "https://api.polygon.io/v1/marketstatus/now?apiKey=" + self.api_key_polygon
        response = requests.get(url)
        if response.status_code == 200:
            server_time = response.json().get("serverTime", None)
            if server_time:
                try:
                    # Intentar con el formato UTC
                    return datetime.strptime(server_time, '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=pytz.UTC)
                except ValueError:
                    # Intentar con formato que incluye zona horaria
                    return datetime.fromisoformat(server_time).astimezone(pytz.UTC)
        return datetime.utcnow().replace(tzinfo=pytz.UTC)  # Fallback en caso de error

    def verificar_estado_mercado(self):
        """
        Verifica si el mercado Forex está abierto.
        """
        return True  # Implementa la lógica si es necesario

    def obtener_datos_api(self, symbol, timeframe='minute', days=1):
        """
        Obtiene datos de la API de Polygon.io para el símbolo dado.
        """
        try:
            # Obtener la fecha del servidor para evitar problemas de desfase
            fecha_final = self.obtener_hora_servidor()
            fecha_inicio = fecha_final - timedelta(days=days)

            start_date = fecha_inicio.strftime('%Y-%m-%d')
            end_date = fecha_final.strftime('%Y-%m-%d')

            url = f"https://api.polygon.io/v2/aggs/ticker/C:{symbol}/range/1/{timeframe}/{start_date}/{end_date}"
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
                df['timestamp'] = pd.to_datetime(df['t'], unit='ms', utc=True)
                df.set_index('timestamp', inplace=True)
                df.rename(columns={'h': 'High', 'l': 'Low', 'c': 'Close', 'o': 'Open'}, inplace=True)

                # Verificar que la última vela esté completa, si no, tomar la penúltima
                if (fecha_final - df.index[-1]).total_seconds() < 60:
                    df = df.iloc[:-1]  # Remover la última vela incompleta

                return df[['High', 'Low', 'Close', 'Open']]
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
            
            ultimo_rsi = rsi.iloc[-2]  # Usar la penúltima vela para el cálculo del RSI
            print(f"RSI para {df.index[-2]}: {ultimo_rsi}")

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
