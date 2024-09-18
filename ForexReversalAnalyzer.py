import requests
import pandas as pd
import pandas_ta as ta
from concurrent.futures import ThreadPoolExecutor

class ForexReversalAnalyzer:
    def __init__(self, mt5_executor, api_key_polygon):
        self.mt5_executor = mt5_executor
        self.api_key_polygon = api_key_polygon
        self.resultados = {}

    def normalizar_par(self, pair):
        return pair.replace("-", "")

    def obtener_datos_api(self, symbol, timeframe='minute', multiplier=15, horas=75):
        """
        Solicita datos directamente a la API de Polygon.io para el símbolo dado.
        """
        try:
            fecha_fin = pd.Timestamp.now()
            fecha_inicio = fecha_fin - pd.Timedelta(hours=horas)

            start_date = fecha_inicio.strftime('%Y-%m-%d')
            end_date = fecha_fin.strftime('%Y-%m-%d')

            url = f"https://api.polygon.io/v2/aggs/ticker/C:{symbol}/range/1/{timeframe}/{start_date}/{end_date}?apiKey={self.api_key_polygon}&sort=asc"
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()
                if 'results' in data:
                    df = pd.DataFrame(data['results'])
                    df['timestamp'] = pd.to_datetime(df['t'], unit='ms')
                    df.set_index('timestamp', inplace=True)

                    ultimo_timestamp = df.index[-1]
                    if (pd.Timestamp.now() - ultimo_timestamp).total_seconds() > 300:
                        print("Los datos no están actualizados.")
                        return pd.DataFrame()

                    return df[['o', 'h', 'l', 'c']]
                else:
                    print("No se encontraron resultados en la respuesta.")
                    return pd.DataFrame()
            else:
                print(f"Error en la solicitud: {response.status_code}")
                return pd.DataFrame()
        except Exception as e:
            print(f"Error al obtener datos de la API para {symbol}: {e}")
            return pd.DataFrame()

    def obtener_datos_bollinger(self, symbol):
        """
        Obtiene los datos más recientes para calcular las Bandas de Bollinger.
        """
        df = self.obtener_datos_api(symbol)
        if df.empty:
            raise ValueError(f"Los datos obtenidos para {symbol} no son suficientes o están vacíos.")

        df.columns = ['Open', 'High', 'Low', 'Close']
        for col in ['Open', 'High', 'Low', 'Close']:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        df = df.dropna()

        bollinger = ta.bbands(df['Close'], length=20, std=2)
        df['mid'] = bollinger['BBM_20_2.0']
        df['upper'] = bollinger['BBU_20_2.0']
        df['lower'] = bollinger['BBL_20_2.0']

        precio_cierre = df['Close'].iloc[-1]
        banda_central = df['mid'].iloc[-1]
        print(f"Precio de cierre para {symbol}: {precio_cierre}, Banda central de Bollinger: {banda_central}")

        return df

    def detectar_reversion(self, df, tendencia):
        """
        Detecta una posible reversión basada en las Bandas de Bollinger y la tendencia actual.
        """
        try:
            precio_actual = df['Close'].iloc[-1]
            linea_central = df['mid'].iloc[-1]

            if tendencia == "Tendencia Alcista" and precio_actual < linea_central:
                return "Reversión Alcista"
            elif tendencia == "Tendencia Bajista" and precio_actual > linea_central:
                return "Reversión Bajista"
        except Exception as e:
            print(f"Error al detectar reversión: {e}")

        return None

    def verificar_estado_mercado(self):
        return True  # Placeholder para la verificación real

    def analizar_reversiones(self, pares_tendencia):
        """
        Analiza los pares de divisas en tendencia para detectar posibles reversiones.
        """
        if not self.verificar_estado_mercado():
            return {}

        self.resultados.clear()
        pares_validos = {self.normalizar_par(pair): tendencia for pair, tendencia in pares_tendencia.items() if tendencia != "No tendencia"}

        num_pares_validos = len(pares_validos)

        if num_pares_validos == 0:
            print("No hay pares válidos para analizar reversiones.")
            return {}

        # Establecer un mínimo de 1 hilo
        max_workers = max(1, min(10, num_pares_validos))

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            for pair, tendencia in pares_validos.items():
                symbol_polygon = self.normalizar_par(pair)
                future = executor.submit(self.analizar_reversion_para_par, symbol_polygon, tendencia)
                futures.append(future)

            for future in futures:
                future.result()

        self.imprimir_diccionario_reversiones(self.resultados)
        return self.resultados

    def analizar_reversion_para_par(self, symbol_polygon, tendencia):
        """
        Función que maneja el análisis de reversiones para cada par en paralelo.
        """
        try:
            df = self.obtener_datos_bollinger(symbol_polygon)
            resultado_reversion = self.detectar_reversion(df, tendencia)
            if resultado_reversion:
                self.resultados[symbol_polygon] = resultado_reversion
        except ValueError as e:
            print(f"Error en el análisis para {symbol_polygon}: {str(e)}")
        except TypeError as e:
            print(f"Error de tipo en {symbol_polygon}: {str(e)}")

    def imprimir_diccionario_reversiones(self, resultados):
        """
        Imprime el diccionario de las reversiones detectadas.
        """
        if not resultados:
            print("No se detectaron reversiones para ningún par.")
        else:
            print("\nDiccionario de reversiones detectadas:")
            for pair, reversion in resultados.items():
                print(f"{pair}: {reversion}")
