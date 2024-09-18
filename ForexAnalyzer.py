import pandas as pd
import requests
from datetime import datetime, timedelta

class ForexAnalyzer:
    def __init__(self, api_key_polygon, pairs):
        self.api_key_polygon = api_key_polygon
        self.pairs = pairs  # Lista de pares de divisas para analizar
        self.last_trend = {}  # Almacena solo las tendencias alcistas o bajistas de cada par

    def obtener_datos_polygon(self, symbol, timeframe='hour', start_date=None, end_date=None):
        """
        Solicita datos de velas de la API de Polygon.io.
        """
        url = f"https://api.polygon.io/v2/aggs/ticker/C:{symbol}/range/1/{timeframe}/{start_date}/{end_date}"
        params = {
            'apiKey': self.api_key_polygon,
            'limit': 50000,  # Solicitar hasta el máximo permitido
            'sort': 'asc'    # Orden ascendente, de más antiguo a más reciente
        }
        response = requests.get(url, params=params)

        if response.status_code == 200:
            data = response.json().get('results', [])
            if len(data) == 0:
                print(f"Advertencia: No se obtuvieron suficientes datos para {symbol}.")
                return pd.DataFrame()

            # Crear DataFrame con los valores de "High", "Low", "Close" y "Open"
            df = pd.DataFrame(data)
            df['timestamp'] = pd.to_datetime(df['t'], unit='ms')
            df.set_index('timestamp', inplace=True)
            df.rename(columns={'h': 'High', 'l': 'Low', 'c': 'Close', 'o': 'Open'}, inplace=True)

            print(f"Data obtenida correctamente para {symbol}: {df.shape[0]} filas.")
            return df[['High', 'Low', 'Close', 'Open']]  # Incluimos 'Open' también
        else:
            print(f"Error: No se pudieron obtener los datos para {symbol}. Código de estado {response.status_code}")
            return pd.DataFrame()

    def calcular_ichimoku(self, df):
        """
        Calcula Tenkan-sen, Kijun-sen, Senkou Span A, Senkou Span B y Chikou Span.
        """
        if len(df) < 78:  # Ajustado para requerir más datos (52 + 26 para el desplazamiento)
            print(f"Advertencia: Se requieren al menos 78 períodos para calcular Ichimoku. Solo se obtuvieron {len(df)}.")
            return pd.DataFrame()

        # Tenkan-sen: Promedio del máximo y mínimo de los últimos 9 periodos
        df['Tenkan-sen'] = (df['High'].rolling(window=9).max() + df['Low'].rolling(window=9).min()) / 2
        
        # Kijun-sen: Promedio del máximo y mínimo de los últimos 26 periodos
        df['Kijun-sen'] = (df['High'].rolling(window=26).max() + df['Low'].rolling(window=26).min()) / 2
        
        # Senkou Span A: Promedio de Tenkan-sen y Kijun-sen, desplazado 26 periodos hacia adelante
        df['Senkou Span A'] = ((df['Tenkan-sen'] + df['Kijun-sen']) / 2).shift(26)
        
        # Senkou Span B: Promedio del máximo y mínimo de los últimos 52 periodos, desplazado 26 periodos hacia adelante
        df['Senkou Span B'] = ((df['High'].rolling(window=52).max() + df['Low'].rolling(window=52).min()) / 2).shift(26)

        # Chikou Span: Desplazado el precio de cierre 26 periodos hacia atrás
        df['Chikou Span'] = df['Close'].shift(26)
        
        return df

    def obtener_datos_validos(self, symbol_polygon, timeframe='hour', periodos_necesarios=104):
        """
        Obtiene datos históricos válidos para el análisis técnico.
        Solicitar suficientes datos para cubrir 52 períodos más el desplazamiento de 26 períodos.
        """
        fecha_actual = datetime.now()  # Obtener la fecha y hora actual
        fecha_inicio = fecha_actual - timedelta(days=30)  # Pedir datos de 30 días atrás
        fecha_fin = fecha_actual.strftime('%Y-%m-%d')  # Usar la fecha actual como fecha final

        print(f"Solicitando datos desde {fecha_inicio.strftime('%Y-%m-%d')} hasta {fecha_fin} para {symbol_polygon}...")

        # Obtener los datos históricos de Polygon.io
        df = self.obtener_datos_polygon(symbol_polygon, timeframe, 
                                        fecha_inicio.strftime('%Y-%m-%d'), 
                                        fecha_fin)  # Usar fecha actual como fecha final

        if df.empty:
            print(f"No se obtuvieron datos válidos para {symbol_polygon}.")
            return pd.DataFrame()

        # Mantener solo las últimas 'periodos_necesarios' velas válidas
        if len(df) >= periodos_necesarios:
            df = df.tail(periodos_necesarios)  # Mantener las más recientes
            return df
        else:
            print(f"No se pudieron obtener suficientes datos válidos para {symbol_polygon}. Solo se obtuvieron {len(df)}.")
            return pd.DataFrame()

    def analizar_par(self, pair):
        """
        Analiza el par de divisas para determinar la tendencia técnica.
        """
        print(f"Iniciando análisis para {pair}")
        symbol_polygon = pair.replace("-", "")
        df = self.obtener_datos_validos(symbol_polygon, 'hour', 104)  # Solicitar 104 períodos para cubrir el desplazamiento
        
        if df.empty:
            print(f"No se obtuvieron datos válidos para {pair}")
            return "Neutral"

        # Calcular Ichimoku directamente
        df = self.calcular_ichimoku(df)
        
        if df.empty:
            print(f"No se pudieron calcular los valores de Ichimoku para {pair}")
            return "Neutral"

        # Asegurarse de usar la penúltima vela, es decir, la última vela completa
        ultimo_valor = df.iloc[-2]  # Última vela completa, no la actual que se está formando
        fecha_ultimo_valor = df.index[-2]  # Obtener la fecha de la penúltima vela

        print(f"Valores de Ichimoku para {pair} (Fecha: {fecha_ultimo_valor}):")
        print(ultimo_valor[['Close', 'Senkou Span A', 'Senkou Span B', 'Chikou Span']])

        # Lógica para tendencia alcista
        if (ultimo_valor['Senkou Span A'] > ultimo_valor['Senkou Span B'] and
            ultimo_valor['Close'] > ultimo_valor['Senkou Span A'] and
            ultimo_valor['Chikou Span'] > ultimo_valor['Close']):
            # Almacenar tendencia alcista en el diccionario
            print(f"Tendencia Alcista detectada para {pair}")
            self.last_trend[pair] = "Tendencia Alcista"
            return "Tendencia Alcista"

        # Lógica para tendencia bajista
        elif (ultimo_valor['Senkou Span B'] > ultimo_valor['Senkou Span A'] and
              ultimo_valor['Close'] < ultimo_valor['Senkou Span B'] and
              ultimo_valor['Chikou Span'] < ultimo_valor['Close']):
            # Almacenar tendencia bajista en el diccionario
            print(f"Tendencia Bajista detectada para {pair}")
            self.last_trend[pair] = "Tendencia Bajista"
            return "Tendencia Bajista"

        # Lógica para mercado neutral o indecisión
        print(f"Mercado neutral detectado para {pair}")
        return "Neutral"
