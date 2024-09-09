import requests
import pandas_ta as ta
from datetime import datetime, timedelta
from DataFetcher import DataFetcher

class ForexAnalyzer:
    def __init__(self, data_fetcher, api_token_forexnews, api_key_polygon, pairs, usar_sentimiento_mercado=False, backtesting=False):
        self.data_fetcher = data_fetcher
        self.api_token_forexnews = api_token_forexnews
        self.api_key_polygon = api_key_polygon
        self.pairs = pairs  # Mantener los pares originales con guiones para la API
        self.usar_sentimiento_mercado = usar_sentimiento_mercado
        self.backtesting = backtesting  # Indicador de si estamos en modo de backtesting
        self.last_trend = {}  # Almacena solo las tendencias alcistas o bajistas de cada par
        self.resultados_backtesting = []  # Almacena los resultados del backtesting

    def verificar_estado_mercado(self):
        """
        Verifica si el mercado Forex está abierto utilizando la API de Polygon.io.
        Para el modo de backtesting, no se utiliza esta función.
        """
        if self.backtesting:
            return True  # En modo de backtesting, el estado del mercado no es relevante
        url = f"https://api.polygon.io/v1/marketstatus/now?apiKey={self.api_key_polygon}"
        response = requests.get(url)
        if response.status_code != 200:
            return False
        return response.json().get('currencies', {}).get('fx') == "open"

    def calcular_sma(self, series, length):
        """
        Calcula la media móvil simple (SMA) para una serie de datos.
        """
        return ta.sma(series, length=length)

    def determinar_tendencia_tecnica(self, df):
        """
        Determina la tendencia técnica de un par de divisas utilizando indicadores técnicos.
        """
        if df.empty:
            return 0  # No tendencia técnica
        
        tenkan_sen = (self.calcular_sma(df['High'], 9) + self.calcular_sma(df['Low'], 9)) / 2
        kijun_sen = (self.calcular_sma(df['High'], 26) + self.calcular_sma(df['Low'], 26)) / 2
        senkou_span_a = ((tenkan_sen + kijun_sen) / 2).shift(26)
        senkou_span_b = ((self.calcular_sma(df['High'], 52) + self.calcular_sma(df['Low'], 52)) / 2).shift(26)

        if df['Close'].iloc[-1] > senkou_span_a.iloc[-1] and df['Close'].iloc[-1] > senkou_span_b.iloc[-1]:
            return 1  # Tendencia Técnica Alcista
        elif df['Close'].iloc[-1] < senkou_span_a.iloc[-1] and df['Close'].iloc[-1] < senkou_span_b.iloc[-1]:
            return 2  # Tendencia Técnica Bajista
        return 0  # No tendencia técnica

    def analizar_par(self, pair):
        """
        Analiza el par de divisas para determinar la tendencia técnica.
        """
        if not self.verificar_estado_mercado():
            return "Neutral"  # Siempre devolver un valor significativo

        symbol_polygon = pair.replace("-", "")
        # En modo de backtesting, se puede definir una fecha específica o usar datos históricos
        df = self.data_fetcher.obtener_datos(symbol_polygon, 'hour', '1', 60)
        if df.empty:
            return "Neutral"  # Siempre devolver un valor significativo

        # Determinar la tendencia técnica
        tendencia_tecnica = self.determinar_tendencia_tecnica(df)

        # Si la tendencia técnica es alcista o bajista, se almacena y devuelve
        if tendencia_tecnica == 1:
            tendencia_final = "Tendencia Alcista"
        elif tendencia_tecnica == 2:
            tendencia_final = "Tendencia Bajista"
        else:
            tendencia_final = "Neutral"  # Cambiado para devolver siempre un valor

        # Almacenar la tendencia final solo si no es neutral
        if tendencia_final in ["Tendencia Alcista", "Tendencia Bajista"]:
            pair_formatted = pair.replace("-", "")
            self.last_trend[pair_formatted] = tendencia_final

            # Si estamos en modo de backtesting, almacenamos los resultados
            if self.backtesting:
                self.resultados_backtesting.append({
                    'pair': pair_formatted,
                    'tendencia': tendencia_final,
                    'timestamp': df.index[-1]
                })

        return tendencia_final  # Siempre retornar un valor significativo

    def imprimir_diccionario_resultados(self):
        """
        Imprime el diccionario con los resultados de la tendencia final de cada par (solo los que tienen tendencia).
        """
        if not self.last_trend:
            print("No se detectaron tendencias para ningún par.")
        else:
            for pair, tendencia in self.last_trend.items():
                print(f"Tendencia para {pair}: {tendencia}")

    def realizar_backtesting(self):
        """
        Realiza el backtesting en todos los pares disponibles utilizando datos históricos.
        """
        for pair in self.pairs:
            self.analizar_par(pair)

        # Al finalizar el backtesting, imprime los resultados
        self.imprimir_resultados_backtesting()

    def imprimir_resultados_backtesting(self):
        """
        Imprime los resultados acumulados del backtesting.
        """
        if not self.resultados_backtesting:
            print("No se encontraron resultados de backtesting.")
        else:
            print("\nResultados del Backtesting:")
            for resultado in self.resultados_backtesting:
                print(f"Par: {resultado['pair']}, Tendencia: {resultado['tendencia']}, Fecha: {resultado['timestamp']}")
