import requests
import pandas_ta as ta
from datetime import datetime, timedelta
from DataFetcher import DataFetcher

class ForexAnalyzer:
    def __init__(self, data_fetcher, api_token_forexnews, api_key_polygon, pairs, usar_sentimiento_mercado=True):
        self.data_fetcher = data_fetcher
        self.api_token_forexnews = api_token_forexnews
        self.api_key_polygon = api_key_polygon
        self.pairs = pairs  # Mantener los pares originales con guiones para la API
        self.usar_sentimiento_mercado = usar_sentimiento_mercado
        self.last_trend = {}  # Almacena solo las tendencias alcistas o bajistas de cada par

    def verificar_estado_mercado(self):
        """
        Verifica si el mercado Forex está abierto utilizando la API de Polygon.io.
        """
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

    def obtener_sentimiento(self, pair):
        """
        Obtiene el sentimiento del mercado para un par de divisas si está habilitado, de lo contrario omite esta función.
        """
        if not self.usar_sentimiento_mercado:
            return 0  # Si no se utiliza el sentimiento del mercado, devuelve un valor neutral
        
        # Código para obtener el sentimiento del mercado de Forex News API
        datos_forex_news = self.data_fetcher.solicitar_datos_forex_news(pair)
        if datos_forex_news:
            sentimiento_par = datos_forex_news.get('data', {}).get(pair, {}).get('sentiment_score', 0)
            if sentimiento_par > 0:
                return 1  # Sentimiento Alcista
            elif sentimiento_par < 0:
                return 2  # Sentimiento Bajista
        return 0  # Sentimiento Neutral

    def analizar_par(self, pair):
        """
        Analiza el par de divisas para determinar la tendencia técnica y el sentimiento del mercado.
        Si no se utiliza el sentimiento del mercado, solo se basa en los indicadores técnicos.
        """
        if not self.verificar_estado_mercado():
            return "Neutral"  # Siempre devolver un valor significativo

        symbol_polygon = pair.replace("-", "")
        df = self.data_fetcher.obtener_datos(symbol_polygon, 'hour', '1', 60)
        if df.empty:
            return "Neutral"  # Siempre devolver un valor significativo

        # Determinar la tendencia técnica
        tendencia_tecnica = self.determinar_tendencia_tecnica(df)

        # Obtener el sentimiento del mercado solo si está habilitado
        sentimiento = self.obtener_sentimiento(pair)

        # Combinar resultados para generar la tendencia final
        if tendencia_tecnica == 1 and (sentimiento == 1 or not self.usar_sentimiento_mercado):
            tendencia_final = "Tendencia Alcista"
        elif tendencia_tecnica == 2 and (sentimiento == 2 or not self.usar_sentimiento_mercado):
            tendencia_final = "Tendencia Bajista"
        else:
            tendencia_final = "Neutral"  # Cambiado para devolver siempre un valor

        # Almacenar la tendencia final solo si no es neutral
        if tendencia_final in ["Tendencia Alcista", "Tendencia Bajista"]:
            pair_formatted = pair.replace("-", "")
            self.last_trend[pair_formatted] = tendencia_final

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

# Ejemplo de uso con solo indicadores técnicos
data_fetcher = DataFetcher("tu_polygon_api_key", "tu_forexnews_api_token")
analyzer = ForexAnalyzer(data_fetcher, "api_token", "api_key", ["EUR-USD", "GBP-USD"], usar_sentimiento_mercado=False)
analyzer.analizar_par("EUR-USD")
analyzer.analizar_par("GBP-USD")
analyzer.imprimir_diccionario_resultados()
