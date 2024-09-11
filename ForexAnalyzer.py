import requests
import pandas as pd
import pandas_ta as ta
from DataFetcher import DataFetcher

class ForexAnalyzer:
    def __init__(self, data_fetcher, api_key_polygon, pairs):
        self.data_fetcher = data_fetcher
        self.api_key_polygon = api_key_polygon
        self.pairs = pairs  # Lista de pares de divisas para analizar
        self.last_trend = {}  # Almacena solo las tendencias alcistas o bajistas de cada par

    def verificar_estado_mercado(self):
        """
        Verifica si el mercado Forex está abierto utilizando la API de Polygon.io.
        """
        return self.data_fetcher.obtener_estado_mercado()

    def calcular_sma(self, series, length):
        """
        Calcula la media móvil simple (SMA) para una serie de datos.
        """
        return ta.sma(series, length=length)

    def determinar_tendencia_tecnica(self, df):
        """
        Determina la tendencia técnica de un par de divisas utilizando indicadores técnicos de Ichimoku.
        """
        if df.empty:
            return 0  # No hay datos suficientes para determinar tendencia

        # Calcular los componentes de Ichimoku
        tenkan_sen = (self.calcular_sma(df['High'], 9) + self.calcular_sma(df['Low'], 9)) / 2
        kijun_sen = (self.calcular_sma(df['High'], 26) + self.calcular_sma(df['Low'], 26)) / 2
        senkou_span_a = ((tenkan_sen + kijun_sen) / 2).shift(26)
        senkou_span_b = ((self.calcular_sma(df['High'], 52) + self.calcular_sma(df['Low'], 52)) / 2).shift(26)

        # Verificar el orden de Senkou Span A y Senkou Span B
        is_kumo_alcista = senkou_span_a.iloc[-1] > senkou_span_b.iloc[-1]
        is_kumo_bajista = senkou_span_b.iloc[-1] > senkou_span_a.iloc[-1]

        # Condiciones de tendencia
        if df['Close'].iloc[-1] > senkou_span_a.iloc[-1] and df['Close'].iloc[-1] > senkou_span_b.iloc[-1] and is_kumo_alcista:
            return 1  # Tendencia Técnica Alcista
        elif df['Close'].iloc[-1] < senkou_span_a.iloc[-1] and df['Close'].iloc[-1] < senkou_span_b.iloc[-1] and is_kumo_bajista:
            return 2  # Tendencia Técnica Bajista
        return 0  # No hay tendencia técnica clara

    def analizar_par(self, pair):
        """
        Analiza el par de divisas para determinar la tendencia técnica.
        """
        if not self.verificar_estado_mercado():
            return "Neutral"  # Si el mercado está cerrado, devolver 'Neutral'

        symbol_polygon = pair.replace("-", "")
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
            self.last_trend[pair] = tendencia_final

        return tendencia_final  # Siempre retornar un valor significativo

    def imprimir_diccionario_resultados(self):
        """
        Imprime el diccionario con los resultados de la tendencia final de cada par.
        """
        if not self.last_trend:
            print("No se detectaron tendencias para ningún par.")
        else:
            for pair, tendencia in self.last_trend.items():
                print(f"Tendencia para {pair}: {tendencia}")
