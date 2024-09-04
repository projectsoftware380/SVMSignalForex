import requests
import pandas as pd
import pandas_ta as ta
from datetime import datetime, timedelta
from DataFetcher import DataFetcher

class ForexAnalyzer:
    def __init__(self, data_fetcher, api_token_forexnews, api_key_polygon):
        self.data_fetcher = data_fetcher
        self.api_token_forexnews = api_token_forexnews
        self.api_key_polygon = api_key_polygon
        self.last_trend = {}  # Almacena la última tendencia de cada par

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
        if data['currencies']['fx'] == "open":
            return True
        else:
            print("El mercado Forex está cerrado. No se realizarán análisis.")
            return False

    def calcular_sma(self, series, length):
        if len(series) < length:
            raise ValueError(f"No se puede calcular SMA de longitud {length} debido a datos insuficientes.")
        sma = ta.sma(series, length=length)
        if sma is None:
            raise ValueError(f"No se pudo calcular SMA para length={length}")
        return sma

    def determinar_tendencia(self, df):
        tenkan_sen = (self.calcular_sma(df['High'], length=9) + self.calcular_sma(df['Low'], length=9)) / 2
        kijun_sen = (self.calcular_sma(df['High'], length=26) + self.calcular_sma(df['Low'], length=26)) / 2
        senkou_span_a = ((tenkan_sen + kijun_sen) / 2).shift(26)
        
        senkou_span_b_high = self.calcular_sma(df['High'], length=52)
        senkou_span_b_low = self.calcular_sma(df['Low'], length=52)
        
        if senkou_span_b_high is None or senkou_span_b_low is None:
            return "Datos insuficientes para calcular la tendencia"
        
        senkou_span_b = (senkou_span_b_high + senkou_span_b_low) / 2
        senkou_span_b = senkou_span_b.shift(26)

        if df['Close'].iloc[-1] > senkou_span_a.iloc[-1] and df['Close'].iloc[-1] > senkou_span_b.iloc[-1]:
            if tenkan_sen.iloc[-1] > kijun_sen.iloc[-1]:
                return "Tendencia Alcista"
            else:
                return "No tendencia (posible reversión)"
        elif df['Close'].iloc[-1] < senkou_span_a.iloc[-1] and df['Close'].iloc[-1] < senkou_span_b.iloc[-1]:
            if tenkan_sen.iloc[-1] < kijun_sen.iloc[-1]:
                return "Tendencia Bajista"
            else:
                return "No tendencia (posible reversión)"
        else:
            return "No tendencia (en consolidación)"

    def obtener_sentimiento(self, pair):
        url = f"https://forexnewsapi.com/api/v1/stat?currencypair={pair}&date=last30days&page=1&token={self.api_token_forexnews}"
        response = requests.get(url)
        data = response.json()

        if 'data' in data:
            fechas = sorted(data['data'].keys(), reverse=True)
            fecha_reciente = fechas[0]
            sentimiento_data = data['data'][fecha_reciente][pair]
            sentiment_score = sentimiento_data['sentiment_score']

            if sentiment_score > 0:
                sentimiento = "Sentimiento Alcista"
            elif sentiment_score < 0:
                sentimiento = "Sentimiento Bajista"
            else:
                sentimiento = "Sentimiento Neutral"

            return sentimiento
        else:
            raise ValueError(f"No se encontraron datos para el par {pair}")

    def analizar_par(self, pair):
        """
        Analiza el par de divisas para determinar la tendencia y sentimiento.
        """
        # Verificar si el mercado Forex está abierto
        if not self.verificar_estado_mercado():
            return f"{pair}: El mercado está cerrado. No se realizó análisis."

        try:
            symbol_polygon = pair.replace("-", "")
            df = self.data_fetcher.obtener_datos(symbol_polygon, timeframe='hour', range='1', days=60)

            if df.empty:
                raise ValueError(f"Los datos obtenidos para {symbol_polygon} no son los más recientes o están incompletos.")

            tendencia = self.determinar_tendencia(df)
            sentimiento = self.obtener_sentimiento(pair)

            if tendencia == "Tendencia Alcista" and sentimiento == "Sentimiento Alcista":
                return f"{symbol_polygon} Tendencia Alcista"
            elif tendencia == "Tendencia Bajista" and sentimiento == "Sentimiento Bajista":
                return f"{symbol_polygon} Tendencia Bajista"
            else:
                return f"{symbol_polygon} Neutral"
        except ValueError as e:
            return f"{pair}: Error en el análisis - {str(e)}"

    def detectar_cambio_tendencia(self, pair):
        """
        Detecta si ha habido un cambio en la tendencia para un par específico.
        """
        tendencia_actual = self.analizar_par(pair)
        if pair not in self.last_trend:
            self.last_trend[pair] = tendencia_actual
            return False  # No se considera un cambio si no hay historial

        if tendencia_actual != self.last_trend[pair]:
            print(f"Cambio de tendencia detectado en {pair}: de {self.last_trend[pair]} a {tendencia_actual}")
            self.last_trend[pair] = tendencia_actual
            return True  # Cambio de tendencia detectado

        return False

# Uso del programa
if __name__ == "__main__":
    api_key_polygon = "0E6O_kbTiqLJalWtmJmlGpTztFUFmmFR"
    api_token_forexnews = "25wpwpebrawmafmvjuagciubjoylthzaybzvbtqk"
    
    data_fetcher = DataFetcher(api_key_polygon)
    
    analyzer = ForexAnalyzer(data_fetcher, api_token_forexnews, api_key_polygon)
    
    pairs = ["GBP-USD", "USD-CHF", "USD-JPY", "GBP-CAD", "USD-CAD"]
    
    for pair in pairs:
        resultado = analyzer.analizar_par(pair)
        print(resultado)  # Solo imprime el resultado final para cada par de divisas

