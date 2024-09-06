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
        if response.status_code != 200:
            return False
        return response.json().get('currencies', {}).get('fx') == "open"

    def calcular_sma(self, series, length):
        """
        Calcula la media móvil simple (SMA) para una serie de datos.
        """
        return ta.sma(series, length=length)

    def determinar_tendencia(self, df):
        """
        Determina la tendencia de un par de divisas utilizando indicadores técnicos.
        """
        tenkan_sen = (self.calcular_sma(df['High'], 9) + self.calcular_sma(df['Low'], 9)) / 2
        kijun_sen = (self.calcular_sma(df['High'], 26) + self.calcular_sma(df['Low'], 26)) / 2
        senkou_span_a = ((tenkan_sen + kijun_sen) / 2).shift(26)
        senkou_span_b = ((self.calcular_sma(df['High'], 52) + self.calcular_sma(df['Low'], 52)) / 2).shift(26)

        if df['Close'].iloc[-1] > senkou_span_a.iloc[-1] and df['Close'].iloc[-1] > senkou_span_b.iloc[-1]:
            return "Tendencia Alcista"
        elif df['Close'].iloc[-1] < senkou_span_a.iloc[-1] and df['Close'].iloc[-1] < senkou_span_b.iloc[-1]:
            return "Tendencia Bajista"
        return "No tendencia"

    def obtener_sentimiento_detallado(self, pair, dias=5):
        """
        Obtiene el sentimiento del mercado para un par de divisas desde la API de ForexNews.
        Acumula los sentimientos y calcula el promedio del sentimiento durante un período.
        """
        url = f"https://forexnewsapi.com/api/v1/stat?currencypair={pair}&date=last30days&page=1&token={self.api_token_forexnews}"
        response = requests.get(url)
        if response.status_code != 200:
            raise ValueError(f"Error al obtener datos de sentimiento para el par {pair}. Código: {response.status_code}")
        
        data = response.json().get('data', {})
        if not data:
            return "Sin datos de sentimiento"

        total_positive = 0
        total_negative = 0
        sentiment_scores = []

        # Recorrer los datos para acumular sentimientos y calcular el promedio
        for fecha, sentimiento_par in sorted(data.items(), reverse=True):
            sentimiento = sentimiento_par.get(pair, {})
            sentiment_score = sentimiento.get('sentiment_score', 0)
            sentiment_scores.append(sentiment_score)
            
            if sentimiento.get('Positive', 0) > sentimiento.get('Negative', 0):
                total_positive += 1
            elif sentimiento.get('Negative', 0) > sentimiento.get('Positive', 0):
                total_negative += 1
            
            if len(sentiment_scores) >= dias:
                break

        if len(sentiment_scores) == 0:
            return "Sin datos de sentimiento"

        # Promedio del puntaje de sentimiento
        promedio_sentimiento = sum(sentiment_scores) / len(sentiment_scores)

        # Determinar el sentimiento en función de los sentimientos acumulados
        if promedio_sentimiento > 0 and total_positive > total_negative:
            return "Sentimiento Alcista"
        elif promedio_sentimiento < 0 and total_negative > total_positive:
            return "Sentimiento Bajista"
        else:
            return "Sin clasificación clara"

    def analizar_par(self, pair):
        """
        Analiza el par de divisas para determinar la tendencia y el sentimiento.
        Si los datos de sentimiento son insuficientes, buscará pares correlacionados.
        """
        if not self.verificar_estado_mercado():
            return f"{pair}: El mercado está cerrado. No se realizó análisis."

        symbol_polygon = pair.replace("-", "")
        df = self.data_fetcher.obtener_datos(symbol_polygon, 'hour', '1', 60)
        if df.empty:
            return f"{pair}: Datos insuficientes para análisis."

        # Determinar la tendencia y el sentimiento del mercado
        tendencia = self.determinar_tendencia(df)
        sentimiento = self.obtener_sentimiento_detallado(pair)

        # Actualizar la tendencia del par en el diccionario interno
        self.last_trend[pair] = tendencia

        # Solo confirmamos tendencia si la tendencia técnica y el sentimiento coinciden
        if (tendencia == "Tendencia Alcista" and sentimiento == "Sentimiento Alcista") or \
           (tendencia == "Tendencia Bajista" and sentimiento == "Sentimiento Bajista"):
            return f"{symbol_polygon}: {tendencia}, {sentimiento}"
        else:
            return f"{symbol_polygon}: No tendencia"

# Uso del programa
if __name__ == "__main__":
    data_fetcher = DataFetcher("0E6O_kbTiqLJalWtmJmlGpTztFUFmmFR")
    analyzer = ForexAnalyzer(data_fetcher, "25wpwpebrawmafmvjuagciubjoylthzaybzvbtqk", "0E6O_kbTiqLJalWtmJmlGpTztFUFmmFR")
    pairs = ["GBP-USD", "USD-CHF", "USD-JPY", "GBP-CAD", "USD-CAD"]
    for pair in pairs:
        resultado = analyzer.analizar_par(pair)
        print(resultado)
