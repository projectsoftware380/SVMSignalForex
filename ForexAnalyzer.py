import requests
import pandas as pd
import pandas_ta as ta
from datetime import datetime, timedelta

class ForexAnalyzer:
    def __init__(self, api_key_polygon, api_token_forexnews):
        self.api_key_polygon = api_key_polygon
        self.api_token_forexnews = api_token_forexnews

    def obtener_datos_forex(self, symbol):
        # Obtener la fecha actual y la fecha 10 días antes en UTC
        fecha_actual = datetime.utcnow().strftime('%Y-%m-%d')
        fecha_inicio = (datetime.utcnow() - timedelta(days=10)).strftime('%Y-%m-%d')
        
        # URL con el rango de fechas ajustado
        url = f"https://api.polygon.io/v2/aggs/ticker/C:{symbol}/range/1/hour/{fecha_inicio}/{fecha_actual}"
        params = {
            "adjusted": "true",
            "sort": "asc",
            "apiKey": self.api_key_polygon
        }
        response = requests.get(url, params=params)
        data = response.json()

        if 'results' in data:
            df = pd.DataFrame(data['results'])
            df['timestamp'] = pd.to_datetime(df['t'], unit='ms', utc=True)
            df.set_index('timestamp', inplace=True)
            df.rename(columns={'o': 'Open', 'h': 'High', 'l': 'Low', 'c': 'Close', 'v': 'Volume'}, inplace=True)
            return df[['Open', 'High', 'Low', 'Close', 'Volume']]
        else:
            raise ValueError(f"No se pudieron obtener datos de la API para {symbol}.")

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
        senkou_span_b = (self.calcular_sma(df['High'], length=52) + self.calcular_sma(df['Low'], length=52)) / 2
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
        try:
            # Usa el símbolo con el formato correcto (con guion) para ambas consultas
            symbol_polygon = pair.replace("-", "")
            df = self.obtener_datos_forex(symbol_polygon)
            tendencia = self.determinar_tendencia(df)
            sentimiento = self.obtener_sentimiento(pair)  # Usa el formato correcto con guion

            if tendencia == "Tendencia Alcista" and sentimiento == "Sentimiento Alcista":
                return f"{symbol_polygon} Tendencia Alcista"
            elif tendencia == "Tendencia Bajista" and sentimiento == "Sentimiento Bajista":
                return f"{symbol_polygon} Tendencia Bajista"
            else:
                return f"{symbol_polygon} Neutral"
        except ValueError as e:
            return f"{pair}: Error en el análisis - {str(e)}"

# Uso del programa
if __name__ == "__main__":
    api_key_polygon = "0E6O_kbTiqLJalWtmJmlGpTztFUFmmFR"
    api_token_forexnews = "25wpwpebrawmafmvjuagciubjoylthzaybzvbtqk"
    
    analyzer = ForexAnalyzer(api_key_polygon, api_token_forexnews)
    
    pairs = ["GBP-USD", "USD-CHF", "USD-JPY", "GBP-CAD", "USD-CAD"]
    
    for pair in pairs:
        resultado = analyzer.analizar_par(pair)
        print(resultado)
