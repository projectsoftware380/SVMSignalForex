import requests
import pandas as pd
import pandas_ta as ta
from datetime import datetime, timedelta

class ForexReversalAnalyzer:
    def __init__(self, api_key_polygon):
        self.api_key_polygon = api_key_polygon

    def obtener_datos_bollinger(self, symbol):
        # Obtener la fecha actual y la fecha 5 días antes en UTC
        fecha_inicio = (datetime.utcnow() - timedelta(days=5)).strftime('%Y-%m-%d')
        fecha_fin = datetime.utcnow().strftime('%Y-%m-%d')
        
        url = f"https://api.polygon.io/v2/aggs/ticker/C:{symbol}/range/15/minute/{fecha_inicio}/{fecha_fin}"
        params = {
            "adjusted": "true",
            "sort": "asc",
            "apiKey": self.api_key_polygon
        }
        response = requests.get(url, params=params)
        
        if response.status_code != 200:
            raise ValueError(f"Error al obtener datos de la API para {symbol}: {response.status_code}")
        
        data = response.json()

        if 'results' in data:
            df = pd.DataFrame(data['results'])
            df['timestamp'] = pd.to_datetime(df['t'], unit='ms', utc=True)
            df.set_index('timestamp', inplace=True)
            df.rename(columns={'o': 'Open', 'h': 'High', 'l': 'Low', 'c': 'Close', 'v': 'Volume'}, inplace=True)
            return df[['Open', 'High', 'Low', 'Close', 'Volume']]
        else:
            raise ValueError(f"No se pudieron obtener datos de la API para {symbol}.")

    def detectar_reversion(self, df, tendencia, symbol_polygon):
        # Aplicar las Bandas de Bollinger
        bollinger = ta.bbands(df['Close'], length=20, std=2)
        df['mid'] = bollinger['BBM_20_2.0']  # Línea central
        precio_actual = df['Close'].iloc[-1]
        linea_central = df['mid'].iloc[-1]

        # Determinar la reversión basada en la tendencia y el precio actual
        if tendencia == "Tendencia Alcista" and precio_actual < linea_central:
            return "Reversión Alcista Detectada"
        elif tendencia == "Tendencia Bajista" and precio_actual > linea_central:
            return "Reversión Bajista Detectada"
        else:
            return "No hay Reversión"

    def analizar_reversiones(self, pares_tendencia):
        resultados = {}
        
        # Verificar que el diccionario de tendencias esté actualizado
        if not pares_tendencia:
            return resultados
        
        for pair, tendencia in pares_tendencia.items():
            if tendencia != "Neutral":
                try:
                    # Solo pasar "Tendencia Alcista" o "Tendencia Bajista" a la función detectar_reversion
                    if "Tendencia Alcista" in tendencia:
                        tendencia_simple = "Tendencia Alcista"
                    elif "Tendencia Bajista" in tendencia:
                        tendencia_simple = "Tendencia Bajista"
                    else:
                        tendencia_simple = "Neutral"
                    
                    symbol_polygon = pair.replace("-", "")
                    df = self.obtener_datos_bollinger(symbol_polygon)
                    resultado_reversion = self.detectar_reversion(df, tendencia_simple, symbol_polygon)
                    resultados[pair] = resultado_reversion
                    
                    # Imprimir solo si se detecta una reversión
                    if "Reversión" in resultado_reversion:
                        print(f"Reversión para {pair}: {resultado_reversion}")
                except ValueError as e:
                    resultados[pair] = f"Error en el análisis - {str(e)}"
        
        # Retornar el diccionario con los resultados de las reversiones
        return resultados

# Ejemplo de uso
if __name__ == "__main__":
    api_key_polygon = "TU_API_KEY_POLYGON"
    
    # Instancia de ForexReversalAnalyzer
    reversal_analyzer = ForexReversalAnalyzer(api_key_polygon)
    
    # Ejemplo de pares en tendencia desde ForexAnalyzer (Simulado)
    pares_en_tendencia = {
        "GBP-USD": "Tendencia Alcista",
        "USD-CHF": "Tendencia Bajista",
        "USD-JPY": "Neutral",
        "GBP-CAD": "Tendencia Alcista",
        "USD-CAD": "Tendencia Bajista"
    }
    
    # Verificar si el diccionario está vacío
    if not pares_en_tendencia:
        print("El diccionario de pares en tendencia está vacío. No se calcularán reversiones.")
    else:
        # Analizar reversiones
        resultados_reversiones = reversal_analyzer.analizar_reversiones(pares_en_tendencia)
        
        # Opcionalmente, imprimir todos los resultados
        for pair, resultado in resultados_reversiones.items():
            print(f"{pair}: {resultado}")
