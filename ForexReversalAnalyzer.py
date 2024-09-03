import pandas as pd
import pandas_ta as ta
from DataFetcher import DataFetcher

class ForexReversalAnalyzer:
    def __init__(self, data_fetcher):
        self.data_fetcher = data_fetcher

    def obtener_datos_bollinger(self, symbol):
        """
        Solicita los datos más recientes para calcular las Bandas de Bollinger.
        Se obtienen datos de los últimos 5 días con velas de 15 minutos.
        """
        df = self.data_fetcher.obtener_datos(symbol=symbol, timeframe='minute', range='15', days=5)
        return df

    def detectar_reversion(self, df, tendencia):
        """
        Detecta una posible reversión basada en las Bandas de Bollinger y la tendencia actual.
        """
        bollinger = ta.bbands(df['Close'], length=20, std=2)
        df['mid'] = bollinger['BBM_20_2.0']  # Línea central
        precio_actual = df['Close'].iloc[-1]
        linea_central = df['mid'].iloc[-1]

        if tendencia == "Tendencia Alcista" and precio_actual < linea_central:
            return "Reversión Alcista Detectada"
        elif tendencia == "Tendencia Bajista" and precio_actual > linea_central:
            return "Reversión Bajista Detectada"
        else:
            return "No hay Reversión"

    def analizar_reversiones(self, pares_tendencia):
        """
        Analiza los pares de divisas en tendencia para detectar posibles reversiones.
        """
        # Imprimir el diccionario que recibe
        print("Diccionario de pares en tendencia recibido:")
        print(pares_tendencia)

        resultados = {}

        for pair, tendencia in pares_tendencia.items():
            if tendencia != "Neutral":
                try:
                    tendencia_simple = "Tendencia Alcista" if "Tendencia Alcista" in tendencia else "Tendencia Bajista"
                    symbol_polygon = pair.replace("-", "")
                    df = self.obtener_datos_bollinger(symbol_polygon)
                    resultado_reversion = self.detectar_reversion(df, tendencia_simple)
                    resultados[pair] = resultado_reversion
                except ValueError as e:
                    resultados[pair] = f"Error en el análisis - {str(e)}"

        return resultados

# Ejemplo de uso
if __name__ == "__main__":
    api_key_polygon = "0E6O_kbTiqLJalWtmJmlGpTztFUFmmFR"  # Reemplaza con tu clave API

    # Instancia de DataFetcher
    data_fetcher = DataFetcher(api_key_polygon)

    # Instancia de ForexReversalAnalyzer utilizando DataFetcher
    reversal_analyzer = ForexReversalAnalyzer(data_fetcher)

    # Ejemplo de pares en tendencia desde ForexAnalyzer (Simulado)
    pares_en_tendencia = {
        "GBP-USD": "Tendencia Alcista",
        "USD-CHF": "Tendencia Bajista",
        "USD-JPY": "Neutral",
        "GBP-CAD": "Tendencia Alcista",
        "USD-CAD": "Tendencia Bajista"
    }

    # Analizar reversiones
    resultados_reversiones = reversal_analyzer.analizar_reversiones(pares_en_tendencia)

    # Imprimir resultados
    for pair, resultado in resultados_reversiones.items():
        print(f"{pair}: {resultado}")

