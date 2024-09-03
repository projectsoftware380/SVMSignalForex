import pandas as pd
import pandas_ta as ta
from datetime import datetime, timezone
from DataFetcher import DataFetcher  # Asegúrate de que data_fetcher.py esté en la misma carpeta

class ForexReversalAnalyzer:
    def __init__(self, data_fetcher):
        self.data_fetcher = data_fetcher

    def obtener_datos_bollinger(self, symbol):
        # Obtener la fecha actual en UTC
        fecha_actual = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        
        # Solicitar los datos más recientes, 5 días hacia atrás desde la fecha actual
        df = self.data_fetcher.obtener_datos(symbol=symbol, timeframe='minute', range='15', days=5)
        return df

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
    
    # Verificar si el diccionario está vacío
    if not pares_en_tendencia:
        print("El diccionario de pares en tendencia está vacío. No se calcularán reversiones.")
    else:
        # Analizar reversiones
        resultados_reversiones = reversal_analyzer.analizar_reversiones(pares_en_tendencia)
        
        # Opcionalmente, imprimir todos los resultados
        for pair, resultado in resultados_reversiones.items():
            print(f"{pair}: {resultado}")

