import requests
import pandas as pd
import pandas_ta as ta
from datetime import datetime, timedelta

class ForexSignalAnalyzer:
    def __init__(self, api_key_polygon):
        self.api_key_polygon = api_key_polygon
        self.operaciones_abiertas = {}  # Diccionario para almacenar las operaciones abiertas

    def obtener_datos_rsi(self, symbol):
        # Obtener datos en un intervalo de 3 minutos desde 5 días antes hasta la fecha actual en UTC
        fecha_inicio = (datetime.utcnow() - timedelta(days=5)).strftime('%Y-%m-%d')
        fecha_fin = datetime.utcnow().strftime('%Y-%m-%d')
        
        url = f"https://api.polygon.io/v2/aggs/ticker/C:{symbol}/range/3/minute/{fecha_inicio}/{fecha_fin}"
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

    def verificar_operacion_abierta(self, symbol, tipo):
        # Verifica si ya hay una operación abierta con el mismo símbolo y tipo (compra/venta)
        return self.operaciones_abiertas.get(symbol) == tipo

    def generar_senal_trading(self, df, reverso_tendencia, symbol):
        # Calcular RSI de 2 períodos
        rsi = ta.rsi(df['Close'], length=2)
        rsi_actual = rsi.iloc[-1]

        if reverso_tendencia == "Reversión Alcista Detectada":
            if rsi_actual < 20:
                if not self.verificar_operacion_abierta(symbol, "compra"):
                    self.operaciones_abiertas[symbol] = "compra"
                    return f"Señal de Compra Detectada (RSI Actual: {rsi_actual})"
            return f"No hay Señal de Compra (RSI Actual: {rsi_actual})"
        elif reverso_tendencia == "Reversión Bajista Detectada":
            if rsi_actual > 80:
                if not self.verificar_operacion_abierta(symbol, "venta"):
                    self.operaciones_abiertas[symbol] = "venta"
                    return f"Señal de Venta Detectada (RSI Actual: {rsi_actual})"
            return f"No hay Señal de Venta (RSI Actual: {rsi_actual})"
        else:
            return "Neutral - No se analiza señal"

    def analizar_senales(self, pares_reversiones):
        resultados = {}
        
        if not pares_reversiones:
            print("El diccionario de pares en reversión está vacío o no es válido.")
            return resultados
        
        print("Diccionario de pares en reversión recibido:")
        print(pares_reversiones)
        
        for pair, reverso_tendencia in pares_reversiones.items():
            if reverso_tendencia not in ["Reversión Alcista Detectada", "Reversión Bajista Detectada"]:
                print(f"{pair}: Reverso tendencia inválido o neutral, no se analiza señal.")
                resultados[pair] = "Neutral - No se analiza señal"
            else:
                try:
                    symbol_polygon = pair.replace("-", "")
                    df = self.obtener_datos_rsi(symbol_polygon)
                    resultado_senal = self.generar_senal_trading(df, reverso_tendencia, symbol_polygon)
                    resultados[pair] = resultado_senal
                    print(f"{pair}: {resultado_senal}")
                except ValueError as e:
                    resultados[pair] = f"Error en el análisis - {str(e)}"
        return resultados

# Ejemplo de uso
if __name__ == "__main__":
    api_key_polygon = "TU_API_KEY_POLYGON"
    
    # Instancia de ForexSignalAnalyzer
    signal_analyzer = ForexSignalAnalyzer(api_key_polygon)
    
    # Ejemplo de pares en reversión desde ForexReversalAnalyzer (Simulado)
    pares_reversiones = {
        "GBP-USD": "Reversión Alcista Detectada",
        "USD-CHF": "Reversión Bajista Detectada",
        "USD-JPY": "Neutral",
        "GBP-CAD": "Reversión Alcista Detectada",
        "USD-CAD": "Reversión Bajista Detectada"
    }
    
    # Analizar señales de trading
    resultados_senales = signal_analyzer.analizar_senales(pares_reversiones)
    
    for pair, resultado in resultados_senales.items():
        print(f"{pair}: {resultado}")
