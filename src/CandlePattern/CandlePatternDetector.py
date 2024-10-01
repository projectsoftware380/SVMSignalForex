import requests
import pandas as pd
import talib
import logging
import os
from datetime import datetime, timedelta
import pytz

# Configuración básica de logging
logging.basicConfig(
    filename=os.path.join(os.path.dirname(__file__), '..', 'logs', 'candle_pattern_analyzer.log'),
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class CandlePatternAnalyzer:
    def __init__(self, api_key_polygon):
        self.api_key_polygon = api_key_polygon

    def obtener_datos_api(self, symbol, timeframe='minute', multiplier=3, horas=12):
        """
        Solicita datos directamente a la API de Polygon.io para el símbolo dado.
        Se toma la penúltima vela para evitar el análisis de velas en formación.
        """
        try:
            logging.info(f"Solicitando datos para {symbol} desde la API de Polygon.io")
            fecha_fin = datetime.utcnow().replace(tzinfo=pytz.UTC)
            fecha_inicio = fecha_fin - timedelta(hours=horas)

            start_date = fecha_inicio.strftime('%Y-%m-%d')
            end_date = fecha_fin.strftime('%Y-%m-%d')

            symbol_polygon = symbol.replace("/", "").replace("-", "").upper()

            url = f"https://api.polygon.io/v2/aggs/ticker/C:{symbol_polygon}/range/{multiplier}/{timeframe}/{start_date}/{end_date}?apiKey={self.api_key_polygon}&sort=asc"
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()

            if 'results' in data:
                df = pd.DataFrame(data['results'])
                df['timestamp'] = pd.to_datetime(df['t'], unit='ms', utc=True)
                df.set_index('timestamp', inplace=True)

                if df.empty:
                    logging.warning(f"No se obtuvieron suficientes datos para {symbol}.")
                    return pd.DataFrame()

                logging.info(f"Datos obtenidos correctamente para {symbol}: {df.shape[0]} filas.")
                
                # Retornamos todos los datos menos la última vela en formación
                return df[['o', 'h', 'l', 'c']].iloc[:-1]  # open, high, low, close, menos la última fila (vela)
            else:
                logging.warning(f"No se encontraron resultados en la respuesta para {symbol}.")
                return pd.DataFrame()

        except requests.exceptions.RequestException as e:
            logging.error(f"Error al obtener datos de la API para {symbol}: {e}")
            return pd.DataFrame()

    def detectar_patrones_talib(self, df):
        """
        Detecta patrones de velas japonesas utilizando la librería TA-Lib.
        Devuelve un diccionario con los patrones detectados.
        """
        patrones = {}
        patrones_alcistas = ['CDLHAMMER', 'CDLINVERTEDHAMMER', 'CDLPIERCING', 'CDLMORNINGSTAR', 'CDL3WHITESOLDIERS']
        patrones_bajistas = ['CDLSHOOTINGSTAR', 'CDLENGULFING', 'CDLDARKCLOUDCOVER', 'CDLEVENINGSTAR', 'CDL3BLACKCROWS']

        try:
            # TA-Lib pattern recognition
            for pattern in talib.get_function_groups()['Pattern Recognition']:
                result = getattr(talib, pattern)(df['o'], df['h'], df['l'], df['c'])

                # Penúltimo valor indica el patrón detectado (positivo para alcista, negativo para bajista)
                if result.iloc[-2] > 0 and pattern in patrones_alcistas:
                    patrones[pattern] = 'alcista'
                elif result.iloc[-2] < 0 and pattern in patrones_bajistas:
                    patrones[pattern] = 'bajista'

            if patrones:
                logging.info(f"Patrones detectados: {patrones}")
            else:
                logging.info(f"No se detectaron patrones.")
        except Exception as e:
            logging.error(f"Error al detectar patrones con TA-Lib: {e}")

        return patrones

    def detectar_patrones_para_par(self, symbol):
        """
        Detecta patrones de velas japonesas para un par de divisas específico en 4h, 15m y 3m.
        """
        logging.info(f"Iniciando la detección de patrones para {symbol}.")

        patrones_resultantes = {}

        # Detección en diferentes temporalidades
        temporalidades = {
            '4h': {'timeframe': 'minute', 'multiplier': 240, 'horas': 96},  # 4 horas
            '15m': {'timeframe': 'minute', 'multiplier': 15, 'horas': 48},  # 15 minutos
            '3m': {'timeframe': 'minute', 'multiplier': 3, 'horas': 12}     # 3 minutos
        }

        for temporalidad, config in temporalidades.items():
            df = self.obtener_datos_api(symbol, timeframe=config['timeframe'], multiplier=config['multiplier'], horas=config['horas'])
            if not df.empty:
                patrones_detectados = self.detectar_patrones_talib(df)
                if patrones_detectados:
                    patrones_resultantes[temporalidad] = patrones_detectados

        return patrones_resultantes
