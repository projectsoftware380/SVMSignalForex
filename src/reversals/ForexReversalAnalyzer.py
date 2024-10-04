import requests
import pandas as pd
import pandas_ta as ta
from datetime import datetime, timedelta, timezone
import pytz
import logging
import os
import json
import threading
import time

# Verificar si el directorio 'logs' existe, si no, crearlo
logs_directory = os.path.join(os.path.dirname(__file__), '..', 'logs')
if not os.path.exists(logs_directory):
    os.makedirs(logs_directory)

# Configurar logging
logging.basicConfig(
    filename=os.path.join(logs_directory, 'reversal_server.log'),
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Cargar configuración desde config.json
CONFIG_FILE = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.json')
with open(CONFIG_FILE, "r") as f:
    config = json.load(f)

class ForexReversalAnalyzer:
    def __init__(self, api_key_polygon=None):
        self.api_key_polygon = api_key_polygon

    def obtener_hora_colombia(self):
        """Obtiene la hora actual en la zona horaria de Colombia."""
        zona_colombia = pytz.timezone('America/Bogota')
        hora_actual = datetime.now(zona_colombia)
        return hora_actual.strftime('%Y-%m-%d %H:%M:%S')

    def obtener_hora_servidor(self):
        """Obtiene la hora actual del servidor de Polygon.io."""
        url = f"https://api.polygon.io/v1/marketstatus/now?apiKey={self.api_key_polygon}"
        try:
            response = requests.get(url)
            response.raise_for_status()
            server_time = response.json().get("serverTime", None)
            if server_time:
                return datetime.strptime(server_time, '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=pytz.UTC)
            else:
                raise ValueError("No se pudo obtener la hora del servidor.")
        except Exception as e:
            logging.error(f"Error al obtener la hora del servidor: {e}")
            return datetime.now(timezone.utc)  # Usar datetime.now con timezone UTC

    def obtener_datos_api(self, symbol, timeframe='minute', multiplier=15, horas=75):
        """Solicita datos de la API de Polygon.io y los devuelve en un DataFrame."""
        try:
            fecha_fin = self.obtener_hora_servidor()
            fecha_inicio = fecha_fin - timedelta(hours=horas)

            start_date = fecha_inicio.strftime('%Y-%m-%d')
            end_date = fecha_fin.strftime('%Y-%m-%d')

            symbol_polygon = symbol.replace("/", "").replace("-", "").upper()

            # Cambiar a sort=desc para ordenar de nuevo a viejo
            url = f"https://api.polygon.io/v2/aggs/ticker/C:{symbol_polygon}/range/{multiplier}/{timeframe}/{start_date}/{end_date}?apiKey={self.api_key_polygon}&sort=desc"
            logging.info(f"Consultando datos para {symbol} con URL: {url}")
            
            response = requests.get(url)
            response.raise_for_status()

            data = response.json()
            if 'results' in data:
                df = pd.DataFrame(data['results'])
                df['timestamp'] = pd.to_datetime(df['t'], unit='ms', utc=True)
                df.set_index('timestamp', inplace=True)
                logging.info(f"Datos obtenidos para {symbol}: {df.shape[0]} filas.")

                # Usar todos los datos para indicadores y tomar solo el penúltimo valor de 'Close'
                penultimo_close = df[['c']].iloc[0]  # El último valor de cierre
                logging.info(f"Penúltimo valor de cierre para {symbol}: {penultimo_close['c']}")

                return df[['o', 'h', 'l', 'c']], penultimo_close
            else:
                logging.warning(f"No se encontraron resultados en la API para {symbol}.")
                return pd.DataFrame(), pd.DataFrame()
        except Exception as e:
            logging.error(f"Error al obtener datos de la API para {symbol}: {e}")
            return pd.DataFrame(), pd.DataFrame()

    def calcular_fibonacci(self, df):
        """Calcula los niveles de Fibonacci basados en los últimos 20 períodos."""
        max_price = df['High'].max()  # Precio máximo en el rango
        min_price = df['Low'].min()   # Precio mínimo en el rango
        rango = max_price - min_price  # Rango total

        # Retrocesos desde el precio máximo para un retroceso alcista
        fibonacci_alcista = {
            '23.6%': max_price - 0.236 * rango,
            '38.2%': max_price - 0.382 * rango,
            '76.8%': max_price - 0.768 * rango
        }

        # Retrocesos desde el precio mínimo para un retroceso bajista
        fibonacci_bajista = {
            '23.6%': min_price + 0.236 * rango,
            '38.2%': min_price + 0.382 * rango,
            '76.8%': min_price + 0.768 * rango
        }

        logging.info(f"Niveles de Fibonacci Alcista calculados: {fibonacci_alcista}")
        logging.info(f"Niveles de Fibonacci Bajista calculados: {fibonacci_bajista}")
        
        return fibonacci_alcista, fibonacci_bajista

    def obtener_datos_bollinger(self, symbol):
        """Obtiene y calcula las Bandas de Bollinger para el símbolo."""
        df, penultimo_close = self.obtener_datos_api(symbol)
        if df.empty:
            raise ValueError(f"Los datos obtenidos para {symbol} no son suficientes o están vacíos.")

        df.columns = ['Open', 'High', 'Low', 'Close']
        for col in ['Open', 'High', 'Low', 'Close']:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        df = df.dropna()

        if len(df) < 1:
            raise ValueError(f"No hay suficientes datos para calcular las Bandas de Bollinger para {symbol}.")

        bollinger = ta.bbands(df['Close'], length=20, std=2)
        df['mid'] = bollinger['BBM_20_2.0']  # Banda central
        df['upper'] = bollinger['BBU_20_2.0']
        df['lower'] = bollinger['BBL_20_2.0']

        logging.info(f"Bollinger Bandas calculadas. Línea central: {df['mid'].iloc[0]}")
        return df, penultimo_close

    def detectar_retroceso(self, df, fibonacci_alcista, fibonacci_bajista):
        """Detecta un retroceso basado en Fibonacci y las Bandas de Bollinger."""
        try:
            # Usar la primera vela (más reciente) ya formada
            precio_close = df['Close'].iloc[0]  
            banda_media = df['mid'].iloc[0]  

            # Reversión Alcista: Si el precio está por debajo del 23.6% y por encima del menor entre 76.8% y la banda media
            if precio_close < fibonacci_alcista['23.6%'] and precio_close > min(banda_media, fibonacci_alcista['76.8%']):
                logging.info(f"Retroceso Alcista detectado en {df.index[0]} con precio de cierre {precio_close}")
                return "Reversión Alcista"

            # Reversión Bajista: Si el precio está por encima del 23.6% y por debajo del mayor entre 76.8% y la banda media
            if precio_close > fibonacci_bajista['23.6%'] and precio_close < max(banda_media, fibonacci_bajista['76.8%']):
                logging.info(f"Retroceso Bajista detectado en {df.index[0]} con precio de cierre {precio_close}")
                return "Reversión Bajista"

        except Exception as e:
            logging.error(f"Error al detectar retroceso: {e}")

        return "Sin Reversión"

    def analizar_reversion_para_par(self, symbol):
        """Función que analiza las reversiones para un par de divisas."""
        try:
            logging.info(f"Analizando reversión para {symbol}")
            df, penultimo_close = self.obtener_datos_bollinger(symbol)
            fibonacci_alcista, fibonacci_bajista = self.calcular_fibonacci(df)
            resultado_reversion = self.detectar_retroceso(df, fibonacci_alcista, fibonacci_bajista)
            
            # Agregar fecha y hora de Colombia al log
            hora_colombia = self.obtener_hora_colombia()
            logging.info(f"Análisis de reversión para {symbol} completado a las {hora_colombia} (hora de Colombia).")
            
            return resultado_reversion
        except ValueError as e:
            logging.error(f"Error en el análisis para {symbol}: {str(e)}")
        except TypeError as e:
            logging.error(f"Error de tipo en {symbol}: {str(e)}")
        except Exception as e:
            logging.error(f"Error inesperado en {symbol}: {str(e)}")
        return None

    def analizar_reversiones(self):
        """Analiza todos los pares de divisas que están en config.json."""
        pares_a_analizar = config['pairs']
        resultados = {}

        logging.info(f"Analizando reversiones para los pares: {pares_a_analizar}")

        for pair in pares_a_analizar:
            reversion = self.analizar_reversion_para_par(pair)
            if reversion:
                resultados[pair] = reversion

        return resultados

    def tiempo_para_proxima_vela(self):
        """Calcula el tiempo restante hasta la próxima vela de 15 minutos."""
        ahora = datetime.now(timezone.utc)  # Usar timezone-aware UTC object
        proxima_vela = ahora.replace(minute=(ahora.minute // 15) * 15, second=0, microsecond=0) + timedelta(minutes=15)
        return (proxima_vela - ahora).total_seconds()

    def ejecutar_analisis_cuando_nueva_vela(self):
        """Ejecuta el análisis de reversiones sincronizado con la creación de nuevas velas de 15 minutos."""
        while True:
            # Calcular el tiempo hasta la próxima vela de 15 minutos
            tiempo_restante = self.tiempo_para_proxima_vela()
            logging.info(f"Esperando {tiempo_restante} segundos para la próxima vela de 15 minutos.")
            time.sleep(tiempo_restante)  # Esperar hasta la nueva vela
            logging.info("Iniciando análisis de reversión con nueva vela de 15 minutos.")
            self.analizar_reversiones()

# Crear un hilo para ejecutar el análisis sincronizado con la creación de nuevas velas de 15 minutos
def iniciar_hilo_analisis():
    reversal_analyzer = ForexReversalAnalyzer(api_key_polygon=config['api_key_polygon'])
    hilo_analisis = threading.Thread(target=reversal_analyzer.ejecutar_analisis_cuando_nueva_vela)
    hilo_analisis.daemon = True  # Hilo como demonio
    hilo_analisis.start()

if __name__ == "__main__":
    iniciar_hilo_analisis()
    while True:
        time.sleep(1)  # Mantener el script corriendo
