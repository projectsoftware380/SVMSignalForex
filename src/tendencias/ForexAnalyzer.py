import os
import pandas as pd
import requests
from datetime import datetime, timedelta
import pytz
import threading
import time
import logging
import json
from dateutil import parser
from concurrent.futures import ThreadPoolExecutor

# Configurar un logger específico para este módulo
logger = logging.getLogger(__name__)

class ForexAnalyzer:
    def __init__(self, api_key_polygon, pairs):
        """
        Inicializa el analizador Forex con la API key y la lista de pares.

        :param api_key_polygon: API key para acceder a Polygon.io
        :param pairs: Lista de pares de divisas a analizar
        """
        self.api_key_polygon = api_key_polygon
        self.pairs = pairs  # Lista de pares de divisas para analizar
        self.last_trend = {}  # Almacena las tendencias de cada par
        self.lock = threading.Lock()  # Proteger el acceso a recursos compartidos
        self.base_url = 'https://api.polygon.io'

    def obtener_hora_servidor(self):
        """Obtiene la hora actual del servidor de Polygon.io."""
        url = f"{self.base_url}/v1/marketstatus/now?apiKey={self.api_key_polygon}"
        for intento in range(3):
            try:
                response = requests.get(url)
                response.raise_for_status()
                server_time = response.json().get("serverTime", None)
                if server_time:
                    # Utilizar dateutil.parser para manejar la zona horaria
                    server_time_parsed = parser.isoparse(server_time).astimezone(pytz.UTC)
                    logger.info(f"Hora del servidor obtenida: {server_time_parsed}")
                    return server_time_parsed
            except requests.exceptions.RequestException as e:
                logger.warning(f"Fallo al obtener hora del servidor, intento {intento + 1}: {e}")
                time.sleep(5)
        logger.error("No se pudo obtener la hora del servidor tras 3 intentos.")
        return datetime.utcnow().replace(tzinfo=pytz.UTC)

    def tiempo_para_proxima_vela(self):
        """Calcula el tiempo restante hasta la próxima vela de 4 horas."""
        ahora = datetime.utcnow().replace(tzinfo=pytz.UTC)
        proxima_vela = ahora.replace(minute=0, second=0, microsecond=0)
        while proxima_vela <= ahora:
            proxima_vela += timedelta(hours=4)
        tiempo_restante = (proxima_vela - ahora).total_seconds()
        logger.info(f"Tiempo restante para la próxima vela: {tiempo_restante} segundos")
        return tiempo_restante, proxima_vela

    def obtener_datos_polygon(self, symbol, timeframe='minute', multiplier=4, start_date=None, end_date=None):
        """Obtiene datos de mercado de Polygon.io para un símbolo específico."""
        base_url = f"{self.base_url}/v2/aggs/ticker/C:{symbol}/range/{multiplier}/{timeframe}/{start_date}/{end_date}"
        params = {
            'apiKey': self.api_key_polygon,
            'limit': 50000,  # Límite de datos
            'sort': 'asc'
        }
        all_data = []
        for intento in range(3):
            try:
                url = base_url
                while True:
                    response = requests.get(url, params=params)
                    response.raise_for_status()
                    data = response.json()
                    results = data.get('results', [])
                    all_data.extend(results)
                    if 'next_url' not in data:
                        break
                    url = data['next_url']
                logger.info(f"Datos obtenidos para {symbol}: {len(all_data)} registros")
                break
            except requests.exceptions.RequestException as e:
                logger.warning(f"Fallo al obtener datos para {symbol}, intento {intento + 1}: {e}")
                time.sleep(5)
        if not all_data:
            logger.error(f"Datos insuficientes para {symbol}.")
            return pd.DataFrame()
        df = pd.DataFrame(all_data)
        df['timestamp'] = pd.to_datetime(df['t'], unit='ms', utc=True)
        df.set_index('timestamp', inplace=True)
        df.rename(columns={'h': 'High', 'l': 'Low', 'c': 'Close', 'o': 'Open'}, inplace=True)
        return df[['High', 'Low', 'Close', 'Open']]

    def calcular_ichimoku(self, df):
        """Calcula los componentes del indicador Ichimoku."""
        if len(df) < 156:
            logger.warning(f"Datos insuficientes para calcular Ichimoku. Se requieren 156 períodos.")
            return pd.DataFrame()
        df['Tenkan-sen'] = (df['High'].rolling(window=9).max() + df['Low'].rolling(window=9).min()) / 2
        df['Kijun-sen'] = (df['High'].rolling(window=26).max() + df['Low'].rolling(window=26).min()) / 2
        df['Senkou Span A'] = ((df['Tenkan-sen'] + df['Kijun-sen']) / 2).shift(26)
        df['Senkou Span B'] = ((df['High'].rolling(window=52).max() + df['Low'].rolling(window=52).min()) / 2).shift(26)
        df['Chikou Span'] = df['Close'].shift(-26)
        df_dropna = df.dropna()
        if df_dropna.empty:
            logger.warning("No hay suficientes datos después de calcular Ichimoku (NaNs presentes).")
            return df
        ultimo_valor = df_dropna.iloc[-1]
        logger.info(f"Últimos valores de Ichimoku:\nTenkan-sen: {ultimo_valor['Tenkan-sen']}, Kijun-sen: {ultimo_valor['Kijun-sen']},\n"
                    f"Senkou Span A: {ultimo_valor['Senkou Span A']}, Senkou Span B: {ultimo_valor['Senkou Span B']},\n"
                    f"Chikou Span: {ultimo_valor['Chikou Span']}")
        return df

    def obtener_datos_validos(self, symbol_polygon):
        """Obtiene los datos de mercado válidos para un símbolo."""
        fecha_actual_servidor = self.obtener_hora_servidor()
        fecha_inicio_utc = fecha_actual_servidor - timedelta(days=200)
        start_date = fecha_inicio_utc.strftime('%Y-%m-%d')
        end_date = fecha_actual_servidor.strftime('%Y-%m-%d')
        df = self.obtener_datos_polygon(symbol_polygon, 'hour', multiplier=4, start_date=start_date, end_date=end_date)
        logger.info(f"Número de registros obtenidos para {symbol_polygon}: {len(df)}")
        if df.empty or len(df) < 156:
            logger.warning(f"Datos insuficientes para {symbol_polygon}. Se requieren al menos 156 registros.")
            return pd.DataFrame()
        df = df.tail(156)
        logger.info(f"Datos de {symbol_polygon} recortados a los últimos 156 registros.")
        return df

    def validar_actualizacion_reciente(self, df):
        """Valida si los datos son recientes comparando con la hora del servidor."""
        ultima_entrada = df.index[-1]
        server_time = self.obtener_hora_servidor()
        diferencia = server_time - ultima_entrada
        logger.info(f"Diferencia entre la última entrada y el tiempo del servidor: {diferencia}")
        return diferencia <= timedelta(hours=4)

    def analizar_par(self, pair):
        """Analiza el par de divisas usando Ichimoku y guarda la tendencia."""
        try:
            logger.info(f"Analizando par: {pair}")
            symbol_polygon = pair.replace("-", "")
            df = self.obtener_datos_validos(symbol_polygon)
            if df.empty:
                with self.lock:
                    self.last_trend[pair] = "Datos insuficientes"
                logger.warning(f"Datos insuficientes para {pair}")
                return "Datos insuficientes"
            df = self.calcular_ichimoku(df)
            if df.empty:
                with self.lock:
                    self.last_trend[pair] = "Datos insuficientes"
                logger.warning(f"Datos insuficientes después de calcular Ichimoku para {pair}")
                return "Datos insuficientes"
            df_dropna = df.dropna()
            if df_dropna.empty:
                with self.lock:
                    self.last_trend[pair] = "Datos insuficientes"
                logger.warning(f"Datos insuficientes después de eliminar NaNs para {pair}")
                return "Datos insuficientes"
            ultimo_valor = df_dropna.iloc[-1]
            logger.info(f"Último valor para {pair}:\n{ultimo_valor[['Tenkan-sen', 'Kijun-sen', 'Close', 'Senkou Span A', 'Senkou Span B']]}")
            if ultimo_valor['Tenkan-sen'] > ultimo_valor['Kijun-sen'] and ultimo_valor['Close'] > ultimo_valor['Senkou Span A']:
                tendencia = "Tendencia Alcista"
            elif ultimo_valor['Tenkan-sen'] < ultimo_valor['Kijun-sen'] and ultimo_valor['Close'] < ultimo_valor['Senkou Span B']:
                tendencia = "Tendencia Bajista"
            else:
                tendencia = "Neutral"
            logger.info(f"Tendencia determinada para {pair}: {tendencia}")
            with self.lock:
                self.last_trend[pair] = tendencia
            return tendencia
        except Exception as e:
            logger.error(f"Error al analizar par {pair}: {str(e)}")
            with self.lock:
                self.last_trend[pair] = "Error"
            return "Error"

    def analizar_pares(self):
        """Analiza todos los pares de divisas utilizando un ThreadPoolExecutor."""
        logger.info("Iniciando análisis de todos los pares.")
        with ThreadPoolExecutor(max_workers=5) as executor:
            executor.map(self.analizar_par, self.pairs)
        logger.info("Análisis de todos los pares completado.")
        with self.lock:
            return self.last_trend.copy()

    def guardar_tendencias_en_json(self, timestamp_utc):
        """Guarda las tendencias en un archivo JSON con el timestamp en hora de Colombia."""
        zona_colombia = pytz.timezone('America/Bogota')
        timestamp_colombia = timestamp_utc.astimezone(zona_colombia)
        timestamp_legible = timestamp_colombia.strftime('%Y-%m-%d %H:%M:%S')
        with self.lock:
            tendencias = self.last_trend.copy()
            tendencias['last_timestamp'] = timestamp_legible
            with open('src/data/tendencias.json', 'w') as f:
                json.dump(tendencias, f, indent=4)
        logger.info(f"Tendencias guardadas con timestamp: {timestamp_legible}")

    def sincronizar_con_nueva_vela(self):
        """Sincroniza el cálculo de tendencias con la aparición de nuevas velas de 4 horas."""
        while True:
            try:
                tiempo_restante, _ = self.tiempo_para_proxima_vela()
                logger.info(f"Esperando {tiempo_restante} segundos para la próxima vela.")
                time.sleep(tiempo_restante)
                logger.info("Iniciando análisis después de nueva vela.")
                self.analizar_pares()
                self.guardar_tendencias_en_json(datetime.utcnow().replace(tzinfo=pytz.UTC))
                logger.info("Análisis y guardado de tendencias completado.")
                time.sleep(3600)  # Esperar hasta que se acerque la próxima vela
            except Exception as e:
                logger.error(f"Error en sincronizar_con_nueva_vela: {str(e)}")
                time.sleep(60)  # Esperar antes de reintentar
