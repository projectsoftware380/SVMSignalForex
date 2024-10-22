import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, timezone
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

# Configurar logger específico
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class ForexAnalyzer:
    def __init__(self, db_config, pairs):
        """Inicializa el analizador Forex."""
        self.pairs = pairs
        self.engine = create_engine(
            f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}/{db_config['database']}"
        )
        self.lock = Lock()  # Control de concurrencia

    def obtener_ultimo_timestamp_y_close(self):
        """Obtiene el último timestamp y precio de cierre desde forex_data_4h."""
        query = text("""
            SELECT timestamp, close 
            FROM forex_data_4h 
            ORDER BY timestamp DESC 
            LIMIT 1;
        """)
        try:
            with self.engine.connect() as conn:
                result = conn.execute(query).fetchone()
                if result:
                    logger.info(f"Último timestamp y close obtenido: {result[0]}, {result[1]}")
                    return result[0], float(result[1])
                else:
                    logger.warning("No se encontró ningún timestamp en forex_data_4h.")
                    return None, None
        except Exception as e:
            logger.error(f"Error al obtener el último timestamp: {str(e)}", exc_info=True)
            return None, None

    def obtener_precio_por_timestamp(self, pair, timestamp):
        """Obtiene el precio de cierre exacto según el timestamp."""
        query = text("""
            SELECT close 
            FROM forex_data_4h 
            WHERE timestamp = :timestamp 
            AND pair = :pair 
            LIMIT 1;
        """)
        try:
            with self.engine.connect() as conn:
                result = conn.execute(query, {"timestamp": timestamp, "pair": pair}).fetchone()
                if result:
                    logger.info(f"Precio encontrado para {pair} en {timestamp}: {result[0]}")
                    return float(result[0])
                else:
                    logger.warning(f"No se encontró precio de cierre para {pair} en {timestamp}.")
                    return None
        except Exception as e:
            logger.error(f"Error al obtener precio para {pair}: {str(e)}", exc_info=True)
            return None

    def obtener_datos_validos(self, pair):
        """Obtiene los 156 registros más recientes para el par especificado."""
        query = text("""
            SELECT timestamp, open, high, low, close 
            FROM forex_data_4h 
            WHERE pair = :pair 
            ORDER BY timestamp DESC 
            LIMIT 156;
        """)
        try:
            with self.engine.connect() as conn:
                df = pd.read_sql(query, conn, params={"pair": pair})
                if not df.empty:
                    logger.info(f"Datos obtenidos para {pair}. Último timestamp: {df.iloc[0]['timestamp']}")
                    df = df.sort_values(by='timestamp', ascending=True)  # Ordenar en orden ascendente
                    df = df.dropna(subset=['high', 'low', 'close'])  # Eliminar filas con NaN
                    return df, df.iloc[-1]['timestamp']  # Retornar el último timestamp válido
                else:
                    logger.warning(f"Datos insuficientes para {pair}.")
                    return pd.DataFrame(), None
        except Exception as e:
            logger.error(f"Error al obtener datos para {pair}: {str(e)}", exc_info=True)
            return pd.DataFrame(), None

    def registrar_tendencia(self, pair, tipo_tendencia, precio_actual, timestamp=None):
        """Registra la tendencia en la tabla de tendencias."""
        timestamp = timestamp or datetime.now(timezone.utc)

        query = text("""
            INSERT INTO tendencias (timestamp, par_de_divisas, tipo_tendencia, origen, precio_actual)
            VALUES (:timestamp, :pair, :tipo_tendencia, :origen, :precio_actual)
            ON CONFLICT (timestamp, par_de_divisas) DO UPDATE 
            SET tipo_tendencia = EXCLUDED.tipo_tendencia, precio_actual = EXCLUDED.precio_actual
            RETURNING id;
        """)
        try:
            with self.lock, self.engine.begin() as conn:
                result = conn.execute(query, {
                    "timestamp": timestamp,
                    "pair": pair,
                    "tipo_tendencia": tipo_tendencia,
                    "origen": "ForexAnalyzer",
                    "precio_actual": precio_actual
                })
                inserted_id = result.fetchone()[0]
                logger.info(f"Tendencia registrada correctamente para {pair} con ID {inserted_id}")
        except Exception as e:
            logger.error(f"Error al registrar tendencia para {pair}: {str(e)}", exc_info=True)

    def analizar_par(self, pair):
        """Analiza un par de divisas y registra la tendencia."""
        df, timestamp = self.obtener_datos_validos(pair)
        if df.empty:
            return

        # Calcular el indicador Ichimoku
        df['Tenkan-sen'] = (df['high'].rolling(window=9).max() + df['low'].rolling(window=9).min()) / 2
        df['Kijun-sen'] = (df['high'].rolling(window=26).max() + df['low'].rolling(window=26).min()) / 2
        df['Senkou Span A'] = ((df['Tenkan-sen'] + df['Kijun-sen']) / 2).shift(26)
        df['Senkou Span B'] = ((df['high'].rolling(window=52).max() + df['low'].rolling(window=52).min()) / 2).shift(26)

        ultimo_valor = df.dropna().iloc[-1]  # Tomar la última fila válida

        logger.info(
            f"Valores Ichimoku para {pair} - Tenkan-sen: {ultimo_valor['Tenkan-sen']}, "
            f"Kijun-sen: {ultimo_valor['Kijun-sen']}, Senkou Span A: {ultimo_valor['Senkou Span A']}, "
            f"Senkou Span B: {ultimo_valor['Senkou Span B']}"
        )

        tendencia = None
        if ultimo_valor['Tenkan-sen'] > ultimo_valor['Kijun-sen']:
            tendencia = 'alcista'
        elif ultimo_valor['Tenkan-sen'] < ultimo_valor['Kijun-sen']:
            tendencia = 'bajista'

        logger.info(f"Tendencia determinada para {pair}: {tendencia}")

        if tendencia:
            precio_actual = self.obtener_precio_por_timestamp(pair, timestamp)
            if precio_actual is not None:
                self.registrar_tendencia(pair, tendencia, precio_actual)

    def analizar_pares(self):
        """Analiza todos los pares en paralelo."""
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = {executor.submit(self.analizar_par, pair): pair for pair in self.pairs}
            for future in as_completed(futures):
                pair = futures[future]
                try:
                    future.result()
                    logger.info(f"Análisis completado para {pair}.")
                except Exception as e:
                    logger.error(f"Error al analizar {pair}: {str(e)}", exc_info=True)
