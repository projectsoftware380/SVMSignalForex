import psycopg2
import logging
import os
import time
from datetime import datetime, timezone

# Configuración del logger
log_dir = os.path.join(os.path.dirname(__file__), '..', 'logs')
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

logging.basicConfig(
    filename=os.path.join(log_dir, 'DataBase.log'),
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)

class DatabaseManager:
    def __init__(self, db_config, max_retries=3, retry_delay=5, batch_size=100):
        self.db_config = db_config
        self.max_retries = max_retries  # Número máximo de reintentos
        self.retry_delay = retry_delay  # Intervalo entre reintentos (segundos)
        self.batch_size = batch_size  # Tamaño del batch para inserciones

    def conectar_db(self):
        """Establecer conexión con la base de datos PostgreSQL con reintentos."""
        for attempt in range(self.max_retries):
            try:
                conn = psycopg2.connect(
                    host=self.db_config["host"],
                    database=self.db_config["database"],
                    user=self.db_config["user"],
                    password=self.db_config["password"]
                )
                logging.info("Conexión exitosa a la base de datos.")
                return conn
            except Exception as error:
                logging.error(f"Intento {attempt + 1} - Error al conectar a la base de datos: {error}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)  # Espera antes de reintentar
                else:
                    logging.error("No se pudo establecer conexión después de varios intentos.")
                    return None

    def verificar_timestamp(self, conn, pair, timeframe):
        """Verifica el último timestamp almacenado en la base de datos para asegurarse de que los datos están sincronizados."""
        cursor = conn.cursor()
        table_name = f'forex_data_{timeframe}'
        query = f"SELECT MAX(timestamp) FROM {table_name} WHERE pair = %s"
        cursor.execute(query, (pair,))
        ultimo_timestamp = cursor.fetchone()[0]
        cursor.close()

        if ultimo_timestamp is None:
            logging.info(f"No se encontraron registros previos para {pair} en timeframe {timeframe}.")
        else:
            logging.info(f"Último timestamp para {pair} en timeframe {timeframe}: {ultimo_timestamp}")
        return ultimo_timestamp

    def insertar_datos(self, conn, datos, pair, timeframe):
        """Inserta los datos obtenidos en la tabla PostgreSQL en bloques pequeños."""
        if not datos.get("results"):
            logging.warning(f"No hay datos para insertar para {pair} en timeframe {timeframe}")
            return

        cursor = conn.cursor()
        table_name = f'forex_data_{timeframe}'
        
        try:
            count = 0
            batch = []
            ultimo_timestamp = self.verificar_timestamp(conn, pair, timeframe)
            if ultimo_timestamp is None:
                ultimo_timestamp = datetime.utcfromtimestamp(0)  # Insertar todos los datos si no hay registros previos

            for result in datos["results"]:
                timestamp = result['t'] / 1000  # Convertir a segundos
                timestamp_dt = datetime.utcfromtimestamp(timestamp)
                if timestamp_dt > ultimo_timestamp:  # Validar si es más reciente que el último timestamp
                    open_price = result['o']
                    close_price = result['c']
                    high_price = result['h']
                    low_price = result['l']
                    volume = result['v']

                    batch.append((timestamp, pair, open_price, close_price, high_price, low_price, volume))
                    
                    query = f"""
                    INSERT INTO {table_name} (timestamp, pair, open, close, high, low, volume)
                    VALUES (to_timestamp(%s), %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (timestamp, pair) DO NOTHING
                    """
                    count += 1

                    if count % self.batch_size == 0:
                        cursor.executemany(query, batch)
                        conn.commit()
                        batch = []

            if batch:
                cursor.executemany(query, batch)
                conn.commit()
            logging.info(f"Datos insertados para {pair} en timeframe {timeframe} en la tabla {table_name}.")

        except psycopg2.Error as e:
            logging.error(f"Error al insertar datos para {pair}: {e}")
            conn.rollback()
        finally:
            cursor.close()

    def insertar_datos_realtime(self, conn, datos_realtime, pair, timeframe='3m'):
        """Inserta los datos en tiempo real recibidos del WebSocket."""
        cursor = conn.cursor()
        table_name = f'forex_data_{timeframe}'
        try:
            for data in datos_realtime:
                timestamp = data['t'] / 1000  # Convertir a segundos
                open_price = data['o']
                close_price = data['c']
                high_price = data['h']
                low_price = data['l']
                volume = data['v']

                query = f"""
                INSERT INTO {table_name} (timestamp, pair, open, close, high, low, volume)
                VALUES (to_timestamp(%s), %s, %s, %s, %s, %s, %s)
                ON CONFLICT (timestamp, pair) DO NOTHING
                """
                cursor.execute(query, (timestamp, pair, open_price, close_price, high_price, low_price, volume))

            conn.commit()
            logging.info(f"Datos en tiempo real insertados para {pair} en timeframe {timeframe}.")

        except psycopg2.Error as e:
            logging.error(f"Error al insertar datos en tiempo real para {pair}: {e}")
            conn.rollback()
        finally:
            cursor.close()

    def eliminar_datos_antiguos(self, timeframe, retention_period):
        """Eliminar datos antiguos según el timeframe y periodo de retención."""
        conn = self.conectar_db()
        if not conn:
            return

        cursor = conn.cursor()
        table_name = f'forex_data_{timeframe}'
        
        try:
            query = f"""
            DELETE FROM {table_name}
            WHERE timestamp < NOW() - INTERVAL %s
            """
            cursor.execute(query, (retention_period,))
            conn.commit()
            logging.info(f"Datos antiguos eliminados para la tabla {table_name} con retención de {retention_period}.")
        except Exception as e:
            logging.error(f"Error al eliminar datos antiguos: {e}")
            conn.rollback()
        finally:
            cursor.close()
            conn.close()

    def monitorear_insercion(self, conn, pair, timeframe):
        """Monitorea el tiempo de inserción y registros duplicados para identificar cuellos de botella."""
        try:
            cursor = conn.cursor()
            table_name = f'forex_data_{timeframe}'
            query = f"SELECT COUNT(*), MAX(timestamp), MIN(timestamp) FROM {table_name} WHERE pair = %s"
            cursor.execute(query, (pair,))
            count, max_timestamp, min_timestamp = cursor.fetchone()
            cursor.close()

            logging.info(f"Monitoreo de inserción para {pair} en timeframe {timeframe}: Total registros: {count}, Rango de timestamps: {min_timestamp} a {max_timestamp}")

            if count > 100000:
                logging.warning(f"Posible cuello de botella en la inserción para {pair}. Hay más de 100,000 registros.")
            
            if (datetime.now(timezone.utc) - max_timestamp).total_seconds() > 180:
                logging.warning(f"Retraso significativo en la inserción de datos para {pair} en {timeframe}. Último timestamp: {max_timestamp}")

        except Exception as e:
            logging.error(f"Error al monitorear la inserción de datos: {e}")
