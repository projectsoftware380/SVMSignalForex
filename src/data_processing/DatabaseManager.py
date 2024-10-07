import psycopg2
import logging
import os
import time
from datetime import datetime, timezone

# Configuración del logger directamente en el archivo
log_dir = os.path.join(os.path.dirname(__file__), '..', 'logs')
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

logging.basicConfig(
    filename=os.path.join(log_dir, 'DataBase.log'),
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)

class DatabaseManager:
    def __init__(self, db_config, max_retries=3, retry_delay=5):
        self.db_config = db_config
        self.max_retries = max_retries  # Número máximo de reintentos
        self.retry_delay = retry_delay  # Intervalo entre reintentos (segundos)

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
        return ultimo_timestamp

    def insertar_datos(self, conn, datos, pair, timeframe):
        """Inserta los datos obtenidos en la tabla PostgreSQL en bloques pequeños, dirigiendo a la tabla adecuada."""
        if not datos.get("results"):
            logging.warning(f"No hay datos para insertar para {pair} en timeframe {timeframe}")
            return

        cursor = conn.cursor()
        batch_size = 100  # Tamaño del batch para hacer commits cada 100 inserciones
        count = 0

        # Asignar la tabla según el timeframe
        if timeframe == '3m':
            table_name = 'forex_data_3m'
        elif timeframe == '15m':
            table_name = 'forex_data_15m'
        elif timeframe == '4h':
            table_name = 'forex_data_4h'
        else:
            logging.error(f"Timeframe {timeframe} no reconocido para la inserción de datos.")
            return

        try:
            for result in datos["results"]:
                timestamp = result['t']
                open_price = result['o']
                close_price = result['c']
                high_price = result['h']
                low_price = result['l']
                volume = result['v']

                # Verificación adicional para asegurar que los datos no son duplicados
                query = f"""
                INSERT INTO {table_name} (timestamp, pair, open, close, high, low, volume)
                VALUES (to_timestamp(%s / 1000.0), %s, %s, %s, %s, %s, %s)
                ON CONFLICT (timestamp, pair) DO NOTHING
                """
                cursor.execute(query, (timestamp, pair, open_price, close_price, high_price, low_price, volume))

                count += 1
                if count % batch_size == 0:
                    conn.commit()  # Hacer commit después de cada batch

            conn.commit()  # Último commit al final del procesamiento
            logging.info(f"Datos insertados para {pair} en timeframe {timeframe} en la tabla {table_name}.")

            # Verificar si el último timestamp insertado coincide con los datos más recientes
            ultimo_timestamp = self.verificar_timestamp(conn, pair, timeframe)
            logging.info(f"El último timestamp insertado para {pair} en {timeframe} es {ultimo_timestamp}")

        except Exception as e:
            logging.error(f"Error al insertar datos para {pair}: {e}")
            conn.rollback()  # Hacer rollback en caso de error
        finally:
            cursor.close()

    def eliminar_datos_antiguos(self, timeframe, retention_period):
        """Eliminar datos antiguos según el timeframe y periodo de retención."""
        conn = self.conectar_db()
        if not conn:
            return

        cursor = conn.cursor()

        # Determinar la tabla según el timeframe
        if timeframe == '3m':
            table_name = 'forex_data_3m'
        elif timeframe == '15m':
            table_name = 'forex_data_15m'
        elif timeframe == '4h':
            table_name = 'forex_data_4h'
        else:
            logging.error(f"Timeframe {timeframe} no reconocido para la eliminación de datos.")
            return

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
            # Verificar el tiempo de inserción y posibles duplicados
            cursor = conn.cursor()
            table_name = f'forex_data_{timeframe}'
            query = f"SELECT COUNT(*), MAX(timestamp), MIN(timestamp) FROM {table_name} WHERE pair = %s"
            cursor.execute(query, (pair,))
            count, max_timestamp, min_timestamp = cursor.fetchone()
            cursor.close()

            logging.info(f"Monitoreo de inserción para {pair} en timeframe {timeframe}: Total registros: {count}, Rango de timestamps: {min_timestamp} a {max_timestamp}")

            # Si hay muchos registros duplicados o un retraso significativo en los timestamps, registrarlo
            if count > 100000:  # Ajusta este valor según las expectativas
                logging.warning(f"Posible cuello de botella en la inserción para {pair}. Hay más de 100,000 registros.")
            
            # Monitorear si hay retrasos significativos en la inserción de datos
            if (datetime.now(timezone.utc) - max_timestamp).total_seconds() > 180:  # Más de 3 minutos
                logging.warning(f"Retraso significativo en la inserción de datos para {pair} en {timeframe}. Último timestamp: {max_timestamp}")

        except Exception as e:
            logging.error(f"Error al monitorear la inserción de datos: {e}")

