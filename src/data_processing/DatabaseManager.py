import psycopg2
import logging
import os

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
    def __init__(self, db_config):
        self.db_config = db_config

    def conectar_db(self):
        """Establecer conexión con la base de datos PostgreSQL."""
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
            logging.error(f"Error al conectar a la base de datos: {error}")
            return None

    def insertar_datos(self, conn, datos, pair, timeframe):
        """Inserta los datos obtenidos en la tabla PostgreSQL en bloques pequeños, dirigiendo a la tabla adecuada."""
        cursor = conn.cursor()
        batch_size = 100
        count = 0
        
        # Determinar la tabla según el timeframe
        if timeframe == '3m':
            table_name = 'forex_data_3m'
        elif timeframe == '15m':
            table_name = 'forex_data_15m'
        elif timeframe == '4h':
            table_name = 'forex_data_4h'
        else:
            logging.error(f"Timeframe {timeframe} no reconocido para la inserción de datos.")
            return

        for result in datos.get("results", []):
            timestamp = result['t']
            open_price = result['o']
            close_price = result['c']
            high_price = result['h']
            low_price = result['l']
            volume = result['v']

            query = f"""
            INSERT INTO {table_name} (timestamp, pair, timeframe, open, close, high, low, volume)
            VALUES (to_timestamp(%s / 1000.0), %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
            """
            cursor.execute(query, (timestamp, pair, timeframe, open_price, close_price, high_price, low_price, volume))

            count += 1
            if count % batch_size == 0:
                conn.commit()

        conn.commit()
        cursor.close()
        logging.info(f"Datos insertados para {pair} en timeframe {timeframe} en la tabla {table_name}.")

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

        query = f"""
        DELETE FROM {table_name}
        WHERE timestamp < NOW() - INTERVAL %s
        """
        cursor.execute(query, (retention_period,))
        conn.commit()
        cursor.close()
        conn.close()
        logging.info(f"Datos antiguos eliminados para la tabla {table_name} con retención de {retention_period}.")
