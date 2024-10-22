import os
import json
import sys
import logging
import time
import uuid
import psycopg2
from datetime import datetime, timezone
from src.SignalManager.SignalValidator import SignalValidator

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
sys.path.append(ROOT_DIR)

class SignalTracker:
    def __init__(self, config_file):
        """Inicializa SignalTracker con la configuración y SignalValidator."""
        self.logger = self.configurar_logger()
        self.logger.info("Inicializando SignalTracker...")
        self.config = self.cargar_configuracion(config_file)
        self.db_config = self.config.get('db_config', {})
        self.validator = SignalValidator(self.db_config, self.logger)
        self.ultimo_timestamp_procesado = {}  # Diccionario para guardar el último timestamp procesado por par

    def configurar_logger(self):
        """Configura un logger específico para SignalTracker."""
        log_dir = os.path.join(os.path.dirname(__file__), '..', 'logs')
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, 'signal_tracker.log')

        logger = logging.getLogger('SignalTracker')
        logger.setLevel(logging.DEBUG)

        if not logger.handlers:
            handler = logging.FileHandler(log_file)
            handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
            logger.addHandler(handler)

        return logger

    def cargar_configuracion(self, ruta):
        """Carga la configuración desde config.json."""
        try:
            with open(ruta, 'r', encoding='utf-8') as f:
                config = json.load(f)
                self.logger.info("Configuración cargada exitosamente.")
                return config
        except (FileNotFoundError, json.JSONDecodeError) as e:
            self.logger.error(f"Error al cargar {ruta}: {e}")
            raise

    def conectar_base_datos(self):
        """Establece una conexión a la base de datos PostgreSQL."""
        try:
            conn = psycopg2.connect(
                host=self.db_config['host'],
                database=self.db_config['database'],
                user=self.db_config['user'],
                password=self.db_config['password'],
                options='-c client_encoding=UTF8'
            )
            self.logger.info("Conexión a la base de datos PostgreSQL exitosa.")
            return conn
        except psycopg2.Error as e:
            self.logger.error(f"Error al conectar a la base de datos: {e}")
            return None

    def obtener_senal_activa(self, par_de_divisas):
        """Obtiene la señal activa actual para el par de divisas dado."""
        conn = self.conectar_base_datos()
        if not conn:
            return None
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, par_de_divisas, tipo, accion, timestamp, timeframe, price_signal
                    FROM tracked_signals
                    WHERE par_de_divisas = %s AND estado = 'activo'
                    ORDER BY timestamp DESC LIMIT 1
                """, (par_de_divisas,))
                return cur.fetchone()
        except psycopg2.Error as e:
            self.logger.error(f"Error al obtener señal activa para {par_de_divisas}: {e}")
            return None
        finally:
            conn.close()

    def inactivar_senal(self, senal_id):
        """Inactiva una señal en la tabla `tracked_signals`."""
        conn = self.conectar_base_datos()
        if not conn:
            return
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE tracked_signals
                    SET estado = 'inactivo', timestamp_actual = %s
                    WHERE id = %s
                """, (datetime.now(timezone.utc), senal_id))
                conn.commit()
                self.logger.info(f"Señal {senal_id} inactivada exitosamente.")
        except psycopg2.Error as e:
            self.logger.error(f"Error al inactivar señal {senal_id}: {e}")
            conn.rollback()
        finally:
            conn.close()

    def validar_y_copiar_senal(self, senal):
        """Valida y copia la señal si es válida y reemplaza la activa."""
        self.logger.debug(f"Validando señal: {senal}")

        par_de_divisas = senal[1]
        accion = self.validator.normalizar(senal[3])  # Normalizar la acción
        senal_activa = self.obtener_senal_activa(par_de_divisas)

        if senal_activa:
            self.logger.info(f"Inactivando señal existente para {par_de_divisas}.")
            self.inactivar_senal(senal_activa[0])

        es_valido, tendencia, reversion, tipo_patron, timeframe = \
            self.validator.validar_condiciones(par_de_divisas)

        if es_valido:
            tipo = self.validator.determinar_tipo_senal(tendencia, reversion, accion, tipo_patron, timeframe)
            if tipo != 'Desconocida':
                self.insertar_nueva_senal(senal, tipo)
            else:
                self.logger.warning(f"No se determinó un tipo válido para la señal: {senal}")
        else:
            self.logger.warning(f"Condiciones no válidas para {par_de_divisas}. No se registrará la señal.")

    def insertar_nueva_senal(self, senal, tipo):
        """Inserta una nueva señal activa en `tracked_signals`."""
        self.logger.info(f"Insertando señal {senal[0]} en tracked_signals...")
        conn = self.conectar_base_datos()
        if not conn:
            return

        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO tracked_signals 
                    (id, par_de_divisas, tipo, accion, timestamp, 
                     timeframe, price_signal, estado, timestamp_actual) 
                    VALUES (%s, %s, %s, %s, %s::timestamp, %s, %s, 'activo', %s)
                """, (
                    str(uuid.uuid4()), senal[1], tipo, senal[3], senal[4],
                    senal[5], float(senal[6]), datetime.now(timezone.utc)
                ))
                conn.commit()
                self.logger.info(f"Señal {senal[0]} registrada exitosamente.")
        except psycopg2.Error as e:
            self.logger.error(f"Error al insertar señal {senal[0]}: {e}")
            conn.rollback()
        finally:
            conn.close()

    def ejecutar_monitoreo(self):
        """Ejecuta el monitoreo continuo de señales."""
        while True:
            self.logger.info("Iniciando monitoreo de señales.")
            senales = self.obtener_senales_nuevas()
            if not senales:
                self.logger.info("No se encontraron señales nuevas.")
            for senal in senales:
                self.validar_y_copiar_senal(senal)
            time.sleep(60)

    def obtener_senales_nuevas(self):
        """Obtiene las señales nuevas desde `generated_signals`."""
        conn = self.conectar_base_datos()
        if not conn:
            return []

        try:
            with conn.cursor() as cur:
                # Obtener el timestamp del último procesamiento por par de divisas
                ultimos_timestamps = self.obtener_ultimos_timestamps_por_par(conn)

                # Construir la consulta para obtener señales nuevas
                query = """
                    SELECT id, par_de_divisas, tipo, accion, timestamp, 
                           timeframe, price_signal 
                    FROM generated_signals
                    WHERE par_de_divisas = %s AND timestamp > %s
                    ORDER BY timestamp ASC
                """

                senales_nuevas = []
                for par_de_divisas in self.config.get('pairs', []):
                    ultimo_timestamp = ultimos_timestamps.get(par_de_divisas, datetime.min)
                    cur.execute(query, (par_de_divisas, ultimo_timestamp))
                    senales = cur.fetchall()
                    if senales:
                        senales_nuevas.extend(senales)
                        # Actualizar el último timestamp procesado para el par
                        self.ultimo_timestamp_procesado[par_de_divisas] = senales[-1][4]
                return senales_nuevas
        except psycopg2.Error as e:
            self.logger.error(f"Error al obtener señales nuevas: {e}")
            return []
        finally:
            conn.close()

    def obtener_ultimos_timestamps_por_par(self, conn):
        """Obtiene el último timestamp procesado por par de divisas."""
        ultimos_timestamps = {}
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT par_de_divisas, MAX(timestamp) as ultimo_timestamp
                    FROM tracked_signals
                    GROUP BY par_de_divisas
                """)
                rows = cur.fetchall()
                for row in rows:
                    ultimos_timestamps[row[0]] = row[1]
            return ultimos_timestamps
        except psycopg2.Error as e:
            self.logger.error(f"Error al obtener últimos timestamps por par: {e}")
            return ultimos_timestamps

if __name__ == "__main__":
    config_file = os.path.abspath("src/config/config.json")
    try:
        tracker = SignalTracker(config_file)
        tracker.ejecutar_monitoreo()
    except FileNotFoundError as e:
        logging.error(e)
        exit(1)
