import os
import json
import logging
import time
import uuid
import psycopg2
from datetime import datetime
from src.SignalManager.SignalValidator import SignalValidator

class SignalManager:
    def __init__(self, config_file, logger=None):
        """Inicializa SignalManager con la configuración y SignalValidator."""
        self.logger = logger or logging.getLogger(__name__)
        self.config = self.cargar_configuracion(config_file)
        self.db_config = self.config.get('db_config', {})
        self.loop_interval = self.config.get('loop_interval', 180)  # 3 minutos
        self.pairs = self.config.get('pairs', [])
        self.validator = SignalValidator(self.db_config, self.logger)
        self.logger.info("SignalManager inicializado correctamente.")

    def cargar_configuracion(self, ruta):
        """Carga la configuración desde un archivo JSON."""
        try:
            with open(ruta, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError) as e:
            self.logger.error(f"Error al cargar {ruta}: {e}")
            raise

    def conectar_base_datos(self):
        """Establece una conexión a la base de datos PostgreSQL."""
        try:
            return psycopg2.connect(
                host=self.db_config['host'],
                database=self.db_config['database'],
                user=self.db_config['user'],
                password=self.db_config['password'],
                options='-c client_encoding=UTF8'
            )
        except psycopg2.Error as e:
            self.logger.error(f"Error al conectar a la base de datos: {e}")
            return None

    def obtener_senal_por_par(self, par):
        """Obtiene la señal más reciente para un par de divisas."""
        query = """
            SELECT id, timestamp, par_de_divisas, tipo_senal, price_signal 
            FROM senales 
            WHERE par_de_divisas = %s 
            ORDER BY timestamp DESC LIMIT 1
        """
        with self.conectar_base_datos() as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute(query, (par,))
                    return cur.fetchone()
                except psycopg2.Error as e:
                    self.logger.error(f"Error al obtener señal para {par}: {e}")
                    return None

    def obtener_sentimiento_desde_db(self, par):
        """Obtiene el sentimiento del mercado desde la base de datos."""
        query = """
            SELECT sentimiento 
            FROM market_sentiments 
            WHERE symbol = %s 
            ORDER BY created_at DESC LIMIT 1;
        """
        with self.conectar_base_datos() as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute(query, (par,))
                    result = cur.fetchone()
                    if result:
                        return result[0]
                    self.logger.info(f"No se encontraron sentimientos para {par}.")
                    return None
                except psycopg2.Error as e:
                    self.logger.error(f"Error al obtener sentimiento para {par}: {e}")
                    return None

    def registrar_senal(self, senal):
        """Registra una nueva señal en la base de datos."""
        id_senal = str(uuid.uuid4())
        timestamp = datetime.utcnow()
        query = """
            INSERT INTO generated_signals 
            (id, par_de_divisas, tipo, accion, timestamp, timeframe, price_signal) 
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        with self.conectar_base_datos() as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute(query, (
                        id_senal, senal[2], 'Tipo1', senal[3], timestamp, '3m', senal[4]
                    ))
                    conn.commit()
                    self.logger.info(f"Señal registrada: {id_senal}, Par: {senal[2]}")
                except psycopg2.Error as e:
                    self.logger.error(f"Error al registrar señal: {e}")
                    conn.rollback()

    def procesar_senal(self, par):
        """Valida y registra una señal si coincide con el sentimiento del mercado."""
        senal = self.obtener_senal_por_par(par)
        if not senal:
            self.logger.info(f"No hay señales nuevas para {par}.")
            return

        accion = self.validator.normalizar(senal[3])
        sentimiento = self.obtener_sentimiento_desde_db(par)

        if sentimiento is None:
            self.logger.warning(f"No se publicará señal para {par}. Sentimiento no disponible.")
            return

        if sentimiento != accion:
            self.logger.warning(f"Señal no publicada para {par}. Sentimiento: {sentimiento}, Acción: {accion}.")
            return

        if self.validator.validar_senal_tipo1(par, accion):
            self.registrar_senal(senal)
        else:
            self.logger.warning(f"Condiciones no válidas para {par}. No se registrará la señal.")

    def procesar_registros(self):
        """Ejecuta el ciclo continuo de procesamiento de señales."""
        while True:
            inicio = time.time()
            try:
                for par in self.pairs:
                    self.procesar_senal(par)
                self.logger.info("Ciclo de procesamiento completo.")
            except Exception as e:
                self.logger.error(f"Error procesando registros: {e}", exc_info=True)

            tiempo_restante = self.loop_interval - (time.time() - inicio)
            if tiempo_restante > 0:
                self.logger.info(f"Esperando {tiempo_restante:.2f} segundos para el próximo ciclo.")
                time.sleep(tiempo_restante)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    config_file = os.path.abspath("src/config/config.json")

    try:
        manager = SignalManager(config_file)
        manager.procesar_registros()
    except Exception as e:
        logging.error(f"Error al iniciar SignalManager: {e}")
