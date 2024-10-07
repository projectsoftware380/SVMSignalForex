import os
import json
import logging
from datetime import datetime, timezone
import unicodedata
import uuid
import psycopg2

class SignalManager:
    def __init__(self, data_dir, config_file, logger=None):
        self.tendencias_file = os.path.join(data_dir, 'tendencias.json')
        self.reversiones_file = os.path.join(data_dir, 'reversiones.json')
        self.signals_file = os.path.join(data_dir, 'signals.json')
        self.candle_patterns_file = os.path.join(data_dir, 'candle_patterns.json')
        self.config_file = config_file

        # Configuración del logger
        if logger is None:
            self.logger = logging.getLogger()
        else:
            self.logger = logger

        # Cargar configuración desde el archivo config.json
        self.config = self.cargar_json(self.config_file)
        self.db_config = self.config['db_config']
        self.api_key_polygon = self.config.get("api_key_polygon")
        self.logger.info("SignalManager inicializado correctamente con la API Key de Polygon.io y la base de datos.")

        # Conectar a la base de datos
        self.conn = self.conectar_base_datos()

    def conectar_base_datos(self):
        """Establece conexión a la base de datos PostgreSQL."""
        try:
            conn = psycopg2.connect(
                host=self.db_config['host'],
                database=self.db_config['database'],
                user=self.db_config['user'],
                password=self.db_config['password']
            )
            self.logger.info("Conexión a la base de datos PostgreSQL exitosa.")
            return conn
        except psycopg2.Error as e:
            self.logger.error(f"Error al conectar a la base de datos: {e}")
            return None

    def cargar_json(self, ruta):
        """Carga el archivo JSON desde la ruta especificada, inicializa vacío si no existe."""
        self.logger.info(f"Cargando archivo desde la ruta: {ruta}")
        if not os.path.exists(ruta):
            self.logger.warning(f"El archivo {ruta} no existe, se inicializará vacío.")
            return {}

        try:
            with open(ruta, 'r', encoding='utf-8') as f:
                contenido = json.load(f)
                if not contenido:
                    self.logger.warning(f"El archivo {ruta} está vacío.")
                    return {}  # Inicializa como diccionario vacío
                self.logger.info(f"Archivo {ruta} cargado correctamente.")
                return contenido
        except json.JSONDecodeError as e:
            self.logger.error(f"Error en el archivo {ruta}: {e}")
            return {}
        except Exception as e:
            self.logger.error(f"Error al cargar el archivo {ruta}: {e}")
            return {}

    def normalizar_string(self, texto):
        """Normaliza una cadena de texto para eliminar caracteres especiales."""
        return unicodedata.normalize('NFKD', texto).encode('ascii', 'ignore').decode('ascii')

    def obtener_timestamp_utc(self):
        """Obtiene el timestamp actual en formato UTC."""
        return datetime.now(timezone.utc).isoformat()

    def generar_senales(self):
        """Genera las señales de compra o venta basadas en las tendencias, reversiones y patrones de velas."""
        self.logger.info("Iniciando la generación de señales...")

        try:
            # Cargar los datos necesarios desde los archivos JSON
            tendencias = self.cargar_json(self.tendencias_file)
            reversiones = self.cargar_json(self.reversiones_file)
            signals = self.cargar_json(self.signals_file)
            candle_patterns = self.cargar_json(self.candle_patterns_file)
        except Exception as e:
            self.logger.error(f"Error cargando los archivos JSON: {e}")
            return []

        nuevas_senales = []
        ahora_utc = self.obtener_timestamp_utc()

        try:
            # Verificar si los datos están vacíos
            if not tendencias or not reversiones or not signals or not candle_patterns:
                self.logger.warning("Los datos de tendencias, reversiones, señales o patrones de velas están vacíos.")
                return []

            # Recorrer los pares de divisas y aplicar la lógica de generación de señales
            for par in tendencias:
                tendencia = self.normalizar_string(tendencias.get(par, ""))
                reversion = self.normalizar_string(reversiones.get(par, ""))
                senal = self.normalizar_string(signals.get(par, ""))
                patron = self.normalizar_string(candle_patterns.get(par, ""))

                self.logger.info(f"Datos para {par}: Tendencia = {tendencia}, Reversión = {reversion}, Señal = {senal}, Patrón = {patron}")

                # Generar un ID único para la señal
                signal_id = str(uuid.uuid4())

                # Señal tipo 1: Tendencia Alcista + Reversión Alcista + Señal de Compra
                if tendencia == 'Tendencia Alcista' and reversion == 'Alcista' and senal == 'Senal de Compra':
                    nuevas_senales.append(self.crear_senal(signal_id, par, 'Señal 1', 'compra', ahora_utc, '3 minutos'))

                # Señal tipo 2: Patrón de Velas Japonesas en 4 horas + Tendencia Alcista
                if tendencia == 'Tendencia Alcista' and candle_patterns.get('timeframe') == '4h' and patron == 'alcista':
                    nuevas_senales.append(self.crear_senal(signal_id, par, 'Señal 2', 'compra', ahora_utc, '4 horas'))

                # Señal tipo 3: Tendencia Alcista + Reversión Alcista + Vela Alcista en 15 minutos
                if tendencia == 'Tendencia Alcista' and reversion == 'Alcista' and candle_patterns.get('timeframe') == '15m' and patron == 'alcista':
                    nuevas_senales.append(self.crear_senal(signal_id, par, 'Señal 3', 'compra', ahora_utc, '15 minutos'))

                # Señal tipo 4: Tendencia Alcista + Patrón de Vela Alcista en 15 minutos
                if tendencia == 'Tendencia Alcista' and candle_patterns.get('timeframe') == '15m' and patron == 'alcista':
                    nuevas_senales.append(self.crear_senal(signal_id, par, 'Señal 4', 'compra', ahora_utc, '15 minutos'))

                # Señal tipo 5: Tendencia Alcista + Señal de Compra
                if tendencia == 'Tendencia Alcista' and senal == 'Senal de Compra':
                    nuevas_senales.append(self.crear_senal(signal_id, par, 'Señal 5', 'compra', ahora_utc, '3 minutos'))

                # Señales de Venta (similar a las señales de compra)
                if tendencia == 'Tendencia Bajista' and reversion == 'Bajista' and senal == 'Senal de Venta':
                    nuevas_senales.append(self.crear_senal(signal_id, par, 'Señal 1', 'venta', ahora_utc, '3 minutos'))

                if tendencia == 'Tendencia Bajista' and candle_patterns.get('timeframe') == '4h' and patron == 'bajista':
                    nuevas_senales.append(self.crear_senal(signal_id, par, 'Señal 2', 'venta', ahora_utc, '4 horas'))

                if tendencia == 'Tendencia Bajista' and reversion == 'Bajista' and candle_patterns.get('timeframe') == '15m' and patron == 'bajista':
                    nuevas_senales.append(self.crear_senal(signal_id, par, 'Señal 3', 'venta', ahora_utc, '15 minutos'))

                if tendencia == 'Tendencia Bajista' and candle_patterns.get('timeframe') == '15m' and patron == 'bajista':
                    nuevas_senales.append(self.crear_senal(signal_id, par, 'Señal 4', 'venta', ahora_utc, '15 minutos'))

                if tendencia == 'Tendencia Bajista' and senal == 'Senal de Venta':
                    nuevas_senales.append(self.crear_senal(signal_id, par, 'Señal 5', 'venta', ahora_utc, '3 minutos'))

        except KeyError as e:
            self.logger.error(f"Error al acceder a una clave inexistente: {e}")
        except Exception as e:
            self.logger.error(f"Error durante la generación de señales: {e}")

        self.logger.info(f"Se generaron {len(nuevas_senales)} señales.")

        # Guardar señales en la base de datos
        self.guardar_senales_db(nuevas_senales)

        # Asegurarnos de replicar las señales en SignalTracker
        self.replicar_senales_a_tracker(nuevas_senales)

        return nuevas_senales

    def crear_senal(self, signal_id, par, tipo, accion, timestamp, timeframe):
        """Crea una nueva señal con los detalles especificados."""
        return {
            'id': signal_id,
            'par': par,
            'tipo': tipo,
            'accion': accion,
            'timestamp': timestamp,
            'timeframe_operacion': timeframe
        }

    def guardar_senales_db(self, senales):
        """Guarda las señales generadas en la base de datos."""
        try:
            cur = self.conn.cursor()
            for senal in senales:
                cur.execute("""
                    INSERT INTO generated_signals (id, par, tipo, accion, timestamp, timeframe_operacion)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (senal['id'], senal['par'], senal['tipo'], senal['accion'], senal['timestamp'], senal['timeframe_operacion']))
            self.conn.commit()
            self.logger.info(f"Señales guardadas correctamente en la base de datos.")
        except psycopg2.Error as e:
            self.logger.error(f"Error al guardar señales en la base de datos: {e}")
            self.conn.rollback()

    def replicar_senales_a_tracker(self, senales):
        """Método que garantiza que las señales generadas se pasen a SignalTracker."""
        from src.SignalManager.SignalTracker import SignalTracker
        signal_tracker = SignalTracker(self.conn, logger=self.logger)
        signal_tracker.replicar_logica_senal_activa()

    def ejecutar_proceso_generacion(self):
        """Ejecuta el proceso completo de generación y guardado de señales."""
        self.logger.info("Iniciando el proceso completo de generación y guardado de señales.")
        senales = self.generar_senales()
        self.logger.info("Proceso completo de generación y guardado de señales finalizado.")
