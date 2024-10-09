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
        self.logger = logger or logging.getLogger()

        # Cargar configuración desde el archivo config.json
        self.config = self.cargar_json(self.config_file)
        self.db_config = self.config.get('db_config', {})
        self.api_key_polygon = self.config.get("api_key_polygon")
        self.logger.info("SignalManager inicializado correctamente con la API Key de Polygon.io y la base de datos.")

    def conectar_base_datos(self):
        """Establece conexión a la base de datos PostgreSQL."""
        try:
            conn = psycopg2.connect(
                host=self.db_config.get('host'),
                database=self.db_config.get('database'),
                user=self.db_config.get('user'),
                password=self.db_config.get('password')
            )
            self.logger.info("Conexión a la base de datos PostgreSQL exitosa.")
            return conn
        except psycopg2.Error as e:
            self.logger.error(f"Error al conectar a la base de datos: {e}")
            return None

    def cargar_json(self, ruta):
        """Carga el archivo JSON desde la ruta especificada, inicializa vacío si no existe o si hay errores."""
        self.logger.info(f"Cargando archivo desde la ruta: {ruta}")
        if not os.path.exists(ruta):
            self.logger.warning(f"El archivo {ruta} no existe, inicializando como un diccionario vacío.")
            return {}

        try:
            with open(ruta, 'r', encoding='utf-8') as f:
                contenido = json.load(f)
                if not contenido:
                    self.logger.warning(f"El archivo {ruta} está vacío.")
                    return {}
                self.logger.info(f"Archivo {ruta} cargado correctamente.")
                return contenido
        except json.JSONDecodeError as e:
            self.logger.error(f"Error de decodificación JSON en el archivo {ruta}: {e}")
            return {}
        except Exception as e:
            self.logger.error(f"Error inesperado al cargar el archivo {ruta}: {e}")
            return {}

    def normalizar_string(self, valor):
        """Normaliza una cadena de texto para eliminar caracteres especiales o extraer valores clave de un diccionario."""
        if isinstance(valor, dict):
            return valor.get('tendencia', '')  # Extrae la clave 'tendencia' si es un diccionario
        elif isinstance(valor, str):
            return unicodedata.normalize('NFKD', valor).encode('ascii', 'ignore').decode('ascii')
        else:
            return str(valor)

    def obtener_timestamp_utc(self):
        """Obtiene el timestamp actual en formato UTC."""
        return datetime.now(timezone.utc).isoformat()

    def generar_senales(self):
        """Genera las señales de compra o venta basadas en las tendencias y patrones de velas."""
        self.logger.info("Iniciando la generación de señales...")

        # Cargar los datos necesarios desde los archivos JSON
        tendencias = self.cargar_json(self.tendencias_file)
        candle_patterns = self.cargar_json(self.candle_patterns_file)

        nuevas_senales = []
        ahora_utc = self.obtener_timestamp_utc()

        # Verificar si los datos están vacíos
        if not tendencias or not candle_patterns:
            self.logger.warning("Los datos de tendencias o patrones de velas están vacíos.")
            return []

        # Recorrer los pares de divisas y aplicar la lógica de generación de señales
        for par in tendencias:
            tendencia = self.normalizar_string(tendencias.get(par, ""))
            patrones = candle_patterns.get(par, {})

            # Agregar logs detallados para facilitar la depuración
            self.logger.info(f"Datos para {par}: Tendencia = {tendencia}, Patrones = {patrones}")

            # Recorrer los patrones de velas si existen para ese par
            for patron, patron_data in patrones.items():
                tipo = patron_data.get("tipo", "")
                timeframe = patron_data.get("timeframe", "")

                # Agregar un log para ver qué patrón se está procesando
                self.logger.info(f"Procesando patrón {patron}: Tipo = {tipo}, Timeframe = {timeframe} para {par}")

                # Generar un ID único para la señal
                signal_id = str(uuid.uuid4())

                # Señales tipo 2: Tendencia y patrón deben coincidir en 4 horas
                if timeframe == '4h' and self.es_tendencia_y_patron_compatibles(tendencia, tipo):
                    nuevas_senales.append(self.crear_senal(signal_id, par, 'Señal 2', 'compra' if tipo == 'Alcista' else 'venta', ahora_utc, '4 horas'))

                # Señales tipo 3: Tendencia y patrón deben coincidir en timeframes de 15m o 3m
                elif timeframe in ['15m', '3m'] and self.es_tendencia_y_patron_compatibles(tendencia, tipo):
                    nuevas_senales.append(self.crear_senal(signal_id, par, 'Señal 3', 'compra' if tipo == 'Alcista' else 'venta', ahora_utc, timeframe))

        self.logger.info(f"Se generaron {len(nuevas_senales)} señales.")
        self.guardar_senales_db(nuevas_senales)
        return nuevas_senales

    def es_tendencia_y_patron_compatibles(self, tendencia, tipo_patron):
        """Verifica si la tendencia y el patrón de velas son compatibles."""
        if tendencia == 'Tendencia Alcista' and tipo_patron == 'Alcista':
            return True
        elif tendencia == 'Tendencia Bajista' and tipo_patron == 'Bajista':
            return True
        return False

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
        conn = self.conectar_base_datos()
        if conn is None:
            self.logger.error("No se pudo establecer conexión con la base de datos.")
            return

        try:
            cur = conn.cursor()
            for senal in senales:
                cur.execute("""
                    INSERT INTO generated_signals (id, par, tipo, accion, timestamp, timeframe_operacion)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (senal['id'], senal['par'], senal['tipo'], senal['accion'], senal['timestamp'], senal['timeframe_operacion']))
            conn.commit()
            self.logger.info(f"Señales guardadas correctamente en la base de datos.")
        except psycopg2.Error as e:
            self.logger.error(f"Error al guardar señales en la base de datos: {e}")
            conn.rollback()
        finally:
            conn.close()
