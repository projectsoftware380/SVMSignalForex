import logging
from datetime import datetime, timezone
import psycopg2

class SignalTracker:
    def __init__(self, conn, logger=None):
        # Se pasa la conexión de la base de datos como parámetro
        self.conn = conn

        # Configuración del logger
        if logger is None:
            self.logger = logging.getLogger()
        else:
            self.logger = logger

    def obtener_signales_generadas_db(self):
        """Carga las señales generadas desde la base de datos."""
        try:
            cur = self.conn.cursor()
            cur.execute("SELECT id, par, tipo, accion, timestamp, timeframe_operacion FROM generated_signals")
            rows = cur.fetchall()
            generated_signals = []
            for row in rows:
                signal = {
                    'id': row[0],
                    'par': row[1],
                    'tipo': row[2],
                    'accion': row[3],
                    'timestamp': row[4].isoformat(),  # Convertir timestamp a string ISO 8601
                    'timeframe_operacion': row[5]
                }
                generated_signals.append(signal)
            self.logger.info("Señales generadas cargadas correctamente desde la base de datos.")
            return generated_signals
        except psycopg2.Error as e:
            self.logger.error(f"Error al cargar señales generadas desde la base de datos: {e}")
            return []

    def actualizar_estado_senal_db(self, signal_id, estado, tiempo_activo, timestamp_actual):
        """Actualiza el estado de la señal en la base de datos."""
        try:
            cur = self.conn.cursor()
            cur.execute("""
                UPDATE tracked_signals
                SET estado = %s, tiempo_senal_activa = %s, timestamp_actual = %s
                WHERE id = %s
            """, (estado, tiempo_activo, timestamp_actual, signal_id))
            self.conn.commit()
            self.logger.info(f"Señal {signal_id} actualizada correctamente en la base de datos.")
        except psycopg2.Error as e:
            self.logger.error(f"Error al actualizar el estado de la señal {signal_id} en la base de datos: {e}")
            self.conn.rollback()

    def insertar_nueva_senal_db(self, senal):
        """Inserta una nueva señal en la tabla de señales rastreadas."""
        try:
            cur = self.conn.cursor()
            cur.execute("""
                INSERT INTO tracked_signals (id, par, tipo, accion, timestamp, timeframe_operacion, estado, timestamp_actual, tiempo_senal_activa)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (senal['id'], senal['par'], senal['tipo'], senal['accion'], senal['timestamp'], senal['timeframe_operacion'],
                  senal['estado'], senal['timestamp_actual'], senal['tiempo_senal_activa']))
            self.conn.commit()
            self.logger.info(f"Señal {senal['id']} registrada correctamente en la base de datos.")
        except psycopg2.Error as e:
            self.logger.error(f"Error al insertar la señal {senal['id']} en la base de datos: {e}")
            self.conn.rollback()

    def obtener_timestamp_utc(self):
        """Obtiene el timestamp actual en formato UTC."""
        return datetime.now(timezone.utc).isoformat()

    def verificar_condiciones_inactivacion(self, senal, estado_mercado):
        """Verificar si la señal debe pasar a inactiva según las condiciones."""
        tipo = senal.get('tipo')
        tendencia = estado_mercado.get('tendencia', '')
        reversion = estado_mercado.get('reversion', '')
        patron_velas = estado_mercado.get('patron_velas', '')

        # Verificar las condiciones de inactivación según el tipo de señal
        if tipo == 'Señal 1':
            if tendencia != 'Tendencia Alcista' or reversion == 'Bajista' or 'Señal de Venta' in patron_velas:
                return 'inactiva'
        elif tipo == 'Señal 2':
            if tendencia != 'Tendencia Alcista' or patron_velas == 'Patrón Bajista 4h':
                return 'inactiva'
        elif tipo == 'Señal 3':
            if tendencia != 'Tendencia Alcista' or reversion == 'Bajista' or patron_velas == 'Patrón Bajista 15m':
                return 'inactiva'
        elif tipo == 'Señal 4':
            if tendencia != 'Tendencia Alcista' or patron_velas == 'Patrón Bajista 15m':
                return 'inactiva'
        elif tipo == 'Señal 5':
            if tendencia != 'Tendencia Alcista' or 'Señal de Venta' in patron_velas:
                return 'inactiva'

        return 'activa'  # Si no cumple las condiciones, sigue activa.

    def replicar_logica_senal_activa(self):
        """Replicar la lógica de activación e inactivación de SignalManager."""
        try:
            # Cargar las señales generadas desde la base de datos
            self.generated_signals = self.obtener_signales_generadas_db()
            if not self.generated_signals:
                self.logger.warning("No hay señales generadas para procesar.")
                return

            estado_mercado = self.obtener_estado_mercado()

            for senal in self.generated_signals:
                signal_id = senal.get('id')
                ahora_utc = self.obtener_timestamp_utc()

                # Verificar si la señal ya existe en la base de datos (tracked_signals)
                cur = self.conn.cursor()
                cur.execute("SELECT id FROM tracked_signals WHERE id = %s", (signal_id,))
                resultado = cur.fetchone()

                if resultado is None:
                    # Registrar la señal como activa si no existe en tracked_signals
                    senal['estado'] = 'activa'
                    senal['timestamp_actual'] = ahora_utc
                    senal['tiempo_senal_activa'] = 0
                    self.insertar_nueva_senal_db(senal)
                else:
                    # Actualizar el estado de tiempo activo de la señal
                    tiempo_activo = (datetime.now(timezone.utc) - datetime.fromisoformat(senal['timestamp'])).total_seconds() / 60
                    estado_senal = self.verificar_condiciones_inactivacion(senal, estado_mercado)
                    self.actualizar_estado_senal_db(signal_id, estado_senal, tiempo_activo, ahora_utc)

        except Exception as e:
            self.logger.error(f"Error durante la replicación de la lógica de señal activa: {e}")

    def obtener_estado_mercado(self):
        """Simular la obtención del estado del mercado (para condiciones de inactivación)."""
        # Esto es solo un ejemplo. En la realidad, deberías obtener los datos del mercado en tiempo real.
        return {
            'tendencia': 'Tendencia Alcista',  # Podría ser Alcista, Bajista o Neutral
            'reversion': 'Bajista',  # Podría ser Alcista o Bajista
            'patron_velas': 'Patrón Bajista 15m'  # Según timeframe: 15m, 4h, etc.
        }

    def ejecutar_proceso(self):
        """Proceso que se ejecutará de manera continua cada 3 minutos."""
        self.logger.info("Iniciando proceso de replicación de señales activas e inactivación.")
        self.replicar_logica_senal_activa()
        self.logger.info("Proceso completado. Actualizando en 3 minutos.")
