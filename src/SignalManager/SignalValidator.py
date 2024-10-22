import psycopg2  # Conexión con PostgreSQL
import logging   # Registro de logs y mensajes

class SignalValidator:
    def __init__(self, db_config, logger=None):
        """Inicializa el validador de señales con la configuración de la base de datos."""
        self.db_config = db_config
        self.logger = logger or logging.getLogger(__name__)

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
            self.logger.debug("Conexión a la base de datos PostgreSQL exitosa.")
            return conn
        except psycopg2.Error as e:
            self.logger.error(f"Error al conectar a la base de datos: {e}")
            return None

    def normalizar(self, valor):
        """Normaliza los valores para obtener 'alcista', 'bajista', 'neutral' o None."""
        if not valor:
            return None
        valor = str(valor).strip().lower()
        if valor in ['alcista', 'compra', 'buy', 'señal de compra']:
            return 'alcista'
        elif valor in ['bajista', 'venta', 'sell', 'señal de venta']:
            return 'bajista'
        elif valor == 'neutral':
            return 'neutral'
        else:
            return None

    def validar_senal_tipo1(self, par_de_divisas, accion):
        """
        Valida las condiciones específicas para señales tipo 1.
        Devuelve True si la señal es válida, False en caso contrario.
        """
        try:
            # Aquí puedes implementar la lógica específica para validar una señal tipo 1.
            # Por ejemplo, podrías verificar que la señal coincide con la tendencia actual.

            # Para este ejemplo, obtendremos la tendencia actual y compararemos con la acción.
            tendencia = self.obtener_tendencia_actual(par_de_divisas)

            if tendencia is None:
                self.logger.warning(f"No se pudo obtener la tendencia para {par_de_divisas}.")
                return False

            accion_normalizada = self.normalizar(accion)

            if accion_normalizada == tendencia:
                self.logger.info(f"La acción {accion_normalizada} coincide con la tendencia {tendencia}. Señal válida.")
                return True
            else:
                self.logger.info(f"La acción {accion_normalizada} no coincide con la tendencia {tendencia}. Señal no válida.")
                return False

        except Exception as e:
            self.logger.error(f"Error al validar señal tipo 1 para {par_de_divisas}: {e}")
            return False

    def obtener_tendencia_actual(self, par_de_divisas):
        """Obtiene la tendencia actual del par de divisas desde la base de datos."""
        conn = self.conectar_base_datos()
        if not conn:
            return None

        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT tipo_tendencia
                    FROM tendencias
                    WHERE par_de_divisas = %s
                    ORDER BY timestamp DESC LIMIT 1
                """, (par_de_divisas,))
                resultado = cur.fetchone()
                tendencia = self.normalizar(resultado[0]) if resultado else None
                return tendencia
        except psycopg2.Error as e:
            self.logger.error(f"Error al obtener tendencia actual para {par_de_divisas}: {e}")
            return None
        finally:
            conn.close()

    # Métodos adicionales que ya tenías en tu clase

    def validar_condiciones(self, par_de_divisas):
        """Valida las condiciones de tendencia, reversión, patrón y timeframe para el par."""
        conn = self.conectar_base_datos()
        if not conn:
            return False, None, None, None, None

        try:
            with conn.cursor() as cur:
                # Obtener la tendencia
                cur.execute("""
                    SELECT tipo_tendencia 
                    FROM tendencias 
                    WHERE par_de_divisas = %s 
                    ORDER BY timestamp DESC LIMIT 1
                """, (par_de_divisas,))
                resultado = cur.fetchone()
                tendencia = self.normalizar(resultado[0]) if resultado else None

                # Obtener la reversión
                cur.execute("""
                    SELECT tipo_reversion 
                    FROM reversiones 
                    WHERE par_de_divisas = %s 
                    ORDER BY timestamp DESC LIMIT 1
                """, (par_de_divisas,))
                resultado = cur.fetchone()
                reversion = self.normalizar(resultado[0]) if resultado else None

                # Obtener el patrón y su timeframe
                cur.execute("""
                    SELECT tipo, timeframe 
                    FROM patrones_velas 
                    WHERE par_de_divisas = %s 
                    ORDER BY timestamp DESC LIMIT 1
                """, (par_de_divisas,))
                resultado = cur.fetchone()
                tipo_patron = self.normalizar(resultado[0]) if resultado else None
                timeframe = resultado[1] if resultado else None

                if tendencia and reversion and tipo_patron and timeframe:
                    self.logger.info(
                        f"Tendencia: {tendencia}, Reversión: {reversion}, "
                        f"Patrón: {tipo_patron}, Timeframe: {timeframe}"
                    )
                    return True, tendencia, reversion, tipo_patron, timeframe
                else:
                    self.logger.warning(f"No se encontraron datos completos para {par_de_divisas}.")
                    return False, None, None, None, None
        except psycopg2.Error as e:
            self.logger.error(f"Error al validar condiciones para {par_de_divisas}: {e}")
            return False, None, None, None, None
        finally:
            conn.close()

    def determinar_tipo_senal(self, tendencia, reversion, accion, tipo_patron, timeframe):
        """Determina el tipo de señal basado en las condiciones definidas."""
        accion_normalizada = self.normalizar(accion)

        if accion_normalizada is None:
            self.logger.warning(f"Acción desconocida: {accion}")
            return 'Desconocida'

        # Definir las reglas de negocio para determinar el tipo de señal
        # Ejemplo de condiciones adicionales para cubrir más casos
        if tendencia == 'alcista' and reversion == 'alcista':
            if accion_normalizada == 'alcista':
                return 'Señal 1'
            elif accion_normalizada == 'bajista':
                return 'Señal 4'

        if tendencia == 'bajista' and reversion == 'bajista':
            if accion_normalizada == 'bajista':
                return 'Señal 1'
            elif accion_normalizada == 'alcista':
                return 'Señal 4'

        if tendencia == 'alcista' and reversion == 'bajista':
            if tipo_patron == 'bajista' and accion_normalizada == 'bajista':
                return 'Señal de Venta'
            elif tipo_patron == 'alcista' and accion_normalizada == 'alcista':
                return 'Señal de Compra'

        if tendencia == 'bajista' and reversion == 'alcista':
            if tipo_patron == 'alcista' and accion_normalizada == 'alcista':
                return 'Señal de Compra'
            elif tipo_patron == 'bajista' and accion_normalizada == 'bajista':
                return 'Señal de Venta'

        # Condiciones basadas en el timeframe
        if timeframe == '4h':
            if tipo_patron == 'alcista' and accion_normalizada == 'alcista':
                return 'Señal 2'
            elif tipo_patron == 'bajista' and accion_normalizada == 'bajista':
                return 'Señal 2'

        if timeframe == '3m':
            if tipo_patron == 'alcista' and accion_normalizada == 'alcista':
                return 'Señal 3'
            elif tipo_patron == 'bajista' and accion_normalizada == 'bajista':
                return 'Señal 3'

        # Si ninguna condición se cumple, retornar 'Desconocida'
        self.logger.warning(
            f"No se pudo determinar el tipo de señal para las condiciones: "
            f"Tendencia={tendencia}, Reversión={reversion}, Acción={accion_normalizada}, "
            f"Patrón={tipo_patron}, Timeframe={timeframe}"
        )
        return 'Desconocida'

    def obtener_estado_actual(self, par_de_divisas):
        """Obtiene el estado actual del par desde las tablas de tendencias, reversiones y patrones."""
        conn = self.conectar_base_datos()
        if not conn:
            return None, None, None, None

        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT t.tipo_tendencia, r.tipo_reversion, p.tipo, p.timeframe 
                    FROM tendencias t
                    JOIN reversiones r ON t.par_de_divisas = r.par_de_divisas
                    JOIN patrones_velas p ON t.par_de_divisas = p.par_de_divisas
                    WHERE t.par_de_divisas = %s 
                    ORDER BY t.timestamp DESC LIMIT 1
                """, (par_de_divisas,))
                resultado = cur.fetchone()
                if resultado:
                    tendencia = self.normalizar(resultado[0])
                    reversion = self.normalizar(resultado[1])
                    tipo_patron = self.normalizar(resultado[2])
                    timeframe = resultado[3]
                    return tendencia, reversion, tipo_patron, timeframe
                else:
                    self.logger.warning(f"No se encontró estado actual para {par_de_divisas}.")
                    return None, None, None, None
        except psycopg2.Error as e:
            self.logger.error(f"Error al obtener estado actual para {par_de_divisas}: {e}")
            return None, None, None, None
        finally:
            conn.close()
