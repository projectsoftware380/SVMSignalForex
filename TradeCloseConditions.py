import logging
import MetaTrader5 as mt5
import traceback

# Configuración básica de logging
logging.basicConfig(
    filename='logs/trading_system.log',
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class TradeCloseConditions:
    def __init__(self, mt5_executor):
        self.mt5_executor = mt5_executor

    def normalizar_par(self, pair):
        """Normaliza el nombre del par de divisas."""
        return pair.replace("-", "").replace(".", "").upper()

    def verificar_cierre_por_condiciones(self, symbol, tendencia_actual):
        """
        Verifica si alguna de las condiciones de cierre se cumple:
        - Tendencia contraria o neutral.
        """
        logging.info(f"Verificando condiciones de cierre para {symbol}. Tendencia actual: {tendencia_actual if tendencia_actual else 'No determinada'}.")

        # Comprobamos si hay una operación abierta para este símbolo
        operacion_abierta = self.obtener_operacion_abierta(symbol)
        if operacion_abierta:
            logging.info(f"Operación abierta encontrada para {symbol}: {operacion_abierta}")

            if tendencia_actual is None:
                logging.info(f"Tendencia para {symbol} aún no determinada. No se tomará acción.")
                return False

            # Verificamos si la tendencia actual es contraria a la operación abierta
            if self.verificar_tendencia_contraria(tendencia_actual, operacion_abierta['type']):
                logging.info(f"Las condiciones de cierre por tendencia contraria se cumplen para {symbol}. Cerrando operación...")

                # Intentar cerrar la operación
                try:
                    resultado_cierre = self.mt5_executor.cerrar_operacion(operacion_abierta['ticket'])
                    if resultado_cierre:
                        logging.info(f"Operación {operacion_abierta['ticket']} cerrada correctamente.")
                        return True
                    else:
                        logging.error(f"Error al intentar cerrar la operación {operacion_abierta['ticket']}.")
                        return False
                except Exception as e:
                    logging.error(f"Error inesperado al cerrar la operación {operacion_abierta['ticket']}: {str(e)}")
                    logging.error(traceback.format_exc())
                    return False
                finally:
                    # Cerrar conexión para evitar fugas de recursos en hilos
                    self.cerrar_conexion_mt5()

            else:
                logging.info(f"No se cumplen condiciones de cierre por tendencia contraria para {symbol}.")
        else:
            logging.info(f"No hay operación abierta para {symbol}.")
        return False

    def verificar_tendencia_contraria(self, tendencia_actual, tipo_operacion):
        """
        Verifica si la tendencia actual es contraria a la posición abierta o es neutral.
        """
        logging.info(f"Verificando tendencia para una operación de tipo: {tipo_operacion}")

        # Verificamos si la operación es de compra y la tendencia es bajista o neutral
        if tipo_operacion == 'compra' and (tendencia_actual == "Tendencia Bajista" or tendencia_actual == "Neutral"):
            logging.info(f"Tendencia bajista o neutral detectada para una compra. Se recomienda cerrar la operación.")
            return True
        # Verificamos si la operación es de venta y la tendencia es alcista o neutral
        elif tipo_operacion == 'venta' and (tendencia_actual == "Tendencia Alcista" or tendencia_actual == "Neutral"):
            logging.info(f"Tendencia alcista o neutral detectada para una venta. Se recomienda cerrar la operación.")
            return True

        # Si no se detecta una tendencia contraria, no se cierra la operación
        logging.info(f"Tendencia actual no es contraria. No se cierra la operación.")
        return False

    def obtener_operacion_abierta(self, symbol):
        """
        Devuelve la operación abierta para el símbolo dado si existe.
        """
        symbol_normalizado = self.normalizar_par(symbol)
        for operacion in self.mt5_executor.operaciones_abiertas.values():
            operacion_symbol_normalizado = self.normalizar_par(operacion['symbol'])
            logging.debug(f"Comparando {symbol_normalizado} con {operacion_symbol_normalizado}")
            if operacion_symbol_normalizado == symbol_normalizado:
                return operacion
        return None

    def cerrar_conexion_mt5(self):
        """Cierra la conexión con MetaTrader 5 para liberar recursos."""
        try:
            logging.info("Cerrando conexión con MetaTrader 5...")
            mt5.shutdown()
            logging.info("Conexión con MetaTrader 5 cerrada correctamente.")
        except Exception as e:
            logging.error(f"Error al cerrar la conexión con MetaTrader 5: {e}")
            logging.error(traceback.format_exc())
