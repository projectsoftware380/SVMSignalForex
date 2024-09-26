import MetaTrader5 as mt5
import time
import threading
import sys
import logging

# Configurar el logging
logging.basicConfig(filename='logs/trading_system.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

class MetaTrader5Executor:
    def __init__(self, max_loss_level, profit_trailing_level, trailing_stop_percentage):
        self.equity_inicial = None
        self.max_profit = 0
        self.max_loss_level = max_loss_level
        self.profit_trailing_level = profit_trailing_level
        self.trailing_stop_percentage = trailing_stop_percentage
        self.conectado = False
        self.operaciones_abiertas = {}
        self.lock = threading.Lock()

        # Conectar a MetaTrader 5
        if not self.conectar_mt5():
            logging.error("No se pudo conectar con MetaTrader 5.")
            raise ConnectionError("No se pudo conectar con MetaTrader 5.")

        # Obtener equidad inicial de la cuenta
        account_info = mt5.account_info()
        if account_info is None:
            logging.error("No se pudo obtener la información de la cuenta.")
            raise ValueError("No se pudo obtener la información de la cuenta.")
        self.equity_inicial = account_info.equity
        logging.info(f"Equidad inicial obtenida: {self.equity_inicial}")

        # Inicializar las operaciones abiertas
        self.actualizar_operaciones_abiertas()

    def normalizar_par(self, pair):
        """Normaliza el nombre del par de divisas."""
        return pair.replace("-", "").replace(".", "").upper()

    def conectar_mt5(self):
        """Conecta a MetaTrader 5."""
        logging.info("Intentando conectar a MetaTrader 5...")
        if not mt5.initialize():
            logging.error(f"Error al conectar a MetaTrader 5: {mt5.last_error()}")
            return False
        self.conectado = True
        logging.info("Conexión a MetaTrader 5 exitosa.")
        return True

    def actualizar_operaciones_abiertas(self):
        """Actualiza el diccionario con las operaciones abiertas actuales."""
        with self.lock:
            self.operaciones_abiertas.clear()
            posiciones = mt5.positions_get()
            if posiciones is None or len(posiciones) == 0:
                logging.info("No hay posiciones abiertas.")
                return

            for pos in posiciones:
                tipo_operacion = 'compra' if pos.type == 0 else 'venta'
                symbol_normalizado = self.normalizar_par(pos.symbol)
                self.operaciones_abiertas[symbol_normalizado] = {
                    'symbol': symbol_normalizado,
                    'ticket': pos.ticket,
                    'type': tipo_operacion,
                    'volume': pos.volume
                }
            logging.info(f"Operaciones abiertas actualizadas: {self.operaciones_abiertas}")

    def obtener_posiciones_abiertas(self):
        """Devuelve una lista de posiciones abiertas en formato de diccionario."""
        self.actualizar_operaciones_abiertas()  # Asegura que las posiciones están actualizadas
        posiciones = list(self.operaciones_abiertas.values())
        logging.info(f"Posiciones abiertas: {posiciones}")
        return posiciones

    def ejecutar_orden(self, symbol, order_type):
        """Ejecuta una orden de compra o venta en MetaTrader 5."""
        with self.lock:
            logging.info(f"Ejecutando orden {order_type} para {symbol}")

            # Verificar si ya hay una operación abierta con el mismo símbolo y tipo
            symbol_normalizado = self.normalizar_par(symbol)
            posiciones_abiertas = self.obtener_posiciones_abiertas()
            for posicion in posiciones_abiertas:
                if posicion['symbol'] == symbol_normalizado and posicion['type'] == ('compra' if order_type == 'buy' else 'venta'):
                    logging.info(f"Ya existe una operación {order_type} abierta para {symbol}. No se abrirá una nueva.")
                    return

            # Asegurarse de que el símbolo esté disponible
            symbol_info = mt5.symbol_info(symbol)
            if symbol_info is None or not symbol_info.visible:
                logging.warning(f"{symbol} no encontrado o no visible, intentando agregarlo.")
                if not mt5.symbol_select(symbol, True):
                    logging.error(f"No se pudo agregar {symbol}.")
                    return

            # Obtener el precio actual
            symbol_info_tick = mt5.symbol_info_tick(symbol)
            if symbol_info_tick is None:
                logging.error(f"No se pudo obtener el precio actual para {symbol}.")
                return

            price = symbol_info_tick.ask if order_type == 'buy' else symbol_info_tick.bid
            if price is None:
                logging.error(f"Precio no disponible para {symbol} al intentar {order_type}.")
                return

            trade_type = mt5.ORDER_TYPE_BUY if order_type == 'buy' else mt5.ORDER_TYPE_SELL

            request = {
                "action": mt5.TRADE_ACTION_DEAL,
                "symbol": symbol,
                "volume": 0.01,
                "type": trade_type,
                "price": price,
                "deviation": 20,
                "magic": 234000,
                "comment": "Orden automática"
            }

            result = mt5.order_send(request)
            if result is None:
                logging.error(f"Error al ejecutar la orden para {symbol}: No se recibió respuesta de MetaTrader 5.")
            elif result.retcode == mt5.TRADE_RETCODE_DONE:
                logging.info(f"Orden ejecutada con éxito para {symbol} a un precio de {price}")
                self.actualizar_operaciones_abiertas()
            else:
                logging.error(f"Error al ejecutar la orden para {symbol}. Código de error: {result.retcode}")
                logging.error(f"Detalle completo del resultado: {result}")

    def cerrar_operacion(self, ticket):
        """Cierra la operación con el ticket dado."""
        with self.lock:
            # Buscar la operación en las operaciones abiertas
            posicion = next((op for op in self.operaciones_abiertas.values() if op['ticket'] == ticket), None)
            if posicion is None:
                logging.error(f"No se encontró la operación con el ticket {ticket}.")
                return

            symbol = posicion['symbol']
            close_type = mt5.ORDER_TYPE_SELL if posicion['type'] == 'compra' else mt5.ORDER_TYPE_BUY

            # Obtener el precio adecuado para cerrar la operación
            tick_info = mt5.symbol_info_tick(symbol)
            if tick_info is None:
                logging.error(f"No se pudo obtener el precio actual para {symbol}. Operación no cerrada.")
                return

            price = tick_info.bid if posicion['type'] == 'compra' else tick_info.ask

            # Preparar la solicitud de cierre de la operación
            request = {
                "action": mt5.TRADE_ACTION_DEAL,
                "symbol": symbol,
                "volume": posicion['volume'],
                "type": close_type,
                "position": ticket,
                "price": price,
                "deviation": 20,
                "magic": 234000,
                "comment": "Cierre automático"
            }

            # Intentar cerrar la operación
            result = mt5.order_send(request)
            if result is None:
                logging.error(f"Error al cerrar la operación {ticket}: No se recibió respuesta de MetaTrader 5.")
            elif result.retcode == mt5.TRADE_RETCODE_DONE:
                logging.info(f"Operación {ticket} cerrada correctamente.")
                del self.operaciones_abiertas[symbol]  # Eliminar la operación cerrada del diccionario
            else:
                logging.error(f"Error al cerrar la operación {ticket}. Código de error: {result.retcode}")

    def monitorear_equidad_global(self, monitoreo_interval=60):
        """
        Monitorea la equidad global y toma acción cuando se alcanzan los niveles de pérdida o ganancia establecidos.
        Detiene el programa una vez se cumplen las condiciones de cierre.
        """
        while True:
            account_info = mt5.account_info()
            if account_info is None:
                logging.error("No se pudo obtener la información de la cuenta.")
                return

            equity_actual = account_info.equity
            logging.info(f"Equidad actual: {equity_actual}")

            # Verificar si se ha alcanzado el nivel de pérdida máxima
            if equity_actual <= self.max_loss_level:
                logging.info(f"Equidad por debajo del nivel de pérdida máxima ({self.max_loss_level}). Cerrando todas las operaciones...")
                self.cerrar_todas_las_operaciones()
                break

            # Verificar si se ha alcanzado el nivel de trailing stop de ganancias
            if equity_actual >= self.profit_trailing_level:
                self.max_profit = max(self.max_profit, equity_actual)
                trailing_stop = self.max_profit * (1 - self.trailing_stop_percentage / 100)
                if equity_actual <= trailing_stop:
                    logging.info(f"Activado el trailing stop de ganancias. Cerrando todas las operaciones...")
                    self.cerrar_todas_las_operaciones()
                    break

            time.sleep(monitoreo_interval)

        logging.info("Deteniendo el programa debido a pérdida o ganancia alcanzada.")
        self.detener_programa()

    def cerrar_todas_las_operaciones(self):
        """Cierra todas las posiciones abiertas."""
        posiciones = mt5.positions_get()
        if posiciones is None or len(posiciones) == 0:
            logging.info("No hay posiciones abiertas.")
            return

        for posicion in posiciones:
            symbol = posicion.symbol
            ticket = posicion.ticket
            logging.info(f"Cerrando posición para {symbol}, ticket {ticket}")
            self.cerrar_operacion(ticket)

    def detener_programa(self):
        """Detiene el programa completamente."""
        logging.info("Cerrando conexión a MetaTrader 5 y deteniendo el programa.")
        self.cerrar_conexion()
        sys.exit()

    def cerrar_conexion(self):
        """Cierra la conexión con MetaTrader 5."""
        mt5.shutdown()
        self.conectado = False
        logging.info("Conexión con MetaTrader 5 cerrada.")
