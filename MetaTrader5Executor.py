import MetaTrader5 as mt5
import time

class MetaTrader5Executor:
    def __init__(self):
        self.conectado = False

    def conectar_mt5(self):
        if not mt5.initialize():
            print("Error al conectar con MetaTrader 5, código de error =", mt5.last_error())
            return False
        self.conectado = True
        return True

    def seleccionar_simbolo(self, symbol):
        """
        Se asegura de que el símbolo esté visible en MarketWatch.
        """
        symbol_info = mt5.symbol_info(symbol)
        if symbol_info is None:
            print(f"El símbolo {symbol} no se encuentra disponible.")
            return False
        
        if not symbol_info.visible:
            print(f"El símbolo {symbol} no está visible. Intentando hacerlo visible...")
            if not mt5.symbol_select(symbol, True):
                print(f"No se pudo seleccionar el símbolo {symbol}, código de error: {mt5.last_error()}")
                return False
            print(f"Símbolo {symbol} visible en MarketWatch.")
        return True

    def ejecutar_orden(self, symbol, order_type):
        if not self.conectado:
            print("Intento de ejecutar orden sin conexión.")
            return
        
        # Asegurarse de que el símbolo esté visible en MarketWatch
        if not self.seleccionar_simbolo(symbol):
            return

        # Preparar el símbolo
        symbol_info = mt5.symbol_info(symbol)
        if symbol_info is None or not symbol_info.visible:
            print(f"El símbolo {symbol} no está disponible o visible.")
            return

        # Preparar los detalles de la orden
        lot = 0.1  # Tamaño del lote
        price = mt5.symbol_info_tick(symbol).ask if order_type == "buy" else mt5.symbol_info_tick(symbol).bid
        deviation = 20
        order_type_mt5 = mt5.ORDER_TYPE_BUY if order_type == "buy" else mt5.ORDER_TYPE_SELL

        request = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": symbol,
            "volume": lot,
            "type": order_type_mt5,
            "price": price,
            "deviation": deviation,
            "magic": 234000,
            "comment": "Orden automática",
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": mt5.ORDER_FILLING_FOK,
        }
        
        print(f"Enviando orden: {request}")
        result = mt5.order_send(request)
        if result is None or result.retcode != mt5.TRADE_RETCODE_DONE:
            print(f"Fallo al enviar orden: {mt5.last_error()}")
            return None  # Retorna None si falla
        print(f"Orden ejecutada exitosamente, ID de orden: {result.order}")
        return result.order  # Devuelve el ID de la orden si tiene éxito

    def cerrar_posicion(self, symbol, position_id):
        if not self.conectado:
            print("Intento de cerrar posición sin conexión.")
            return
        
        # Cierra la posición abierta basada en el ticket
        price = mt5.symbol_info_tick(symbol).bid
        request = {
            "action": mt5.TRADE_ACTION_DEAL,
            "position": position_id,
            "symbol": symbol,
            "volume": 0.1,
            "type": mt5.ORDER_TYPE_SELL,
            "price": price,
            "deviation": 20,
            "magic": 234000,
            "comment": "Cierre automático",
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": mt5.ORDER_FILLING_FOK,
        }
        result = mt5.order_send(request)
        if result is None or result.retcode != mt5.TRADE_RETCODE_DONE:
            print(f"Fallo al cerrar posición: {mt5.last_error()}")
            return False  # Retorna False si falla
        print(f"Posición cerrada exitosamente.")
        return True  # Retorna True si tiene éxito

    def cerrar_conexion(self):
        mt5.shutdown()
        self.conectado = False

# Ejemplo de uso
if __name__ == "__main__":
    executor = MetaTrader5Executor()
    
    if executor.conectar_mt5():
        # Ejemplo de ejecución de órdenes
        order_id = executor.ejecutar_orden("USDJPY", "buy")
        
        # Esperar antes de cerrar la posición
        time.sleep(2)
        
        # Ejemplo de cierre de posiciones
        if order_id:
            executor.cerrar_posicion("USDJPY", order_id)
        
        # Cerrar la conexión cuando hayas terminado
        executor.cerrar_conexion()
