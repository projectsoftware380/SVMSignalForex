import MetaTrader5 as mt5
import time
import threading

class MetaTrader5Executor:
    def __init__(self):
        self.conectado = False
        self.operaciones_abiertas = {}  # Guardar las operaciones activas para un posible monitoreo

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
        self.operaciones_abiertas[symbol] = result.order  # Guarda la operación activa
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
        if symbol in self.operaciones_abiertas:
            del self.operaciones_abiertas[symbol]  # Elimina la operación cerrada del monitoreo
        return True  # Retorna True si tiene éxito

    def obtener_posiciones_abiertas(self):
        """
        Devuelve todas las posiciones abiertas actualmente.
        """
        posiciones = mt5.positions_get()
        if posiciones is None:
            print(f"Error obteniendo posiciones abiertas, código de error: {mt5.last_error()}")
            return []
        
        # Convertir las posiciones a un formato manejable (diccionario)
        posiciones_abiertas = []
        for posicion in posiciones:
            posiciones_abiertas.append({
                'symbol': posicion.symbol,
                'ticket': posicion.ticket,
                'type': posicion.type,
            })
        return posiciones_abiertas

    def monitorear_operaciones(self):
        """
        Monitorea continuamente las operaciones abiertas y aplica las condiciones de cierre.
        """
        while True:
            for symbol, position_id in list(self.operaciones_abiertas.items()):
                print(f"Monitoreando operación {symbol} con ID {position_id}")
                # Aquí puedes integrar las condiciones usando las otras clases ForexAnalyzer, etc.
                
                # Este bloque es un placeholder para implementar la lógica de cierre
                tendencia_actual = "Neutral"  # Esta sería la tendencia obtenida de ForexAnalyzer
                if tendencia_actual == "Neutral":
                    print(f"Cerrando posición para {symbol} debido a cambio de tendencia a Neutral.")
                    self.cerrar_posicion(symbol, position_id)

            time.sleep(60)  # Espera 1 minuto antes de volver a monitorear

    def iniciar_monitoreo(self):
        """
        Inicia el monitoreo de las operaciones en un hilo separado.
        """
        thread = threading.Thread(target=self.monitorear_operaciones)
        thread.daemon = True
        thread.start()

    def cerrar_conexion(self):
        mt5.shutdown()
        self.conectado = False


# Ejemplo de uso
if __name__ == "__main__":
    executor = MetaTrader5Executor()
    
    if executor.conectar_mt5():
        # Iniciar el monitoreo en un hilo separado
        executor.iniciar_monitoreo()

        # Ejemplo de ejecución de órdenes
        order_id = executor.ejecutar_orden("USDJPY", "buy")
        
        # Esperar antes de cerrar la posición
        time.sleep(2)
        
        # Ejemplo de cierre de posiciones
        if order_id:
            executor.cerrar_posicion("USDJPY", order_id)
        
        # Cerrar la conexión cuando hayas terminado
        executor.cerrar_conexion()
