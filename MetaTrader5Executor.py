import MetaTrader5 as mt5
import time
import threading

class MetaTrader5Executor:
    def __init__(self):
        self.conectado = False
        self.operaciones_abiertas = {}  # Guardar las operaciones activas para monitoreo

    def conectar_mt5(self):
        if not mt5.initialize():
            print("Error al conectar con MetaTrader 5, código de error =", mt5.last_error())
            return False
        self.conectado = True
        self.sincronizar_operaciones_existentes()  # Sincronizar operaciones abiertas al iniciar
        return True

    def sincronizar_operaciones_existentes(self):
        """
        Sincroniza las posiciones abiertas de MetaTrader 5 con el diccionario 'operaciones_abiertas'.
        """
        posiciones = self.obtener_posiciones_abiertas()
        for posicion in posiciones:
            symbol = posicion.get('symbol')
            position_id = posicion.get('ticket')
            if symbol and position_id:
                self.operaciones_abiertas[symbol] = position_id
        print(f"Sincronización completada: {len(self.operaciones_abiertas)} operaciones preexistentes sincronizadas.")

    def seleccionar_simbolo(self, symbol):
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
        
        if not self.seleccionar_simbolo(symbol):
            return

        symbol_info = mt5.symbol_info(symbol)
        if symbol_info is None or not symbol_info.visible:
            print(f"El símbolo {symbol} no está disponible o visible.")
            return

        lot = 0.1
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
            return None
        print(f"Orden ejecutada exitosamente, ID de orden: {result.order}")
        self.operaciones_abiertas[symbol] = result.order
        return result.order

    def cerrar_posicion(self, symbol, position_id):
        if not self.conectado:
            print("Intento de cerrar posición sin conexión.")
            return
        
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
            return False
        print(f"Posición cerrada exitosamente.")
        if symbol in self.operaciones_abiertas:
            del self.operaciones_abiertas[symbol]
        return True

    def procesar_reversion(self, symbol, tipo_operacion):
        """
        Procesa la reversión detectada y toma acciones en consecuencia.
        Si ya hay una operación abierta en la dirección contraria, la cierra.
        """
        posiciones_abiertas = self.obtener_posiciones_abiertas()

        for posicion in posiciones_abiertas:
            if posicion.get('symbol') == symbol:
                if (posicion.get('type') == mt5.ORDER_TYPE_BUY and tipo_operacion == 'sell') or \
                   (posicion.get('type') == mt5.ORDER_TYPE_SELL and tipo_operacion == 'buy'):
                    print(f"Se detectó una señal contraria para {symbol}, cerrando posición.")
                    self.cerrar_posicion(symbol, posicion.get('ticket'))
                    return True
        return False

    def obtener_posiciones_abiertas(self):
        """
        Devuelve una lista de las posiciones abiertas actualmente.
        """
        posiciones = mt5.positions_get()
        if posiciones is None:
            print(f"Error obteniendo posiciones: {mt5.last_error()}")
            return []
        
        lista_posiciones = []
        for posicion in posiciones:
            lista_posiciones.append({
                'symbol': posicion.symbol,
                'ticket': posicion.ticket,
                'type': posicion.type
            })
        return lista_posiciones

    def monitorear_operaciones(self):
        """
        Monitorea todas las operaciones abiertas, tanto preexistentes como nuevas,
        para cerrar las que no cumplen con las condiciones.
        """
        while True:
            posiciones_abiertas = self.obtener_posiciones_abiertas()  # Obtener todas las posiciones abiertas
            for posicion in posiciones_abiertas:
                symbol = posicion.get('symbol')
                position_id = posicion.get('ticket')
                print(f"Monitoreando operación {symbol} con ID {position_id}")
                # Implementar lógica de análisis de tendencia o reversión para cerrar la posición si es necesario
                tendencia_actual = "Neutral"  # Esto sería reemplazado por la lógica real de análisis
                if tendencia_actual == "Neutral":
                    print(f"Cerrando posición para {symbol} debido a cambio de tendencia a Neutral.")
                    self.cerrar_posicion(symbol, position_id)
            time.sleep(60)  # Intervalo de monitoreo

    def iniciar_monitoreo(self):
        """
        Inicia el monitoreo de operaciones en un hilo separado.
        """
        thread = threading.Thread(target=self.monitorear_operaciones)
        thread.daemon = True
        thread.start()

    def cerrar_conexion(self):
        mt5.shutdown()
        self.conectado = False
