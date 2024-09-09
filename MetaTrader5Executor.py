import MetaTrader5 as mt5
import threading

class MetaTrader5Executor:
    def __init__(self, close_conditions):
        self.conectado = False
        self.operaciones_abiertas = {}  # Guardar las operaciones activas para monitoreo
        self.close_conditions = close_conditions  # Instancia de la clase TradeCloseConditions

    def conectar_mt5(self):
        """
        Establece la conexión con MetaTrader 5.
        """
        print("Intentando conectar con MetaTrader 5...")
        if not mt5.initialize():
            print(f"Error al conectar con MetaTrader 5, código de error = {mt5.last_error()}")
            return False
        self.conectado = True
        print("Conexión con MetaTrader 5 exitosa. Sincronizando operaciones existentes...")
        self.sincronizar_operaciones_existentes()  # Sincronizar operaciones abiertas al iniciar
        return True

    def sincronizar_operaciones_existentes(self):
        """
        Sincroniza las posiciones abiertas de MetaTrader 5 con el diccionario 'operaciones_abiertas'.
        """
        print("Sincronizando operaciones existentes...")
        posiciones = self.obtener_posiciones_abiertas()
        for posicion in posiciones:
            symbol = posicion['symbol']
            tipo_operacion = 'compra' if posicion['type'] == mt5.ORDER_TYPE_BUY else 'venta'
            self.operaciones_abiertas[symbol] = {'id': posicion['ticket'], 'tipo': tipo_operacion}
            print(f"Operación sincronizada: {symbol}, Tipo: {tipo_operacion}")

    def obtener_posiciones_abiertas(self):
        """
        Devuelve una lista de las posiciones abiertas actualmente en formato de diccionario.
        """
        posiciones = mt5.positions_get()
        if posiciones is None:
            print("No hay posiciones abiertas.")
            return []

        return [{'symbol': posicion.symbol, 'ticket': posicion.ticket, 'type': posicion.type} for posicion in posiciones]

    def verificar_operacion_existente(self, symbol, order_type):
        """
        Verifica si ya existe una operación del mismo tipo (compra/venta) para un símbolo.
        :param symbol: Símbolo de la divisa.
        :param order_type: Tipo de la orden ('buy' o 'sell').
        :return: True si ya existe una operación del mismo tipo, False si no existe.
        """
        if symbol in self.operaciones_abiertas:
            tipo_operacion = self.operaciones_abiertas[symbol]['tipo']
            if (tipo_operacion == 'compra' and order_type == 'buy') or (tipo_operacion == 'venta' and order_type == 'sell'):
                print(f"Ya existe una operación {order_type.upper()} abierta para {symbol}. No se abrirá otra.")
                return True
        return False

    def ejecutar_orden(self, symbol, order_type):
        """
        Ejecuta una orden de compra o venta en MetaTrader 5, solo si no hay una operación abierta del mismo tipo.
        """
        if not self.conectado or not self.seleccionar_simbolo(symbol):
            return

        # Verificar si ya existe una operación del mismo tipo
        if self.verificar_operacion_existente(symbol, order_type):
            return  # Si ya hay una operación del mismo tipo, no ejecutar otra

        price = mt5.symbol_info_tick(symbol).ask if order_type == "buy" else mt5.symbol_info_tick(symbol).bid
        request = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": symbol,
            "volume": 0.1,  # ejemplo de volumen
            "type": mt5.ORDER_TYPE_BUY if order_type == "buy" else mt5.ORDER_TYPE_SELL,
            "price": price,
            "deviation": 20,
            "magic": 234000,
            "comment": "Orden automática",
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": mt5.ORDER_FILLING_FOK,
        }
        result = mt5.order_send(request)
        if result.retcode != mt5.TRADE_RETCODE_DONE:
            print(f"Error al ejecutar orden: {mt5.last_error()}")
        else:
            print(f"Orden ejecutada con éxito: {symbol}, Tipo: {order_type}, Precio: {result.price}")
            self.operaciones_abiertas[symbol] = {'id': result.order, 'tipo': order_type}

    def cerrar_posicion(self, symbol, ticket):
        """
        Cierra una posición en MetaTrader 5 basada en el ticket de la posición.
        """
        posicion = mt5.positions_get(ticket=ticket)
        if posicion is None or len(posicion) == 0:
            print(f"No se encontró la posición: {ticket}")
            return False

        # Determinar el precio adecuado para cerrar la posición
        precio = mt5.symbol_info_tick(symbol).bid if posicion[0].type == mt5.ORDER_TYPE_BUY else mt5.symbol_info_tick(symbol).ask
        request = {
            "action": mt5.TRADE_ACTION_DEAL,
            "position": ticket,
            "symbol": symbol,
            "volume": posicion[0].volume,
            "type": mt5.ORDER_TYPE_SELL if posicion[0].type == mt5.ORDER_TYPE_BUY else mt5.ORDER_TYPE_BUY,
            "price": precio,
            "deviation": 20,
            "magic": 234000,
            "comment": "Cierre automático"
        }
        result = mt5.order_send(request)
        if result.retcode != mt5.TRADE_RETCODE_DONE:
            print(f"No se pudo cerrar la posición: {symbol}, Ticket: {ticket}, Error: {mt5.last_error()}")
            return False
        else:
            del self.operaciones_abiertas[symbol]
            print(f"Posición cerrada: {symbol}, Ticket: {ticket}")
            return True

    def seleccionar_simbolo(self, symbol):
        """
        Asegura que el símbolo esté seleccionado en MetaTrader 5 para realizar operaciones.
        """
        if not mt5.symbol_select(symbol, True):
            print(f"No se pudo seleccionar el símbolo: {symbol}")
            return False
        return True

    def cerrar_conexion(self):
        """
        Cierra la conexión con MetaTrader 5.
        """
        if mt5.shutdown():
            self.conectado = False
            print("Conexión con MetaTrader 5 cerrada.")
