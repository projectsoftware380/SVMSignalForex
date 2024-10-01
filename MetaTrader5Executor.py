import MetaTrader5 as mt5
import pandas as pd
import time

class MetaTrader5Executor:
    def __init__(self, close_conditions, atr_period=14, atr_factor=2.0, atr_timeframe=mt5.TIMEFRAME_M15, stop_loss_default=0.01):
        self.conectado = False
        self.operaciones_abiertas = {}  # Almacenar operaciones activas
        self.close_conditions = close_conditions  # Instancia de clase TradeCloseConditions
        self.atr_period = atr_period
        self.atr_factor = atr_factor
        self.atr_timeframe = atr_timeframe
        self.stop_loss_default = stop_loss_default
        
        # Conectar a MetaTrader 5
        if not self.conectar_mt5():
            raise ConnectionError("No se pudo conectar con MetaTrader 5.")
        
        # Obtener información de la cuenta
        account_info = mt5.account_info()
        if account_info is None:
            raise ValueError("No se pudo obtener la información de la cuenta.")
        
        self.balance_inicial = account_info.balance
        self.max_profit = 0

        # Sincronizar operaciones existentes al inicio
        self.sincronizar_operaciones_existentes()

    def conectar_mt5(self):
        """Establece la conexión con MetaTrader 5."""
        print("Intentando conectar con MetaTrader 5...")
        if not mt5.initialize():
            print(f"Error al conectar: {mt5.last_error()}")
            return False
        self.conectado = True
        print("Conexión establecida con éxito.")
        return True

    def seleccionar_simbolo(self, symbol):
        """Selecciona el símbolo para operar en MetaTrader 5."""
        if not mt5.symbol_select(symbol, True):
            print(f"Error al seleccionar el símbolo {symbol}: {mt5.last_error()}")
            return False
        return True

    def sincronizar_operaciones_existentes(self):
        """Sincroniza las posiciones abiertas en MetaTrader 5."""
        print("Sincronizando operaciones existentes...")
        posiciones = self.obtener_posiciones_abiertas()
        if not posiciones:
            print("No se encontraron operaciones abiertas.")
            return

        for posicion in posiciones:
            symbol = posicion['symbol']
            tipo_operacion = 'compra' if posicion['type'] == mt5.ORDER_TYPE_BUY else 'venta'
            price = posicion['price_open']

            # Recalcular el ATR para ajustar el stop loss
            atr_value = self.obtener_atr(symbol)
            if atr_value is None:
                print(f"Error al calcular ATR para {symbol}. No se puede ajustar el stop loss.")
                continue  # Salir del bucle si no se puede calcular el ATR

            stop_loss = price - (self.atr_factor * atr_value) if tipo_operacion == 'compra' else price + (self.atr_factor * atr_value)
            self.operaciones_abiertas[symbol] = {'id': posicion['ticket'], 'tipo': tipo_operacion, 'precio_entrada': price, 'stop_loss': stop_loss}
            print(f"Operación sincronizada: {tipo_operacion.capitalize()} en {symbol}, Precio de entrada: {price}, Stop Loss: {stop_loss}")

    def obtener_posiciones_abiertas(self):
        """Devuelve una lista de posiciones abiertas en formato de diccionario."""
        posiciones = mt5.positions_get()
        if posiciones is None:
            print("Error al obtener posiciones: No hay posiciones abiertas.")
            return []

        print(f"Se encontraron {len(posiciones)} posiciones abiertas.")
        return [{'symbol': pos.symbol, 'ticket': pos.ticket, 'type': pos.type, 'price_open': pos.price_open} for pos in posiciones]

    def obtener_atr(self, symbol):
        """Calcula el ATR (Average True Range) para un símbolo."""
        rates = mt5.copy_rates_from_pos(symbol, self.atr_timeframe, 0, self.atr_period + 1)
        if rates is None or len(rates) < self.atr_period:
            print(f"No se pudo obtener el ATR para {symbol}")
            return None

        df = pd.DataFrame(rates)
        df['tr'] = df['high'] - df['low']
        atr = df['tr'].rolling(window=self.atr_period).mean().iloc[-1]
        return atr

    def ejecutar_orden(self, symbol, order_type):
        """Ejecuta una orden de compra o venta si no existe una operación abierta del mismo tipo."""
        if not self.conectado or not self.seleccionar_simbolo(symbol):
            return

        if self.verificar_operacion_existente(symbol, order_type):
            print(f"Operación ya existente: {symbol}, {order_type}.")
            return

        price = mt5.symbol_info_tick(symbol).ask if order_type == "buy" else mt5.symbol_info_tick(symbol).bid

        request = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": symbol,
            "volume": 1.0,
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

    def verificar_operacion_existente(self, symbol, order_type):
        """Verifica si ya existe una operación abierta del mismo tipo."""
        if symbol in self.operaciones_abiertas:
            tipo_operacion = self.operaciones_abiertas[symbol]['tipo']
            if (tipo_operacion == 'compra' and order_type == 'buy') or (tipo_operacion == 'venta' and order_type == 'sell'):
                return True

        posiciones = self.obtener_posiciones_abiertas()
        for posicion in posiciones:
            if posicion['symbol'] == symbol and (
                (order_type == 'buy' and posicion['type'] == mt5.ORDER_TYPE_BUY) or 
                (order_type == 'sell' and posicion['type'] == mt5.ORDER_TYPE_SELL)):
                return True
        return False

    def monitorear_balance_global(self, max_loss_level, profit_trailing_level, trailing_stop_percentage):
        """Monitorea las pérdidas y ganancias globales de la cuenta."""
        while True:
            account_info = mt5.account_info()
            if account_info is None:
                print("No se pudo obtener la información de la cuenta.")
                return

            balance_actual = account_info.balance
            ganancia_neta = balance_actual - self.balance_inicial

            if ganancia_neta <= max_loss_level:
                print(f"Nivel de pérdida alcanzado: {ganancia_neta}. Cerrando operaciones.")
                self.cerrar_todas_las_operaciones()
                mt5.shutdown()
                break

            if ganancia_neta >= profit_trailing_level:
                self.max_profit = max(self.max_profit, ganancia_neta)
                trailing_stop_value = self.max_profit * (1 - trailing_stop_percentage / 100)
                if ganancia_neta <= trailing_stop_value:
                    print(f"Trailing Stop activado. Ganancia: {ganancia_neta}")
                    self.cerrar_todas_las_operaciones()
                    mt5.shutdown()
                    break

            time.sleep(60)

    def cerrar_todas_las_operaciones(self):
        """Cierra todas las operaciones abiertas en MetaTrader 5."""
        posiciones = self.obtener_posiciones_abiertas()
        for posicion in posiciones:
            self.cerrar_posicion(posicion['symbol'], posicion['ticket'])

    def cerrar_posicion(self, symbol, ticket):
        """Cierra una posición abierta en MetaTrader 5."""
        posicion = mt5.positions_get(ticket=ticket)
        if not posicion:
            return False

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
            print(f"Error al cerrar posición: {symbol}, Ticket: {ticket}")
            return False
        else:
            del self.operaciones_abiertas[symbol]
            print(f"Posición cerrada: {symbol}, Ticket: {ticket}")
            return True
