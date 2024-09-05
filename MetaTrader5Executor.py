import MetaTrader5 as mt5
import threading

class MetaTrader5Executor:
    def __init__(self):
        self.conectado = False
        self.operaciones_abiertas = {}  # Guardar las operaciones activas para monitoreo

    def conectar_mt5(self):
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
        if isinstance(posiciones, list):
            print(f"Lista de posiciones abiertas procesada: {posiciones}")
            for posicion in posiciones:
                print(f"Procesando posición: {posicion}")
                symbol = posicion['symbol']
                position_id = posicion['ticket']
                if symbol and position_id:
                    self.operaciones_abiertas[symbol] = position_id
                    print(f"Operación sincronizada: {symbol}, ID: {position_id}")
        else:
            print(f"Error: Se esperaba una lista de posiciones, pero se recibió {type(posiciones)}")

    def seleccionar_simbolo(self, symbol):
        symbol_info = mt5.symbol_info(symbol)
        if symbol_info is None:
            return False
        
        if not symbol_info.visible:
            if not mt5.symbol_select(symbol, True):
                return False
        return True

    def ejecutar_orden(self, symbol, order_type):
        """
        Ejecuta la orden de compra o venta y luego ajusta el SL y TP basados en el precio de ejecución.
        """
        if not self.conectado:
            return
        
        if not self.seleccionar_simbolo(symbol):
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
        result = mt5.order_send(request)
        if result is None or result.retcode != mt5.TRADE_RETCODE_DONE:
            return None
        self.operaciones_abiertas[symbol] = result.order
        
        # Después de la ejecución, ajustar SL y TP
        print(f"Orden ejecutada para {symbol} con precio {result.price}. Ajustando SL y TP...")
        self.ajustar_stop_loss_take_profit(symbol, result.order, result.price, order_type)

        return result.order

    def ajustar_stop_loss_take_profit(self, symbol, position_id, precio_ejecucion, order_type):
        """
        Ajusta el Stop Loss y el Take Profit basados en el precio de ejecución.
        """
        # Ejemplo de cómo obtener los valores de SL y TP desde un archivo config.json
        # Suponiendo que `self.config` tiene estos valores cargados
        stop_loss_factor = 1.5  # Ejemplo: 1.5 veces el ATR (se puede ajustar dinámicamente)
        take_profit_factor = 3.0  # Ejemplo: 3 veces el ATR

        atr_value = self.calcular_atr(symbol)  # Supongamos que tienes una función para calcular el ATR

        if atr_value is None:
            print(f"No se pudo calcular el ATR para {symbol}, no se ajustarán SL y TP.")
            return

        # Calcular los niveles de SL y TP
        stop_loss = precio_ejecucion - (atr_value * stop_loss_factor) if order_type == "buy" else precio_ejecucion + (atr_value * stop_loss_factor)
        take_profit = precio_ejecucion + (atr_value * take_profit_factor) if order_type == "buy" else precio_ejecucion - (atr_value * take_profit_factor)

        print(f"Ajustando Stop Loss a {stop_loss} y Take Profit a {take_profit} para {symbol}")

        # Enviar la modificación para ajustar el SL y TP
        request = {
            "action": mt5.TRADE_ACTION_SLTP,
            "position": position_id,
            "symbol": symbol,
            "sl": stop_loss,
            "tp": take_profit,
        }
        result = mt5.order_send(request)
        if result is None or result.retcode != mt5.TRADE_RETCODE_DONE:
            print(f"Error al ajustar SL y TP para {symbol}")
        else:
            print(f"SL y TP ajustados correctamente para {symbol}")

    def cerrar_posicion(self, symbol, position_id):
        if not self.conectado:
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
            return False
        if symbol in self.operaciones_abiertas:
            del self.operaciones_abiertas[symbol]
        return True

    def obtener_posiciones_abiertas(self):
        """
        Devuelve una lista de las posiciones abiertas actualmente en formato de diccionario.
        """
        posiciones = mt5.positions_get()
        if posiciones is None:
            return []

        lista_posiciones = []
        for posicion in posiciones:
            posicion_diccionario = {
                'symbol': posicion.symbol,
                'ticket': posicion.ticket,
                'type': posicion.type
            }
            lista_posiciones.append(posicion_diccionario)
        print(f"Lista de posiciones abiertas procesada: {lista_posiciones}")
        return lista_posiciones

    def procesar_reversion(self, symbol, resultado_reversion):
        """
        Procesa una reversión detectada y ejecuta la lógica de trading.
        """
        if "Alcista" in resultado_reversion:
            self.ejecutar_orden(symbol, "buy")
        elif "Bajista" in resultado_reversion:
            self.ejecutar_orden(symbol, "sell")

    def monitorear_operaciones(self):
        """
        Monitorea las posiciones abiertas y cierra aquellas que cumplan ciertos criterios.
        """
        print("Monitoreando operaciones...")
        try:
            posiciones = self.obtener_posiciones_abiertas()
            print(f"Posiciones obtenidas durante el monitoreo: {posiciones}")
            
            if not isinstance(posiciones, list):
                print(f"Error: Se esperaba una lista pero se recibió {type(posiciones)}")
                return
            
            for posicion in posiciones:
                print(f"Revisando posición: {posicion}")
                symbol = posicion.get('symbol')
                position_id = posicion.get('ticket')
                print(f"Revisando posición para {symbol} con ID {position_id}")
                
                # Aquí se puede agregar la lógica para cerrar posiciones si se cumplen ciertos criterios.
                # Ejemplo:
                # if self.debe_cerrar_posicion(symbol, position_id):
                #     self.cerrar_posicion(symbol, position_id)

        except Exception as e:
            print(f"Error durante el monitoreo de cierres: {str(e)}")

    def iniciar_monitoreo(self):
        thread = threading.Thread(target=self.monitorear_operaciones)
        thread.daemon = True
        thread.start()

    def cerrar_conexion(self):
        mt5.shutdown()
        self.conectado = False

    def calcular_atr(self, symbol):
        """
        Aquí deberías implementar la lógica para calcular el ATR del símbolo.
        """
        # Esta función debería devolver el valor del ATR calculado.
        # Por ahora devolveremos un valor de ejemplo para pruebas.
        return 0.0015  # Valor de ejemplo
