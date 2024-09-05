import MetaTrader5 as mt5
import threading

class MetaTrader5Executor:
    def __init__(self, close_conditions):
        self.conectado = False
        self.operaciones_abiertas = {}  # Guardar las operaciones activas para monitoreo
        self.close_conditions = close_conditions  # Instancia de la clase TradeCloseConditions

    def estandarizar_simbolo(self, symbol):
        """
        Estandariza el formato del símbolo, removiendo guiones.
        """
        return symbol.replace("-", "")

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
                symbol = self.estandarizar_simbolo(posicion['symbol'])  # Estandarizar símbolo
                position_id = posicion['ticket']
                print(f"Procesando posición: {posicion}")
                if symbol and position_id:
                    self.operaciones_abiertas[symbol] = position_id
                    print(f"Operación sincronizada: {symbol}, ID: {position_id}")
        else:
            print(f"Error: Se esperaba una lista de posiciones, pero se recibió {type(posiciones)}")

    def seleccionar_simbolo(self, symbol):
        symbol_estandarizado = self.estandarizar_simbolo(symbol)
        symbol_info = mt5.symbol_info(symbol_estandarizado)
        if symbol_info is None:
            return False
        
        if not symbol_info.visible:
            if not mt5.symbol_select(symbol_estandarizado, True):
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

        symbol_estandarizado = self.estandarizar_simbolo(symbol)
        lot = 0.1
        price = mt5.symbol_info_tick(symbol_estandarizado).ask if order_type == "buy" else mt5.symbol_info_tick(symbol_estandarizado).bid
        deviation = 20
        order_type_mt5 = mt5.ORDER_TYPE_BUY if order_type == "buy" else mt5.ORDER_TYPE_SELL

        request = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": symbol_estandarizado,
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
        self.operaciones_abiertas[symbol_estandarizado] = result.order
        
        # Después de la ejecución, ajustar SL y TP
        print(f"Orden ejecutada para {symbol_estandarizado} con precio {result.price}. Ajustando SL y TP...")
        self.ajustar_stop_loss_take_profit(symbol_estandarizado, result.order, result.price, order_type)

        return result.order

    def ajustar_stop_loss_take_profit(self, symbol, position_id, precio_ejecucion, order_type):
        """
        Ajusta el Stop Loss y el Take Profit basados en el precio de ejecución.
        """
        symbol_estandarizado = self.estandarizar_simbolo(symbol)
        stop_loss_factor = 1.5  # Ejemplo: 1.5 veces el ATR
        take_profit_factor = 3.0  # Ejemplo: 3 veces el ATR

        atr_value = self.calcular_atr(symbol_estandarizado)

        if atr_value is None:
            print(f"No se pudo calcular el ATR para {symbol_estandarizado}, no se ajustarán SL y TP.")
            return

        stop_loss = precio_ejecucion - (atr_value * stop_loss_factor) if order_type == "buy" else precio_ejecucion + (atr_value * stop_loss_factor)
        take_profit = precio_ejecucion + (atr_value * take_profit_factor) if order_type == "buy" else precio_ejecucion - (atr_value * take_profit_factor)

        print(f"Ajustando Stop Loss a {stop_loss} y Take Profit a {take_profit} para {symbol_estandarizado}")

        request = {
            "action": mt5.TRADE_ACTION_SLTP,
            "position": position_id,
            "symbol": symbol_estandarizado,
            "sl": stop_loss,
            "tp": take_profit,
        }
        result = mt5.order_send(request)
        if result is None or result.retcode != mt5.TRADE_RETCODE_DONE:
            print(f"Error al ajustar SL y TP para {symbol_estandarizado}")
        else:
            print(f"SL y TP ajustados correctamente para {symbol_estandarizado}")

    def cerrar_posicion(self, symbol, position_id):
        symbol_estandarizado = self.estandarizar_simbolo(symbol)
        price = mt5.symbol_info_tick(symbol_estandarizado).bid
        request = {
            "action": mt5.TRADE_ACTION_DEAL,
            "position": position_id,
            "symbol": symbol_estandarizado,
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
        if symbol_estandarizado in self.operaciones_abiertas:
            del self.operaciones_abiertas[symbol_estandarizado]
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
                'symbol': self.estandarizar_simbolo(posicion.symbol),
                'ticket': posicion.ticket,
                'type': posicion.type
            }
            lista_posiciones.append(posicion_diccionario)
        print(f"Lista de posiciones abiertas procesada: {lista_posiciones}")
        return lista_posiciones

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
                symbol = posicion.get('symbol')
                position_id = posicion.get('ticket')
                tendencia_actual = "Tendencia actual placeholder"
                reverso_tendencia = "Reversión placeholder"
                signal = "Señal placeholder"

                if self.close_conditions.verificar_cierre_por_condiciones(symbol, tendencia_actual, reverso_tendencia, signal):
                    print(f"Condiciones de cierre válidas para {symbol}, procediendo a cerrar la posición.")
                    self.cerrar_posicion(symbol, position_id)
                else:
                    print(f"Condiciones de cierre no válidas o incompletas para {symbol}, no se cierra la posición.")

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
        Implementa la lógica para calcular el ATR del símbolo.
        """
        return 0.0015  # Valor de ejemplo
