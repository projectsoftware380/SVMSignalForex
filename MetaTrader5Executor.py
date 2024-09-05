import MetaTrader5 as mt5
import threading
from TradeCloseConditions import TradeCloseConditions  # Importamos la clase TradeCloseConditions

class MetaTrader5Executor:
    def __init__(self):
        self.conectado = False
        self.operaciones_abiertas = {}  # Guardar las operaciones activas para monitoreo
        self.trade_close_conditions = TradeCloseConditions()  # Instancia de la clase TradeCloseConditions

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
        return result.order

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
                
                # Verificar las condiciones de cierre usando la clase TradeCloseConditions
                tendencia_actual = self.obtener_tendencia_actual(symbol)  # Esta función debe obtener la tendencia actual
                reverso_tendencia = self.obtener_reverso_tendencia(symbol)  # Función para obtener reversión
                signal = self.obtener_signal(symbol)  # Función para obtener señal de trading

                if self.trade_close_conditions.verificar_cierre_por_condiciones(symbol, tendencia_actual, reverso_tendencia, signal):
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

    # Placeholder para obtener la tendencia actual
    def obtener_tendencia_actual(self, symbol):
        # Implementar la lógica para obtener la tendencia actual
        return "Neutral"  # Placeholder

    # Placeholder para obtener la reversión de tendencia
    def obtener_reverso_tendencia(self, symbol):
        # Implementar la lógica para obtener la reversión de tendencia
        return "Reversión Bajista"  # Placeholder

    # Placeholder para obtener la señal de trading
    def obtener_signal(self, symbol):
        # Implementar la lógica para obtener la señal de trading
        return "Señal de Venta Detectada"  # Placeholder
