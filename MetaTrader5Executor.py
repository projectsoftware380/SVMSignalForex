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
        print("Conectado a MetaTrader 5")
        return True

    def ejecutar_orden(self, symbol, order_type):
        if not self.conectado:
            print("No conectado a MetaTrader 5")
            return
        
        # Preparar el símbolo
        symbol_info = mt5.symbol_info(symbol)
        if symbol_info is None:
            print(f"{symbol} no encontrado, no se puede enviar la orden")
            return
        
        if not symbol_info.visible:
            print(f"{symbol} no es visible, intentando activarlo")
            if not mt5.symbol_select(symbol, True):
                print(f"Error al seleccionar el símbolo {symbol}")
                return
        
        # Preparar los detalles de la orden
        lot = 0.1  # Tamaño del lote
        point = symbol_info.point
        price = mt5.symbol_info_tick(symbol).ask if order_type == "buy" else mt5.symbol_info_tick(symbol).bid
        deviation = 20

        # Configuración para deshabilitar SL y TP
        sl = price - 100 * point if order_type == "buy" else price + 100 * point
        tp = price + 100 * point if order_type == "buy" else price - 100 * point
        
        order_type_mt5 = mt5.ORDER_TYPE_BUY if order_type == "buy" else mt5.ORDER_TYPE_SELL
        
        request = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": symbol,
            "volume": lot,
            "type": order_type_mt5,
            "price": price,
            "sl": sl,  # Stop Loss
            "tp": tp,  # Take Profit
            "deviation": deviation,
            "magic": 234000,
            "comment": "Orden automática",
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": mt5.ORDER_FILLING_FOK,  # Cambiado a ORDER_FILLING_FOK
        }
        
        # Ejecutar la orden
        result = mt5.order_send(request)
        
        if result is None:
            print("Error: mt5.order_send() devolvió None. Verifica la conexión y los parámetros de la orden.")
        else:
            print(f"1. order_send(): {order_type} {symbol} {lot} lotes a {price} con desviación={deviation} puntos")
            if result.retcode != mt5.TRADE_RETCODE_DONE:
                print(f"2. order_send falló, retcode={result.retcode}")
                # Mostrar detalles de la respuesta
                result_dict = result._asdict()
                for field in result_dict.keys():
                    print(f"   {field}={result_dict[field]}")
                    if field == "request":
                        traderequest_dict = result_dict[field]._asdict()
                        for tradereq_field in traderequest_dict:
                            print(f"       traderequest: {tradereq_field}={traderequest_dict[tradereq_field]}")
            else:
                print(f"Orden ejecutada con éxito: {symbol} {order_type}")
                return result.order  # Devuelve el ID de la orden

    def cerrar_posicion(self, symbol, position_id):
        if not self.conectado:
            print("No conectado a MetaTrader 5")
            return
        
        # Cierra la posición abierta basada en el ticket
        price = mt5.symbol_info_tick(symbol).bid
        request = {
            "action": mt5.TRADE_ACTION_DEAL,
            "position": position_id,
            "symbol": symbol,
            "volume": 0.1,  # Mantén el mismo tamaño de lote
            "type": mt5.ORDER_TYPE_SELL,
            "price": price,
            "deviation": 20,
            "magic": 234000,
            "comment": "Cierre automático",
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": mt5.ORDER_FILLING_FOK,  # Cambiado a ORDER_FILLING_FOK
        }
        result = mt5.order_send(request)
        if result is None:
            print(f"Error: mt5.order_send() devolvió None al intentar cerrar la posición {symbol}")
        else:
            print(f"3. close position: sell {symbol} 0.1 lotes a {price} con desviación={20} puntos")
            if result.retcode != mt5.TRADE_RETCODE_DONE:
                print(f"4. order_send falló, retcode={result.retcode}")
            else:
                print(f"Posición cerrada con éxito: {symbol}")

    def cerrar_conexion(self):
        mt5.shutdown()
        self.conectado = False
        print("Conexión cerrada con MetaTrader 5")

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