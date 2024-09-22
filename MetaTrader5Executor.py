import MetaTrader5 as mt5
import time
import json

class MetaTrader5Executor:
    def __init__(self, max_loss_level, profit_trailing_level, trailing_stop_percentage):
        self.equity_inicial = None
        self.max_profit = 0
        self.max_loss_level = max_loss_level
        self.profit_trailing_level = profit_trailing_level
        self.trailing_stop_percentage = trailing_stop_percentage
        self.conectado = False
        self.operaciones_abiertas = {}  # Inicializar operaciones_abiertas como un diccionario vacío

        # Conectar a MetaTrader 5
        if not self.conectar_mt5():
            raise ConnectionError("No se pudo conectar con MetaTrader 5.")

        # Obtener equidad inicial de la cuenta
        account_info = mt5.account_info()
        if account_info is None:
            raise ValueError("No se pudo obtener la información de la cuenta.")
        self.equity_inicial = account_info.equity

    def conectar_mt5(self):
        """Conecta MetaTrader 5."""
        if not mt5.initialize():
            return False
        self.conectado = True
        return True

    def obtener_profit_abierto(self):
        """Obtiene el profit total de las posiciones abiertas."""
        posiciones = mt5.positions_get()
        if posiciones is None or len(posiciones) == 0:
            return 0.0

        total_profit = sum(pos.profit for pos in posiciones)
        return total_profit

    def monitorear_equidad_global(self, monitoreo_intervalo=60):
        """Monitorea las pérdidas y ganancias globales de la cuenta basadas en la equidad y el profit cada cierto intervalo de tiempo."""
        while True:
            # Obtener información de la cuenta
            account_info = mt5.account_info()
            if account_info is None:
                return

            equity_actual = account_info.equity
            profit_abierto = self.obtener_profit_abierto()  # Obtener profit actual
            ganancia_neta = equity_actual - self.equity_inicial + profit_abierto

            # Verificar nivel de pérdida máxima
            if ganancia_neta <= self.max_loss_level:
                self.cerrar_todas_las_operaciones()
                mt5.shutdown()
                break

            # Verificar trailing stop de ganancias
            if ganancia_neta >= self.profit_trailing_level:
                self.max_profit = max(self.max_profit, ganancia_neta)
                trailing_stop_value = self.max_profit * (1 - self.trailing_stop_percentage / 100)

                if ganancia_neta <= trailing_stop_value:
                    self.cerrar_todas_las_operaciones()
                    mt5.shutdown()
                    break

            # Esperar el intervalo de monitoreo antes de volver a revisar
            time.sleep(monitoreo_intervalo)

    def cerrar_todas_las_operaciones(self):
        """Cierra todas las operaciones abiertas."""
        posiciones = self.obtener_posiciones_abiertas()
        for posicion in posiciones:
            self.cerrar_posicion(posicion['symbol'], posicion['ticket'])

    def obtener_posiciones_abiertas(self):
        """Devuelve una lista de posiciones abiertas en formato de diccionario."""
        posiciones = mt5.positions_get()
        if posiciones is None:
            return []

        return [{'symbol': pos.symbol, 'ticket': pos.ticket, 'type': pos.type, 'price_open': pos.price_open} for pos in posiciones]

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
        if result.retcode == mt5.TRADE_RETCODE_DONE:
            # Eliminar del diccionario solo si está presente
            if symbol in self.operaciones_abiertas:
                del self.operaciones_abiertas[symbol]


# Leer el archivo config.json para obtener los parámetros de prueba
with open("config.json", "r") as file:
    config = json.load(file)

# Parámetros de prueba desde el archivo config.json
max_loss_level = config["max_loss_level"]
profit_trailing_level = config["profit_trailing_level"]
trailing_stop_percentage = config["trailing_stop_percentage"]
monitoreo_intervalo = config["monitoreo_intervalo"]

# Inicializar el ejecutor de MetaTrader 5 con los valores del archivo config.json
executor = MetaTrader5Executor(
    max_loss_level=max_loss_level,
    profit_trailing_level=profit_trailing_level,
    trailing_stop_percentage=trailing_stop_percentage
)

# Monitorear la equidad global con el intervalo de monitoreo ajustado
executor.monitorear_equidad_global(monitoreo_intervalo)
