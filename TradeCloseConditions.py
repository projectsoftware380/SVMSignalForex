class TradeCloseConditions:
    def __init__(self, mt5_executor):
        self.mt5_executor = mt5_executor

    def verificar_cierre_por_condiciones(self, symbol, tendencia_actual, reversion=None, signal=None):
        """
        Verifica si alguna de las condiciones de cierre se cumple:
        - Tendencia contraria
        - Reversión con señal contraria
        """
        print(f"Verificando condiciones de cierre para {symbol}. Tendencia actual: {tendencia_actual}.")
        
        if self.verificar_tendencia_contraria(symbol, tendencia_actual) or self.verificar_reversion(symbol, reversion, signal):
            print(f"Las condiciones de cierre se cumplen para {symbol}.")
            return True
        
        print(f"Las condiciones de cierre no se cumplen para {symbol}.")
        return False

    def verificar_tendencia_contraria(self, symbol, tendencia_actual):
        """
        Verifica si la tendencia actual es contraria a la posición abierta o es neutral.
        """
        posicion = self.mt5_executor.operaciones_abiertas.get(symbol)
        if not posicion:
            print(f"No hay operación abierta para {symbol}.")
            return False  # No hay operación abierta para este símbolo

        tipo_operacion = posicion['tipo']
        print(f"Tipo de operación para {symbol}: {tipo_operacion}.")
        
        if tipo_operacion == 'compra' and (tendencia_actual == "Tendencia Bajista" or tendencia_actual == "Neutral"):
            print(f"Tendencia bajista o neutral detectada para {symbol}, operación de compra. Evaluando cierre...")
            return True
        elif tipo_operacion == 'venta' and (tendencia_actual == "Tendencia Alcista" or tendencia_actual == "Neutral"):
            print(f"Tendencia alcista o neutral detectada para {symbol}, operación de venta. Evaluando cierre...")
            return True
        
        print(f"No hay tendencia contraria para {symbol}.")
        return False

    def verificar_reversion(self, symbol, reversion, signal):
        """
        Verifica si hay una reversión con una señal contraria a la operación abierta.
        """
        if reversion is None or signal is None:
            print(f"No hay reversión o señal que evaluar para {symbol}.")
            return False  # No hay reversión o señal que evaluar

        posicion = self.mt5_executor.operaciones_abiertas.get(symbol)
        if not posicion:
            print(f"No hay operación abierta para {symbol}.")
            return False  # No hay operación abierta para este símbolo

        tipo_operacion = posicion['tipo']
        print(f"Tipo de operación para {symbol}: {tipo_operacion}. Reversión: {reversion}, Señal: {signal}.")
        
        if tipo_operacion == 'compra' and "Reversión Bajista" in reversion and "Señal de Venta" in signal:
            print(f"Reversión bajista con señal de venta detectada para {symbol}. Evaluando cierre...")
            return True
        elif tipo_operacion == 'venta' and "Reversión Alcista" in reversion and "Señal de Compra" in signal:
            print(f"Reversión alcista con señal de compra detectada para {symbol}. Evaluando cierre...")
            return True
        
        print(f"No hay reversión contraria para {symbol}.")
        return False
