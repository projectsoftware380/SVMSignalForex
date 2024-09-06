class TradeCloseConditions:
    def __init__(self, mt5_executor):
        self.mt5_executor = mt5_executor

    def verificar_cierre_por_condiciones(self, symbol, tendencia_actual, reversion=None, signal=None):
        """
        Verifica si alguna de las condiciones de cierre se cumple:
        - Tendencia contraria
        - Reversión con señal contraria
        """
        if self.verificar_tendencia_contraria(symbol, tendencia_actual) or self.verificar_reversion(symbol, reversion, signal):
            return True
        print(f"Las condiciones de cierre no se cumplen para {symbol}.")
        return False

    def verificar_tendencia_contraria(self, symbol, tendencia_actual):
        """
        Verifica si la tendencia actual es contraria a la posición abierta o es neutral.
        Si hay una tendencia bajista o neutral para una posición de compra, o una tendencia alcista o neutral
        para una posición de venta, se evalúa cerrar la operación.
        """
        posicion = self.mt5_executor.operaciones_abiertas.get(symbol)
        if not posicion:
            return False  # No hay operación abierta para este símbolo

        tipo_operacion = posicion['tipo']
        if tipo_operacion == 'compra' and (tendencia_actual == "Tendencia Bajista" or tendencia_actual == "Neutral"):
            print(f"Tendencia bajista o neutral detectada para {symbol}, operación de compra. Evaluando cierre...")
            return True
        elif tipo_operacion == 'venta' and (tendencia_actual == "Tendencia Alcista" or tendencia_actual == "Neutral"):
            print(f"Tendencia alcista o neutral detectada para {symbol}, operación de venta. Evaluando cierre...")
            return True
        return False

    def verificar_reversion(self, symbol, reversion, signal):
        """
        Verifica si hay una reversión con una señal contraria a la operación abierta.
        Si se detecta una reversión bajista con una señal de venta para una posición de compra,
        o una reversión alcista con una señal de compra para una posición de venta, se evalúa cerrar la operación.
        """
        if reversion is None or signal is None:
            return False  # No hay reversión o señal que evaluar

        posicion = self.mt5_executor.operaciones_abiertas.get(symbol)
        if not posicion:
            return False  # No hay operación abierta para este símbolo

        tipo_operacion = posicion['tipo']
        if tipo_operacion == 'compra' and "Reversión Bajista" in reversion and "Señal de Venta" in signal:
            print(f"Reversión bajista con señal de venta detectada para {symbol}. Evaluando cierre...")
            return True
        elif tipo_operacion == 'venta' and "Reversión Alcista" in reversion and "Señal de Compra" in signal:
            print(f"Reversión alcista con señal de compra detectada para {symbol}. Evaluando cierre...")
            return True
        return False
