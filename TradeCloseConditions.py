class TradeCloseConditions:
    def __init__(self):
        self.config = None  # Configuración adicional (si se necesita más adelante)

    def verificar_tendencia_contraria(self, symbol, tendencia_actual):
        """
        Verifica si la tendencia actual del par es contraria a la posición abierta.
        """
        if tendencia_actual == "Neutral" or tendencia_actual == "Bajista":
            print(f"Tendencia contraria o neutral detectada para {symbol}. Evaluando cierre...")
            return True
        return False

    def verificar_reversion(self, symbol, reverso_tendencia, signal):
        """
        Verifica si una reversión acompañada de una señal contraria se ha generado.
        """
        if "Reversión Bajista" in reverso_tendencia and "Señal de Venta Detectada" in signal:
            print(f"Reversión bajista detectada con señal de venta para {symbol}. Evaluando cierre...")
            return True
        elif "Reversión Alcista" in reverso_tendencia and "Señal de Compra Detectada" in signal:
            print(f"Reversión alcista detectada con señal de compra para {symbol}. Evaluando cierre...")
            return True
        return False

    def verificar_cierre_por_condiciones(self, symbol, tendencia_actual, reverso_tendencia, signal):
        """
        Verifica si alguna de las condiciones de cierre se cumple:
        - Tendencia contraria
        - Reversión con señal contraria
        """
        if self.verificar_tendencia_contraria(symbol, tendencia_actual):
            return True
        
        if self.verificar_reversion(symbol, reverso_tendencia, signal):
            return True

        print(f"Las condiciones de cierre no se cumplen para {symbol}.")
        return False
