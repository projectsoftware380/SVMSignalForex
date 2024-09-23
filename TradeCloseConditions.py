class TradeCloseConditions:
    def __init__(self, mt5_executor):
        self.mt5_executor = mt5_executor

    def normalizar_par(self, pair):
        return pair.replace("-", "").replace(".", "").upper()

    def verificar_cierre_por_condiciones(self, symbol, tendencia_actual):
        """
        Verifica si alguna de las condiciones de cierre se cumple:
        - Tendencia contraria o neutral
        """
        print(f"Verificando condiciones de cierre para {symbol}. Tendencia actual: {tendencia_actual if tendencia_actual else 'No determinada'}.")

        # Comprobamos si hay una operación abierta para este símbolo
        operacion_abierta = self.obtener_operacion_abierta(symbol)
        if operacion_abierta:
            print(f"Operación abierta encontrada para {symbol}: {operacion_abierta}")

            if tendencia_actual is None:
                print(f"Tendencia para {symbol} aún no determinada. No se tomará acción.")
                return False

            # Verificamos si la tendencia actual es contraria a la operación abierta
            if self.verificar_tendencia_contraria(tendencia_actual, operacion_abierta['type']):
                print(f"Las condiciones de cierre por tendencia contraria se cumplen para {symbol}. Cerrando operación...")
                self.mt5_executor.cerrar_operacion(operacion_abierta['ticket'])  # Cierra la operación
                return True
            else:
                print(f"No se cumplen condiciones de cierre por tendencia contraria para {symbol}.")
        else:
            print(f"No hay operación abierta para {symbol}.")
        return False

    def verificar_tendencia_contraria(self, tendencia_actual, tipo_operacion):
        """
        Verifica si la tendencia actual es contraria a la posición abierta o es neutral.
        """
        print(f"Verificando tendencia para una operación de tipo: {tipo_operacion}")

        # Verificamos si la operación es de compra y la tendencia es bajista o neutral
        if tipo_operacion == 'compra' and (tendencia_actual == "Tendencia Bajista" or tendencia_actual == "Neutral"):
            print(f"Tendencia bajista o neutral detectada para una compra. Se recomienda cerrar la operación.")
            return True
        # Verificamos si la operación es de venta y la tendencia es alcista o neutral
        elif tipo_operacion == 'venta' and (tendencia_actual == "Tendencia Alcista" or tendencia_actual == "Neutral"):
            print(f"Tendencia alcista o neutral detectada para una venta. Se recomienda cerrar la operación.")
            return True

        # Si no se detecta una tendencia contraria, no se cierra la operación
        print(f"Tendencia actual no es contraria. No se cierra la operación.")
        return False

    def obtener_operacion_abierta(self, symbol):
        """
        Devuelve la operación abierta para el símbolo dado si existe.
        """
        symbol_normalizado = self.normalizar_par(symbol)
        for operacion in self.mt5_executor.operaciones_abiertas.values():
            operacion_symbol_normalizado = self.normalizar_par(operacion['symbol'])
            print(f"Comparando {symbol_normalizado} con {operacion_symbol_normalizado}")
            if operacion_symbol_normalizado == symbol_normalizado:
                return operacion
        return None
