import pandas_ta as ta

class ForexAnalyzer:
    def __init__(self, data_fetcher, api_key_polygon, pairs):
        self.data_fetcher = data_fetcher
        self.api_key_polygon = api_key_polygon
        self.pairs = pairs  # Lista de pares de divisas para analizar
        self.last_trend = {}  # Almacena solo las tendencias alcistas o bajistas de cada par

    def verificar_estado_mercado(self):
        """
        Verifica si el mercado Forex está abierto utilizando la API de Polygon.io.
        """
        return self.data_fetcher.obtener_estado_mercado()

    def calcular_sma(self, series, length):
        """
        Calcula la media móvil simple (SMA) para una serie de datos.
        """
        if len(series) >= length:
            return ta.sma(series, length=length)
        else:
            print(f"Advertencia: No hay suficientes datos para calcular la SMA de longitud {length}")
            return None

    def determinar_tendencia_tecnica(self, df, pair):
        """
        Determina la tendencia técnica de un par de divisas utilizando indicadores técnicos de Ichimoku.
        """
        if df.empty:
            return 0  # No hay datos suficientes para determinar tendencia

        # Calcular los componentes de Ichimoku
        tenkan_sen = self.calcular_sma(df['High'], 9)
        if tenkan_sen is None:
            return 0  # No se puede calcular sin suficientes datos

        kijun_sen = self.calcular_sma(df['High'], 26)
        if kijun_sen is None:
            return 0

        senkou_span_a = ((tenkan_sen + kijun_sen) / 2).shift(26)
        senkou_span_b = ((self.calcular_sma(df['High'], 52) + self.calcular_sma(df['Low'], 52)) / 2).shift(26)

        # Verificar si Senkou Span A o B son None
        if senkou_span_a is None or senkou_span_b is None:
            print(f"Error: No se pudo calcular Senkou Span A o B para {pair}")
            return 0

        # Obtener el precio de cierre más reciente
        precio_cierre = self.data_fetcher.obtener_precio_cierre_mas_reciente(pair)

        # Obtener los valores más recientes de Senkou Span A y B
        valor_senkou_span_a = senkou_span_a.iloc[-1]
        valor_senkou_span_b = senkou_span_b.iloc[-1]

        # Imprimir los valores para verificar
        print(f"\nPar de Divisas: {pair}")
        print(f"Precio de Cierre: {precio_cierre}")
        print(f"Senkou Span A: {valor_senkou_span_a}")
        print(f"Senkou Span B: {valor_senkou_span_b}")

        # Verificar el orden de Senkou Span A y Senkou Span B
        is_kumo_alcista = valor_senkou_span_a > valor_senkou_span_b
        is_kumo_bajista = valor_senkou_span_b > valor_senkou_span_a

        # Condición para tendencia alcista
        if precio_cierre > valor_senkou_span_a and precio_cierre > valor_senkou_span_b and is_kumo_alcista:
            print("Tendencia Alcista detectada")
            return 1  # Tendencia Alcista

        # Condición para tendencia bajista
        elif precio_cierre < valor_senkou_span_a and precio_cierre < valor_senkou_span_b and is_kumo_bajista:
            print("Tendencia Bajista detectada")
            return 2  # Tendencia Bajista

        # Si el precio está dentro del Kumo
        elif (valor_senkou_span_a > precio_cierre > valor_senkou_span_b) or (valor_senkou_span_b > precio_cierre > valor_senkou_span_a):
            print("El precio está dentro del Kumo (zona de indecisión)")
            return 3  # Precio dentro del Kumo (indecisión)

        # No hay tendencia clara
        print("No hay tendencia clara")
        return 0  # Neutral

    def analizar_par(self, pair):
        """
        Analiza el par de divisas para determinar la tendencia técnica.
        """
        if not self.verificar_estado_mercado():
            return "Neutral"  # Si el mercado está cerrado, devolver 'Neutral'

        symbol_polygon = pair.replace("-", "")
        df = self.data_fetcher.obtener_datos(symbol_polygon, 'hour', '1', 60)
        if df.empty:
            return "Neutral"

        tendencia_tecnica = self.determinar_tendencia_tecnica(df, pair)

        if tendencia_tecnica == 1:
            tendencia_final = "Tendencia Alcista"
        elif tendencia_tecnica == 2:
            tendencia_final = "Tendencia Bajista"
        else:
            tendencia_final = "Neutral"

        if tendencia_final in ["Tendencia Alcista", "Tendencia Bajista"]:
            self.last_trend[pair] = tendencia_final

        return tendencia_final

    def imprimir_diccionario_resultados(self):
        """
        Imprime el diccionario con los resultados de la tendencia final de cada par.
        """
        if not self.last_trend:
            print("No se detectaron tendencias para ningún par.")
        else:
            for pair, tendencia in self.last_trend.items():
                print(f"Tendencia para {pair}: {tendencia}")
