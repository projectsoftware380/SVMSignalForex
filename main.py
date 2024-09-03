import time
from ForexAnalyzer import ForexAnalyzer
from ForexReversalAnalyzer import ForexReversalAnalyzer
from ForexSignalAnalyzer import ForexSignalAnalyzer
from MetaTrader5Executor import MetaTrader5Executor
from DataFetcher import DataFetcher
import json

# Cargar configuración desde un archivo JSON
with open("config.json") as config_file:
    config = json.load(config_file)

# Crear una instancia de DataFetcher
data_fetcher = DataFetcher(config['api_key_polygon'])

# Instanciar las clases necesarias
forex_analyzer = ForexAnalyzer(data_fetcher, config['api_token_forexnews'])
forex_reversal_analyzer = ForexReversalAnalyzer(data_fetcher)
forex_signal_analyzer = ForexSignalAnalyzer(data_fetcher)
mt5_executor = MetaTrader5Executor()

# Conectar MetaTrader 5
if not mt5_executor.conectar_mt5():
    print("Error al conectar con MetaTrader 5")
    exit()

# Función para evaluar y operar en los pares seleccionados
def operar_pares():
    try:
        # 1. Evaluar la tendencia principal (cada 4 horas)
        pares_tendencia = {}
        for pair in config['pairs']:
            resultado = forex_analyzer.analizar_par(pair)
            print(f"Tendencia para {pair}: {resultado}")
            if "Tendencia Alcista" in resultado or "Tendencia Bajista" in resultado:
                pares_tendencia[pair] = resultado
        
        # 2. Monitorear reversiones solo en pares con tendencia clara (cada 15 minutos)
        pares_reversion = forex_reversal_analyzer.analizar_reversiones(pares_tendencia)
        for pair, resultado_reversion in pares_reversion.items():
            print(f"Reversión para {pair}: {resultado_reversion}")
        
        # 3. Generar señales solo en pares con reversión detectada (cada 3 minutos)
        for pair, reversion in pares_reversion.items():
            if "Reversión" in reversion:
                resultado_senal = forex_signal_analyzer.analizar_senales({pair: reversion})
                print(f"Señal para {pair}: {resultado_senal[pair]}")
                
                # Ejecutar órdenes si hay señal detectada
                if "Señal de Compra Detectada" in resultado_senal[pair]:
                    mt5_executor.ejecutar_orden(pair.replace("-", ""), "buy")
                elif "Señal de Venta Detectada" in resultado_senal[pair]:
                    mt5_executor.ejecutar_orden(pair.replace("-", ""), "sell")
        
        # 4. Monitorear cierres por cambio de tendencia
        for pair in pares_tendencia:
            nueva_tendencia = forex_analyzer.analizar_par(pair)
            if ("Tendencia Alcista" in pares_tendencia[pair] and "Tendencia Bajista" in nueva_tendencia) or \
            ("Tendencia Bajista" in pares_tendencia[pair] and "Tendencia Alcista" in nueva_tendencia) or \
            ("Neutral" in nueva_tendencia):
                print(f"Cambio de tendencia detectado para {pair}. Cerrando posiciones.")
                mt5_executor.cerrar_posicion(pair.replace("-", ""))
    except Exception as e:
        print(f"Error durante la operación de pares: {str(e)}")

# Bucle principal en tiempo real
while True:
    try:
        operar_pares()
        time.sleep(config['loop_interval'])
    except KeyboardInterrupt:
        print("Proceso interrumpido manualmente.")
        break

# Cerrar conexión a MetaTrader 5 al terminar
mt5_executor.cerrar_conexion()
