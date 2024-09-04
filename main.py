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
mt5_executor = MetaTrader5Executor()  # Crear instancia del ejecutor de MT5
forex_analyzer = ForexAnalyzer(data_fetcher, config['api_token_forexnews'])
forex_reversal_analyzer = ForexReversalAnalyzer(data_fetcher)
forex_signal_analyzer = ForexSignalAnalyzer(data_fetcher, mt5_executor)  # Pasar el ejecutor de MT5

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
            if "Tendencia" in resultado:
                pares_tendencia[pair] = resultado
        
        # 2. Monitorear reversiones solo en pares con tendencia clara (cada 15 minutos)
        pares_reversion = forex_reversal_analyzer.analizar_reversiones(pares_tendencia)
        
        # 3. Generar señales solo en pares con reversión detectada (cada 3 minutos)
        for pair, reversion in pares_reversion.items():
            if "Reversión" in reversion:
                resultado_senal = forex_signal_analyzer.analizar_senales({pair: reversion})
                print(f"Señal para {pair}: {resultado_senal[pair]}")
        
        # 4. Monitorear cierres por cambio de tendencia
        for pair in pares_tendencia:
            nueva_tendencia = forex_analyzer.analizar_par(pair)
            if nueva_tendencia != pares_tendencia[pair]:
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
    except Exception as e:
        print(f"Error inesperado: {e}")
        continue  # Opcional: decide si deseas continuar automáticamente o no

# Cerrar conexión a MetaTrader 5 al terminar
mt5_executor.cerrar_conexion()
