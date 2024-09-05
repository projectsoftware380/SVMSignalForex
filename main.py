import time
import threading
from ForexAnalyzer import ForexAnalyzer
from ForexReversalAnalyzer import ForexReversalAnalyzer
from ForexSignalAnalyzer import ForexSignalAnalyzer
from MetaTrader5Executor import MetaTrader5Executor
from DataFetcher import DataFetcher
import json

# Cargar configuración desde un archivo JSON
with open("config.json") as config_file:
    config = json.load(config_file)

# Obtener los valores con manejo de claves faltantes
loop_interval: int = config.get('loop_interval', 60)
tendencia_interval: int = config.get('tendencia_interval', 300)
cierre_interval: int = config.get('cierre_interval', 180)

# Crear una instancia de DataFetcher
data_fetcher: DataFetcher = DataFetcher(config['api_key_polygon'])

# Instanciar las clases necesarias
mt5_executor: MetaTrader5Executor = MetaTrader5Executor()
forex_analyzer: ForexAnalyzer = ForexAnalyzer(data_fetcher, config['api_token_forexnews'], config['api_key_polygon'])
forex_reversal_analyzer: ForexReversalAnalyzer = ForexReversalAnalyzer(data_fetcher, mt5_executor, config['api_key_polygon'])
forex_signal_analyzer: ForexSignalAnalyzer = ForexSignalAnalyzer(data_fetcher, mt5_executor, config['api_key_polygon'])

# Conectar MetaTrader 5
if not mt5_executor.conectar_mt5():
    print("Error al conectar con MetaTrader 5")
    exit()

# Función para verificar si el mercado está abierto
def mercado_abierto() -> bool:
    try:
        return data_fetcher.obtener_estado_mercado()
    except Exception as e:
        print(f"Error al verificar el estado del mercado: {str(e)}")
        return False

# Función para evaluar la tendencia principal
def evaluar_tendencias() -> None:
    while True:
        if not mercado_abierto():
            time.sleep(loop_interval)
            continue

        try:
            pares_tendencia: dict = {}
            for pair in config['pairs']:
                resultado: str = forex_analyzer.analizar_par(pair)
                if "Tendencia" in resultado:
                    pares_tendencia[pair] = resultado
            
            evaluar_reversiones(pares_tendencia)

        except Exception as e:
            print(f"Error durante la evaluación de tendencias: {str(e)}")
        time.sleep(tendencia_interval)

# Función para evaluar las reversiones y generar señales
def evaluar_reversiones(pares_tendencia: dict) -> None:
    try:
        pares_reversion: dict = forex_reversal_analyzer.analizar_reversiones(pares_tendencia)
        
        for pair, reversion in pares_reversion.items():
            if isinstance(reversion, str) and "Reversión" in reversion:
                resultado_senal: dict = forex_signal_analyzer.analizar_senales({pair: reversion})
            else:
                print(f"Formato inesperado de datos en {pair}: {reversion}")
    except Exception as e:
        print(f"Error durante la evaluación de reversiones: {str(e)}")

# Función paralela para cerrar posiciones si se detecta cambio de tendencia o reversión contraria
def monitorear_cierres() -> None:
    while True:
        if not mercado_abierto():
            time.sleep(loop_interval)
            continue

        try:
            posiciones_abiertas: list = mt5_executor.obtener_posiciones_abiertas()
            for posicion in posiciones_abiertas:
                symbol: str = posicion['symbol']
                tipo_operacion: int = posicion['type']

                nueva_tendencia: str = forex_analyzer.analizar_par(symbol.replace("_", "-"))
                if ("Alcista" in nueva_tendencia and tipo_operacion == mt5.ORDER_TYPE_SELL) or \
                   ("Bajista" in nueva_tendencia and tipo_operacion == mt5.ORDER_TYPE_BUY) or \
                   ("Neutral" in nueva_tendencia):
                    mt5_executor.cerrar_posicion(symbol, posicion['ticket'])
                else:
                    reverso_tendencia: dict = forex_reversal_analyzer.analizar_reversiones({symbol: nueva_tendencia})
                    if isinstance(reverso_tendencia, dict) and symbol in reverso_tendencia:
                        mt5_executor.cerrar_posicion(symbol, posicion['ticket'])

        except Exception as e:
            print(f"Error durante el monitoreo de cierres: {str(e)}")
        time.sleep(cierre_interval)

# Iniciar hilos paralelos
def iniciar_hilos() -> None:
    hilo_tendencias: threading.Thread = threading.Thread(target=evaluar_tendencias)
    hilo_cierres: threading.Thread = threading.Thread(target=monitorear_cierres)

    hilo_tendencias.start()
    hilo_cierres.start()

    hilo_tendencias.join()
    hilo_cierres.join()

# Iniciar el proceso
if __name__ == "__main__":
    try:
        iniciar_hilos()
    except KeyboardInterrupt:
        print("Proceso interrumpido manualmente.")
    finally:
        mt5_executor.cerrar_conexion()
