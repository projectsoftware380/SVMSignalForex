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
loop_interval = config.get('loop_interval', 60)  # Si no está en el JSON, asignar 60
tendencia_interval = config.get('tendencia_interval', 300)  # Valor por defecto de 300 segundos
cierre_interval = config.get('cierre_interval', 180)  # Valor por defecto de 180 segundos

# Crear una instancia de DataFetcher
data_fetcher = DataFetcher(config['api_key_polygon'])

# Instanciar las clases necesarias
mt5_executor = MetaTrader5Executor()  # Crear instancia del ejecutor de MT5
forex_analyzer = ForexAnalyzer(data_fetcher, config['api_token_forexnews'], config['api_key_polygon'])
forex_reversal_analyzer = ForexReversalAnalyzer(data_fetcher, mt5_executor, config['api_key_polygon'])  # Pasar el API key
forex_signal_analyzer = ForexSignalAnalyzer(data_fetcher, mt5_executor, config['api_key_polygon'])  # Pasar el API key

# Conectar MetaTrader 5
if not mt5_executor.conectar_mt5():
    print("Error al conectar con MetaTrader 5")
    exit()

# Función para verificar si el mercado está abierto
def mercado_abierto():
    try:
        return data_fetcher.obtener_estado_mercado()  # Verificar si el mercado está abierto usando DataFetcher
    except Exception as e:
        print(f"Error al verificar el estado del mercado: {str(e)}")
        return False

# Función para evaluar la tendencia principal
def evaluar_tendencias():
    while True:
        if not mercado_abierto():
            print("Mercado cerrado, esperando para reevaluar.")
            time.sleep(loop_interval)
            continue

        try:
            pares_tendencia = {}
            for pair in config['pairs']:
                resultado = forex_analyzer.analizar_par(pair)
                print(f"Tendencia para {pair}: {resultado}")
                if "Tendencia" in resultado:
                    pares_tendencia[pair] = resultado
            
            # Monitorear las reversiones y generar señales
            evaluar_reversiones(pares_tendencia)

        except Exception as e:
            print(f"Error durante la evaluación de tendencias: {str(e)}")
        time.sleep(tendencia_interval)  # Esperar el intervalo definido antes de la siguiente evaluación

# Función para evaluar las reversiones y generar señales
def evaluar_reversiones(pares_tendencia):
    try:
        pares_reversion = forex_reversal_analyzer.analizar_reversiones(pares_tendencia)
        
        # Generar señales en los pares con reversiones detectadas
        for pair, reversion in pares_reversion.items():
            if isinstance(reversion, dict) and "Reversión" in reversion:
                resultado_senal = forex_signal_analyzer.analizar_senales({pair: reversion})
                print(f"Señal para {pair}: {resultado_senal[pair]}")
            else:
                print(f"Formato inesperado de datos en {pair}: {reversion}")
    except Exception as e:
        print(f"Error durante la evaluación de reversiones: {str(e)}")

# Función paralela para cerrar posiciones si se detecta cambio de tendencia o reversión contraria
def monitorear_cierres():
    while True:
        if not mercado_abierto():
            print("Mercado cerrado, esperando para monitorear cierres.")
            time.sleep(loop_interval)
            continue

        try:
            posiciones_abiertas = mt5_executor.obtener_posiciones_abiertas()  # Método nuevo en MetaTrader5Executor
            for posicion in posiciones_abiertas:
                symbol = posicion['symbol']
                tipo_operacion = posicion['type']
                print(f"Monitoreando posición abierta en {symbol}")

                # Revisar si la tendencia ha cambiado o si hay una reversión contraria
                nueva_tendencia = forex_analyzer.analizar_par(symbol.replace("_", "-"))
                if ("Alcista" in nueva_tendencia and tipo_operacion == mt5.ORDER_TYPE_SELL) or \
                   ("Bajista" in nueva_tendencia and tipo_operacion == mt5.ORDER_TYPE_BUY) or \
                   ("Neutral" in nueva_tendencia):
                    print(f"Cambio de tendencia en {symbol}, cerrando posición.")
                    mt5_executor.cerrar_posicion(symbol, posicion['ticket'])
                else:
                    # Monitorear para una señal contraria
                    reverso_tendencia = forex_reversal_analyzer.analizar_reversiones({symbol: nueva_tendencia})
                    if reverso_tendencia:
                        print(f"Señal contraria detectada en {symbol}, cerrando posición.")
                        mt5_executor.cerrar_posicion(symbol, posicion['ticket'])

        except Exception as e:
            print(f"Error durante el monitoreo de cierres: {str(e)}")
        time.sleep(cierre_interval)  # Intervalo de espera antes de la siguiente evaluación de cierres

# Iniciar hilos paralelos
def iniciar_hilos():
    hilo_tendencias = threading.Thread(target=evaluar_tendencias)
    hilo_cierres = threading.Thread(target=monitorear_cierres)

    hilo_tendencias.start()
    hilo_cierres.start()

    # Mantener los hilos en ejecución
    hilo_tendencias.join()
    hilo_cierres.join()

# Iniciar el proceso
if __name__ == "__main__":
    try:
        iniciar_hilos()
    except KeyboardInterrupt:
        print("Proceso interrumpido manualmente.")
    finally:
        # Cerrar la conexión con MetaTrader 5 al terminar
        mt5_executor.cerrar_conexion()
