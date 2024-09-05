import time
import threading
from ForexAnalyzer import ForexAnalyzer
from ForexReversalAnalyzer import ForexReversalAnalyzer
from ForexSignalAnalyzer import ForexSignalAnalyzer
from MetaTrader5Executor import MetaTrader5Executor
from TradeCloseConditions import TradeCloseConditions
from DataFetcher import DataFetcher
import json

# Banderas para controlar las impresiones
imprimir_tendencias = False
imprimir_reversiones = True
imprimir_senales = True
imprimir_cierres = False  # No imprimir en monitorear_cierres

def main():
    # Cargar configuración desde un archivo JSON
    with open("config.json") as config_file:
        config = json.load(config_file)

    # Crear una instancia de DataFetcher
    data_fetcher = DataFetcher(config['api_key_polygon'])

    # Instanciar MetaTrader5Executor
    mt5_executor = MetaTrader5Executor(None)

    # Instanciar TradeCloseConditions con mt5_executor
    close_conditions = TradeCloseConditions(mt5_executor)
    mt5_executor.close_conditions = close_conditions

    # Instanciar las demás clases
    forex_analyzer = ForexAnalyzer(data_fetcher, config['api_token_forexnews'], config['api_key_polygon'])
    forex_reversal_analyzer = ForexReversalAnalyzer(data_fetcher, mt5_executor, config['api_key_polygon'])
    forex_signal_analyzer = ForexSignalAnalyzer(data_fetcher, mt5_executor, config['api_key_polygon'])

    # Conectar MetaTrader 5
    if not mt5_executor.conectar_mt5():
        print("Error al conectar con MetaTrader 5")
        return

    # Definir funciones de operaciones
    def evaluar_tendencias():
        while True:
            try:
                # Verificar si el mercado está abierto
                if not data_fetcher.obtener_estado_mercado():
                    time.sleep(config.get('loop_interval', 60))
                    continue

                # Analizar tendencias
                pares_tendencia = {pair: forex_analyzer.analizar_par(pair) for pair in config['pairs']}
                if imprimir_tendencias:
                    for pair, tendencia in pares_tendencia.items():
                        print(f"Tendencia para {pair}: {tendencia}")
                evaluar_reversiones(pares_tendencia)
            except Exception as e:
                if imprimir_tendencias:
                    print(f"Error durante la evaluación de tendencias: {str(e)}")
            time.sleep(config.get('tendencia_interval', 300))

    def evaluar_reversiones(pares_tendencia):
        try:
            # Analizar reversiones
            pares_reversion = forex_reversal_analyzer.analizar_reversiones(pares_tendencia)
            if imprimir_reversiones:
                for pair, reversion in pares_reversion.items():
                    print(f"Reversión detectada para {pair}: {reversion}")
            # Analizar señales para las reversiones detectadas
            forex_signal_analyzer.analizar_senales(pares_reversion, imprimir_senales)
        except Exception as e:
            if imprimir_reversiones:
                print(f"Error durante la evaluación de reversiones: {str(e)}")

    def monitorear_cierres():
        while True:
            try:
                # Verificar si el mercado está abierto
                if not data_fetcher.obtener_estado_mercado():
                    time.sleep(config.get('loop_interval', 60))
                    continue

                # Monitorear las posiciones abiertas para verificar si deben cerrarse
                for posicion in mt5_executor.obtener_posiciones_abiertas():
                    symbol = posicion['symbol']
                    nueva_tendencia = forex_analyzer.analizar_par(symbol)
                    if close_conditions.verificar_cierre_por_condiciones(symbol, nueva_tendencia):
                        if imprimir_cierres:
                            print(f"Cerrando posición para {symbol}")
                        mt5_executor.cerrar_posicion(symbol, posicion['ticket'])
            except Exception as e:
                if imprimir_cierres:
                    print(f"Error durante el monitoreo de cierres: {str(e)}")
            time.sleep(config.get('cierre_interval', 180))

    # Iniciar hilos paralelos
    hilo_tendencias = threading.Thread(target=evaluar_tendencias)
    hilo_cierres = threading.Thread(target=monitorear_cierres)
    hilo_tendencias.start()
    hilo_cierres.start()
    hilo_tendencias.join()
    hilo_cierres.join()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Proceso interrumpido manualmente.")
    finally:
        mt5_executor.cerrar_conexion()
