import time
import threading
import json
from ForexAnalyzer import ForexAnalyzer
from ForexReversalAnalyzer import ForexReversalAnalyzer
from ForexSignalAnalyzer import ForexSignalAnalyzer
from MetaTrader5Executor import MetaTrader5Executor
from TradeCloseConditions import TradeCloseConditions

# Banderas para controlar las impresiones
imprimir_tendencias = True
imprimir_reversiones = True
imprimir_senales = True
imprimir_cierres = False  # No imprimir en monitorear_cierres

# Variable de control global para detener todos los hilos
detener_hilos = False
hilos_lock = threading.Lock()  # Lock para proteger el acceso a detener_hilos

def normalizar_par(pair):
    return pair.replace("-", "")

def main():
    global detener_hilos

    # Cargar configuración desde un archivo JSON
    with open("config.json") as config_file:
        config = json.load(config_file)

    print("Configuración cargada correctamente")

    # Instanciar MetaTrader5Executor
    mt5_executor = MetaTrader5Executor(
        config['max_loss_level'],
        config['profit_trailing_level'],
        config['trailing_stop_percentage']
    )

    # Instanciar TradeCloseConditions con mt5_executor
    close_conditions = TradeCloseConditions(mt5_executor)
    mt5_executor.close_conditions = close_conditions

    # Instanciar ForexAnalyzer con la clave API y pares de divisas
    forex_analyzer = ForexAnalyzer(config['api_key_polygon'], config['pairs'])
    forex_reversal_analyzer = ForexReversalAnalyzer(mt5_executor, config['api_key_polygon'])
    forex_signal_analyzer = ForexSignalAnalyzer(mt5_executor, config['api_key_polygon'])

    if not mt5_executor.conectado:
        print("Error al conectar con MetaTrader 5")
        return
    else:
        print("Conexión a MetaTrader 5 exitosa")

    # Definir funciones de operaciones
    def evaluar_tendencias():
        print("Iniciando evaluación de tendencias...")
        while True:
            with hilos_lock:
                if detener_hilos:
                    break
            try:
                for pair in config['pairs']:
                    pair_normalizado = normalizar_par(pair)
                    forex_analyzer.analizar_par(pair_normalizado)

                if imprimir_tendencias:
                    with forex_analyzer.lock:
                        last_trend_copy = forex_analyzer.last_trend.copy()
                    if not last_trend_copy:
                        print("No se detectaron tendencias alcistas o bajistas.")
                    else:
                        print("\nTendencias detectadas:")
                        for pair, tendencia in last_trend_copy.items():
                            print(f"{pair}: {tendencia}")

                # Ajustar el tiempo de espera a 4 horas (14400 segundos)
                time.sleep(config.get('tendencia_interval', 14400))  # Intervalo ajustable a 4 horas
            except Exception as e:
                print(f"Error durante la evaluación de tendencias: {str(e)}")
                time.sleep(60)

    def evaluar_reversiones():
        print("Iniciando evaluación de reversiones...")
        while True:
            with hilos_lock:
                if detener_hilos:
                    break
            try:
                with forex_analyzer.lock:
                    tendencias = forex_analyzer.last_trend.copy()
                pares_reversion = forex_reversal_analyzer.analizar_reversiones(tendencias)
                if imprimir_reversiones and pares_reversion:
                    print(f"Pares válidos para analizar reversiones: {pares_reversion}")
                time.sleep(config.get('reversion_interval', 60))  # Intervalo ajustable
            except Exception as e:
                print(f"Error durante la evaluación de reversiones: {str(e)}")
                time.sleep(60)

    def evaluar_senales():
        print("Iniciando evaluación de señales...")
        while True:
            with hilos_lock:
                if detener_hilos:
                    break
            try:
                with forex_reversal_analyzer.lock:
                    resultados_reversiones = forex_reversal_analyzer.resultados.copy()
                if resultados_reversiones:
                    forex_signal_analyzer.analizar_senales(resultados_reversiones, imprimir_senales)
                else:
                    print("No hay reversiones para analizar señales.")
                time.sleep(config.get('senales_interval', 60))  # Intervalo ajustable
            except Exception as e:
                print(f"Error durante la evaluación de señales: {str(e)}")
                time.sleep(60)

    def monitorear_cierres():
        print("Iniciando monitoreo de cierres...")
        while True:
            with hilos_lock:
                if detener_hilos:
                    break
            try:
                posiciones = mt5_executor.obtener_posiciones_abiertas()
                print(f"Posiciones abiertas: {posiciones}")  # Verificación de posiciones abiertas
                for posicion in posiciones:
                    symbol = posicion['symbol']
                    symbol_normalizado = normalizar_par(symbol)
                    # Usar la tendencia almacenada en lugar de volver a analizar
                    with forex_analyzer.lock:
                        nueva_tendencia = forex_analyzer.last_trend.get(symbol_normalizado, None)
                    
                    if nueva_tendencia is None:
                        print(f"Tendencia aún no disponible para {symbol_normalizado}. No se realizará ninguna acción.")
                        continue
                    
                    if close_conditions.verificar_cierre_por_condiciones(symbol_normalizado, nueva_tendencia):
                        if imprimir_cierres:
                            print(f"Cerrando posición para {symbol_normalizado}")
                        resultado = mt5_executor.cerrar_operacion(posicion['ticket'])
                        
                        # Verificar si el cierre de la operación fue exitoso
                        if resultado is None:
                            print(f"Error al intentar cerrar la operación {posicion['ticket']}. No se recibió respuesta.")
                        elif resultado.retcode != mt5.TRADE_RETCODE_DONE:
                            print(f"Error al cerrar la operación {posicion['ticket']}. Código de error: {resultado.retcode}")
                        else:
                            print(f"Operación {posicion['ticket']} cerrada correctamente.")
            except Exception as e:
                print(f"Error durante el monitoreo de cierres: {str(e)}")
            time.sleep(config.get('cierre_interval', 60))  # Intervalo ajustable

    def monitorear_balance():
        print("Iniciando monitoreo de balance...")
        mt5_executor.monitorear_equidad_global(monitoreo_interval=config.get('monitoreo_interval', 60))

        # Al finalizar el monitoreo de la equidad global (cuando se alcanza el límite de pérdida o ganancia),
        # detener todos los hilos
        with hilos_lock:
            global detener_hilos
            detener_hilos = True

    # Iniciar hilos paralelos
    hilos = [
        threading.Thread(target=evaluar_tendencias, daemon=True),
        threading.Thread(target=evaluar_reversiones, daemon=True),
        threading.Thread(target=evaluar_senales, daemon=True),
        threading.Thread(target=monitorear_cierres, daemon=True),
        threading.Thread(target=monitorear_balance, daemon=True)
    ]

    for hilo in hilos:
        hilo.start()

    # Mantener el programa en ejecución
    try:
        while True:
            with hilos_lock:
                if detener_hilos:
                    break
            time.sleep(5)  # Mantiene el programa corriendo y chequea los hilos cada 5 segundos
    except KeyboardInterrupt:
        print("Proceso interrumpido manualmente.")
    finally:
        with hilos_lock:
            detener_hilos = True
        for hilo in hilos:
            hilo.join()  # Asegurar que todos los hilos se terminen correctamente
        if 'mt5_executor' in locals():
            mt5_executor.cerrar_conexion()

if __name__ == "__main__":
    main()
