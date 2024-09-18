import time
import threading
import json
from datetime import datetime
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

def normalizar_par(pair):
    """
    Normaliza el formato del par de divisas, eliminando guiones para que siempre se utilice sin guiones.
    """
    return pair.replace("-", "")

def calcular_tiempo_restante(intervalo_segundos):
    """
    Calcula cuánto tiempo falta para que se complete la vela actual, en función del intervalo (en segundos).
    """
    ahora = datetime.now()
    segundos_transcurridos = (ahora.minute * 60 + ahora.second) % intervalo_segundos
    tiempo_restante = intervalo_segundos - segundos_transcurridos
    return tiempo_restante

def main():
    # Cargar configuración desde un archivo JSON
    with open("config.json") as config_file:
        config = json.load(config_file)

    # Instanciar MetaTrader5Executor
    mt5_executor = MetaTrader5Executor(None)

    # Instanciar TradeCloseConditions con mt5_executor
    close_conditions = TradeCloseConditions(mt5_executor)
    mt5_executor.close_conditions = close_conditions

    # Instanciar ForexAnalyzer con la clave API y pares de divisas
    forex_analyzer = ForexAnalyzer(config['api_key_polygon'], config['pairs'])
    # Instanciar ForexReversalAnalyzer con mt5_executor
    forex_reversal_analyzer = ForexReversalAnalyzer(mt5_executor, config['api_key_polygon'])
    # Instanciar ForexSignalAnalyzer sin DataFetcher
    forex_signal_analyzer = ForexSignalAnalyzer(mt5_executor, config['api_key_polygon'])

    # Conectar MetaTrader 5
    if not mt5_executor.conectar_mt5():
        print("Error al conectar con MetaTrader 5")
        return

    # Definir funciones de operaciones
    def evaluar_tendencias():
        primer_análisis = True  # Bandera para el primer análisis
        while True:
            try:
                if not primer_análisis:
                    # Calcular cuánto tiempo queda para que se forme la siguiente vela completa de 1 hora
                    tiempo_restante = calcular_tiempo_restante(3600)  # 3600 segundos = 1 hora
                    time.sleep(tiempo_restante)

                # Analizar tendencias al cierre de la vela o inmediatamente para el primer análisis
                for pair in config['pairs']:
                    pair_normalizado = normalizar_par(pair)
                    resultado = forex_analyzer.analizar_par(pair_normalizado)

                # Imprimir solo las tendencias alcistas o bajistas desde last_trend
                if imprimir_tendencias:
                    if not forex_analyzer.last_trend:
                        print("No se detectaron tendencias alcistas o bajistas.")
                    else:
                        for pair, tendencia in forex_analyzer.last_trend.items():
                            pair_normalizado = normalizar_par(pair)
                            print(f"Tendencia fuerte para {pair_normalizado}: {tendencia}")

                primer_análisis = False  # Desactivar la bandera después del primer análisis
            except Exception as e:
                print(f"Error durante la evaluación de tendencias: {str(e)}")
                time.sleep(60)  # Esperar un minuto antes de intentar de nuevo

    def evaluar_reversiones():
        primer_análisis = True  # Bandera para el primer análisis
        while True:
            try:
                if not primer_análisis:
                    # Calcular cuánto tiempo queda para que se forme la siguiente vela completa de 15 minutos
                    tiempo_restante = calcular_tiempo_restante(900)  # 900 segundos = 15 minutos
                    time.sleep(tiempo_restante)

                # Evaluar reversiones al cierre de la vela o inmediatamente para el primer análisis
                pares_reversion = forex_reversal_analyzer.analizar_reversiones(forex_analyzer.last_trend)
                if imprimir_reversiones:
                    for pair, reversion in pares_reversion.items():
                        pair_normalizado = normalizar_par(pair)
                        print(f"Reversión detectada para {pair_normalizado}: {reversion}")

                primer_análisis = False  # Desactivar la bandera después del primer análisis
            except Exception as e:
                print(f"Error durante la evaluación de reversiones: {str(e)}")
                time.sleep(60)  # Esperar un minuto antes de intentar de nuevo

    def evaluar_senales():
        primer_análisis = True  # Bandera para el primer análisis
        while True:
            try:
                if not primer_análisis:
                    # Calcular cuánto tiempo queda para que se forme la siguiente vela completa de 3 minutos
                    tiempo_restante = calcular_tiempo_restante(180)  # 180 segundos = 3 minutos
                    time.sleep(tiempo_restante)

                # Evaluar señales al cierre de la vela o inmediatamente para el primer análisis
                forex_signal_analyzer.analizar_senales(forex_reversal_analyzer.resultados, imprimir_senales)

                primer_análisis = False  # Desactivar la bandera después del primer análisis
            except Exception as e:
                print(f"Error durante la evaluación de señales: {str(e)}")
                time.sleep(60)  # Esperar un minuto antes de intentar de nuevo

    def monitorear_cierres():
        while True:
            try:
                # Monitorear las posiciones abiertas para verificar si deben cerrarse
                for posicion in mt5_executor.obtener_posiciones_abiertas():
                    symbol = posicion['symbol']
                    symbol_normalizado = normalizar_par(symbol)
                    nueva_tendencia = forex_analyzer.analizar_par(symbol_normalizado)
                    if close_conditions.verificar_cierre_por_condiciones(symbol_normalizado, nueva_tendencia):
                        if imprimir_cierres:
                            print(f"Cerrando posición para {symbol_normalizado}")
                        mt5_executor.cerrar_posicion(symbol, posicion['ticket'])
            except Exception as e:
                if imprimir_cierres:
                    print(f"Error durante el monitoreo de cierres: {str(e)}")
            time.sleep(config.get('cierre_interval', 180))

    def monitorear_balance():
        """
        Monitorea el balance global de la cuenta para detectar niveles de pérdidas o ganancias.
        """
        mt5_executor.monitorear_balance_global(
            config['max_loss_level'],
            config['profit_trailing_level'],
            config['trailing_stop_percentage']
        )

    # Iniciar hilos paralelos
    hilo_tendencias = threading.Thread(target=evaluar_tendencias)
    hilo_reversiones = threading.Thread(target=evaluar_reversiones)
    hilo_senales = threading.Thread(target=evaluar_senales)
    hilo_cierres = threading.Thread(target=monitorear_cierres)
    hilo_balance = threading.Thread(target=monitorear_balance)  # Nuevo hilo para monitoreo de balance

    hilo_tendencias.start()
    hilo_reversiones.start()
    hilo_senales.start()
    hilo_cierres.start()
    hilo_balance.start()

    hilo_tendencias.join()
    hilo_reversiones.join()
    hilo_senales.join()
    hilo_cierres.join()
    hilo_balance.join()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Proceso interrumpido manualmente.")
    finally:
        if 'mt5_executor' in locals():
            mt5_executor.cerrar_conexion()
