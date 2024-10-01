import time
import json
import logging
from logging.handlers import RotatingFileHandler
from concurrent.futures import ThreadPoolExecutor
import MetaTrader5 as mt5
from tendencias.ForexAnalyzer import ForexAnalyzer
from reversals.ForexReversalAnalyzer import ForexReversalAnalyzer
from src.senales.ForexSignalAnalyzer import ForexSignalAnalyzer
from MetaTrader5Executor import MetaTrader5Executor
from TradeCloseConditions import TradeCloseConditions

# Configuración de logging con codificación UTF-8 y rotación de logs
log_handler = RotatingFileHandler('trading_system.log', maxBytes=5*1024*1024, backupCount=2, encoding='utf-8')
log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
log_handler.setFormatter(log_formatter)

logging.basicConfig(level=logging.INFO, handlers=[log_handler])

# Banderas para controlar las impresiones
imprimir_tendencias = True
imprimir_reversiones = True
imprimir_senales = True
imprimir_cierres = False  # No imprimir en monitorear_cierres

# Variable de control global para detener todos los hilos
detener_hilos = False

def normalizar_par(pair):
    return pair.replace("-", "")

def main():
    global detener_hilos

    # Cargar configuración desde un archivo JSON
    try:
        with open("config.json") as config_file:
            config = json.load(config_file)
        logging.info("Configuración cargada correctamente.")
    except Exception as e:
        logging.error(f"Error al cargar la configuración: {e}")
        return

    # Instanciar MetaTrader5Executor
    try:
        logging.info("Instanciando MetaTrader5Executor...")
        mt5_executor = MetaTrader5Executor(
            config['max_loss_level'],
            config['profit_trailing_level'],
            config['trailing_stop_percentage']
        )
    except Exception as e:
        logging.error(f"Error al instanciar MetaTrader5Executor: {e}")
        return

    # Instanciar TradeCloseConditions con mt5_executor
    logging.info("Instanciando TradeCloseConditions...")
    close_conditions = TradeCloseConditions(mt5_executor)
    mt5_executor.close_conditions = close_conditions

    # Instanciar ForexAnalyzer, ForexReversalAnalyzer, y ForexSignalAnalyzer
    try:
        logging.info("Instanciando analizadores Forex...")
        forex_analyzer = ForexAnalyzer(config['api_key_polygon'], config['pairs'])
        forex_reversal_analyzer = ForexReversalAnalyzer(mt5_executor, config['api_key_polygon'])
        forex_signal_analyzer = ForexSignalAnalyzer(mt5_executor, config['api_key_polygon'])
    except Exception as e:
        logging.error(f"Error al instanciar analizadores: {e}")
        return

    # Verificar la conexión con MetaTrader 5
    if not mt5_executor.conectado:
        logging.error("Error al conectar con MetaTrader 5.")
        return
    else:
        logging.info("Conexión a MetaTrader 5 exitosa.")

    # Funciones de evaluación y monitoreo
    def evaluar_tendencias():
        logging.info("Iniciando evaluación de tendencias...")
        while not detener_hilos:
            try:
                for pair in config['pairs']:
                    pair_normalizado = normalizar_par(pair)
                    logging.info(f"Evaluando tendencia para el par: {pair}")
                    forex_analyzer.analizar_par(pair_normalizado)

                if imprimir_tendencias:
                    with forex_analyzer.lock:
                        last_trend_copy = forex_analyzer.last_trend.copy()
                    if not last_trend_copy:
                        logging.info("No se detectaron tendencias alcistas o bajistas.")
                    else:
                        logging.info("Tendencias detectadas:")
                        for pair, tendencia in last_trend_copy.items():
                            logging.info(f"{pair}: {tendencia}")

                time.sleep(config.get('tendencia_interval', 14400))  # Intervalo ajustable a 4 horas
            except Exception as e:
                logging.error(f"Error durante la evaluación de tendencias: {str(e)}")
                time.sleep(60)

    def evaluar_reversiones():
        logging.info("Iniciando evaluación de reversiones...")
        while not detener_hilos:
            try:
                with forex_analyzer.lock:
                    tendencias = forex_analyzer.last_trend.copy()
                pares_reversion = forex_reversal_analyzer.analizar_reversiones(tendencias)
                if imprimir_reversiones and pares_reversion:
                    logging.info(f"Pares válidos para analizar reversiones: {pares_reversion}")
                time.sleep(config.get('reversion_interval', 60))  # Intervalo ajustable
            except Exception as e:
                logging.error(f"Error durante la evaluación de reversiones: {str(e)}")
                time.sleep(60)

    def evaluar_senales():
        logging.info("Iniciando evaluación de señales...")
        while not detener_hilos:
            try:
                with forex_reversal_analyzer.lock:
                    resultados_reversiones = forex_reversal_analyzer.resultados.copy()
                if resultados_reversiones:
                    logging.info(f"Analizando señales para reversiones detectadas: {resultados_reversiones}")
                    forex_signal_analyzer.analizar_senales(resultados_reversiones, imprimir_senales)
                else:
                    logging.info("No hay reversiones para analizar señales.")
                time.sleep(config.get('senales_interval', 60))  # Intervalo ajustable
            except Exception as e:
                logging.error(f"Error durante la evaluación de señales: {str(e)}")
                time.sleep(60)

    def monitorear_cierres():
        logging.info("Iniciando monitoreo de cierres...")
        while not detener_hilos:
            try:
                posiciones = mt5_executor.obtener_posiciones_abiertas()
                logging.info(f"Posiciones abiertas: {posiciones}")
                for posicion in posiciones:
                    symbol = posicion['symbol']
                    symbol_normalizado = normalizar_par(symbol)

                    with forex_analyzer.lock:
                        nueva_tendencia = forex_analyzer.last_trend.get(symbol_normalizado, None)

                    if nueva_tendencia is None:
                        logging.info(f"Tendencia aún no disponible para {symbol_normalizado}. No se realizará ninguna acción.")
                        continue

                    if close_conditions.verificar_cierre_por_condiciones(symbol_normalizado, nueva_tendencia):
                        if imprimir_cierres:
                            logging.info(f"Cerrando posición para {symbol_normalizado}")
                        resultado = mt5_executor.cerrar_operacion(posicion['ticket'])

                        if resultado is None:
                            logging.error(f"Error al intentar cerrar la operación {posicion['ticket']}. No se recibió respuesta.")
                        elif resultado.retcode != mt5.TRADE_RETCODE_DONE:
                            logging.error(f"Error al cerrar la operación {posicion['ticket']}. Código de error: {resultado.retcode}")
                        else:
                            logging.info(f"Operación {posicion['ticket']} cerrada correctamente.")
            except Exception as e:
                logging.error(f"Error durante el monitoreo de cierres: {str(e)}")
            time.sleep(config.get('cierre_interval', 60))  # Intervalo ajustable

    def monitorear_balance():
        logging.info("Iniciando monitoreo de balance...")
        try:
            mt5_executor.monitorear_equidad_global(monitoreo_interval=config.get('monitoreo_interval', 60))
            logging.info("Monitoreo de balance completado. Deteniendo hilos.")
            global detener_hilos
            detener_hilos = True  # Detener todos los hilos cuando se alcance el límite de balance
        except Exception as e:
            logging.error(f"Error durante el monitoreo de balance: {str(e)}")

    # Usar ThreadPoolExecutor en lugar de hilos manuales
    with ThreadPoolExecutor(max_workers=5) as executor:
        logging.info("Iniciando ThreadPoolExecutor...")
        executor.submit(evaluar_tendencias)
        executor.submit(evaluar_reversiones)
        executor.submit(evaluar_senales)
        executor.submit(monitorear_cierres)
        executor.submit(monitorear_balance)

    # Mantener el programa en ejecución
    try:
        while not detener_hilos:
            time.sleep(5)  # Mantiene el programa corriendo y chequea el estado de los hilos
    except KeyboardInterrupt:
        logging.info("Proceso interrumpido manualmente.")
    finally:
        detener_hilos = True
        logging.info("Cerrando ejecución de hilos...")
        if 'mt5_executor' in locals():
            mt5_executor.cerrar_conexion()
        logging.info("Programa finalizado.")

if __name__ == "__main__":
    main()
