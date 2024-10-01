import sys
import os
import json
import logging
import subprocess
import time
import requests
import MetaTrader5 as mt5
from flask import Flask, jsonify

# Configuración de logging
logging.basicConfig(
    filename=os.path.join(os.path.dirname(__file__), '..', 'logs', 'trading_decision.log'),
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Ajustar la ruta de sys.path para asegurar que los módulos correctos sean accesibles
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

# Importa desde las rutas correctas los analizadores si necesitas importarlos para una configuración adicional
from src.senales.ForexSignalAnalyzer import ForexSignalAnalyzer
from src.reversals.ForexReversalAnalyzer import ForexReversalAnalyzer
from src.tendencias.ForexAnalyzer import ForexAnalyzer

# Definir URLs de los servidores
TENDENCIA_URL = 'http://localhost:5000/tendencias'
REVERSIONES_URL = 'http://localhost:5001/reversals'
SENALES_URL = 'http://localhost:5002/signals'

# Inicializar MetaTrader5
mt5.initialize()

# Crear la aplicación Flask
app = Flask(__name__)

def iniciar_servidor(script_path, nombre_servidor, puerto):
    """Función para inicializar servidores usando subprocess"""
    try:
        logging.info(f"Iniciando el servidor de {nombre_servidor}...")
        subprocess.Popen(["python", script_path])
        # Esperar un tiempo para asegurarnos que el servidor esté levantado
        time.sleep(5)
        # Verificar si el servidor está activo
        if requests.get(f'http://localhost:{puerto}').status_code == 200:
            logging.info(f"Servidor {nombre_servidor} en el puerto {puerto} iniciado correctamente.")
        else:
            logging.error(f"No se pudo verificar el estado del servidor {nombre_servidor} en el puerto {puerto}")
    except Exception as e:
        logging.error(f"Error al iniciar el servidor {nombre_servidor}: {e}")

def iniciar_todos_los_servidores():
    """Inicializa todos los servidores"""
    iniciar_servidor("src/services/forex_analyzer_server.py", "Tendencias", 5000)
    iniciar_servidor("src/services/forex_reversal_server.py", "Reversiones", 5001)
    iniciar_servidor("src/services/forex_signal_server.py", "Señales", 5002)

def obtener_datos_servidor(url):
    """Función para obtener datos de un servidor en particular."""
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Error al obtener datos de {url}: {e}")
        return {}

def combinar_tendencia_reversion_signal():
    """Función que combina los resultados de los tres servidores."""
    # Obtener datos de los tres servidores
    tendencias = obtener_datos_servidor(TENDENCIA_URL)
    reversiones = obtener_datos_servidor(REVERSIONES_URL)
    senales = obtener_datos_servidor(SENALES_URL)

    logging.info(f"Tendencias obtenidas: {tendencias}")
    logging.info(f"Reversiones obtenidas: {reversiones}")
    logging.info(f"Señales obtenidas: {senales}")

    # Centralizar la lógica
    decisiones = {}
    for pair in tendencias:
        tendencia = tendencias.get(pair)
        reversion = reversiones.get(pair)
        senal = senales.get(pair)

        if tendencia and reversion and senal:
            if tendencia == "Tendencia Alcista" and reversion == "Reversión Alcista" and senal == "Señal de Compra":
                decisiones[pair] = "buy"
            elif tendencia == "Tendencia Bajista" and reversion == "Reversión Bajista" and senal == "Señal de Venta":
                decisiones[pair] = "sell"
            else:
                decisiones[pair] = "hold"
        else:
            decisiones[pair] = "hold"
    
    return decisiones

def ejecutar_operacion_mt5(pair, accion):
    """Función que envía la operación a MetaTrader5."""
    symbol_info = mt5.symbol_info(pair)
    if symbol_info is None:
        logging.error(f"El símbolo {pair} no está disponible en MetaTrader5")
        return False

    if not symbol_info.visible:
        logging.info(f"El símbolo {pair} no está visible, intentado activarlo...")
        if not mt5.symbol_select(pair, True):
            logging.error(f"No se pudo activar el símbolo {pair}")
            return False

    lot = 0.1  # Definir el tamaño del lote, este es solo un ejemplo
    price = mt5.symbol_info_tick(pair).ask if accion == "buy" else mt5.symbol_info_tick(pair).bid

    order_type = mt5.ORDER_TYPE_BUY if accion == "buy" else mt5.ORDER_TYPE_SELL
    request = {
        "action": mt5.TRADE_ACTION_DEAL,
        "symbol": pair,
        "volume": lot,
        "type": order_type,
        "price": price,
        "deviation": 20,
        "magic": 234000,
        "comment": "Trade ejecutado automáticamente",
        "type_time": mt5.ORDER_TIME_GTC,
        "type_filling": mt5.ORDER_FILLING_IOC,
    }

    result = mt5.order_send(request)
    if result.retcode != mt5.TRADE_RETCODE_DONE:
        logging.error(f"Error al ejecutar la operación {accion} para {pair}: {result.retcode}")
        return False
    else:
        logging.info(f"Operación {accion} ejecutada correctamente para {pair}")
        return True

@app.route('/execute_trades', methods=['GET'])
def ejecutar_trades():
    """Endpoint que combina los resultados y ejecuta operaciones."""
    decisiones = combinar_tendencia_reversion_signal()
    resultados = {}
    
    for pair, accion in decisiones.items():
        if accion in ["buy", "sell"]:
            logging.info(f"Ejecutando operación {accion} para {pair}")
            resultado = ejecutar_operacion_mt5(pair, accion)
            resultados[pair] = f"Operación {accion} {'exitosa' if resultado else 'fallida'}"
        else:
            resultados[pair] = "Ninguna operación ejecutada"
    
    return jsonify(resultados)

# Iniciar el servidor y los otros servidores de soporte
if __name__ == '__main__':
    try:
        logging.info("Iniciando el servidor central y los servidores de soporte...")
        iniciar_todos_los_servidores()  # Inicializar los otros servidores
        app.run(port=5003)
    except Exception as e:
        logging.error(f"Error al iniciar el servidor central: {e}")
