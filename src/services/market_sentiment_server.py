import json
import os
from datetime import datetime, timedelta
import logging
from flask import Flask, jsonify
import requests
import psycopg2
from time import sleep

app = Flask(__name__)

# Rutas de configuración y logs
CONFIG_PATH = "src/config/config.json"
LOG_PATH = "src/logs/market_sentiment.log"
ultima_consulta = None

# Configuración de logging
if not os.path.exists("src/logs"):
    os.makedirs("src/logs")

logging.basicConfig(
    filename=LOG_PATH,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
)

def log_info(message):
    logging.info(message)

def log_error(message):
    logging.error(message)

def load_config():
    try:
        with open(CONFIG_PATH, 'r') as f:
            config = json.load(f)
            log_info("Archivo config.json cargado correctamente.")
            return config
    except Exception as e:
        log_error(f"Error al cargar config.json: {str(e)}")
        raise

def determinar_sentimiento(score):
    return "alcista" if score > 0 else "bajista" if score < 0 else "neutral"

def conectar_db():
    config = load_config().get("db_config", {})
    try:
        conn = psycopg2.connect(
            host=config['host'],
            database=config['database'],
            user=config['user'],
            password=config['password'],
            options='-c client_encoding=UTF8'
        )
        log_info("Conexión a la base de datos PostgreSQL exitosa.")
        return conn
    except psycopg2.Error as e:
        log_error(f"Error al conectar a la base de datos: {e}")
        return None

def guardar_sentimiento(symbol, sentiment_data):
    conn = conectar_db()
    if not conn:
        log_error(f"No se pudo guardar el sentimiento para {symbol}.")
        return

    query = """
        INSERT INTO market_sentiments (symbol, sentiment_score, sentimiento, date) 
        VALUES (%s, %s, %s, %s)
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute(query, (
                sentiment_data['symbol'],
                sentiment_data['sentiment_score'],
                sentiment_data['sentimiento'],
                sentiment_data['date']
            ))
            conn.commit()
            log_info(f"Sentimiento para {symbol} guardado correctamente.")
    except psycopg2.Error as e:
        log_error(f"Error al guardar sentimiento para {symbol}: {e}")
        conn.rollback()
    finally:
        conn.close()

def obtener_estado_mercado():
    config = load_config()
    api_key = config["api_key_polygon"]
    url = f"https://api.polygon.io/v1/marketstatus/now?apiKey={api_key}"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            fx_status = data["currencies"].get("fx", "closed")
            log_info(f"Estado del mercado Forex: {fx_status}")
            return fx_status == "open"
        else:
            log_error(f"Error al consultar estado del mercado: {response.status_code}")
            return False
    except Exception as e:
        log_error(f"Error al obtener estado del mercado: {str(e)}")
        return False

def obtener_sentimiento(symbol):
    config = load_config()
    api_token = config["api_token_forexnews"]
    formatted_symbol = symbol[:3] + '-' + symbol[3:]
    url = f"https://forexnewsapi.com/api/v1/stat?currencypair={formatted_symbol}&date=last30days&page=1&token={api_token}"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            if data.get("data"):
                latest_date = max(data["data"].keys())
                details = data["data"][latest_date][formatted_symbol]
                sentiment_score = details.get("sentiment_score")
                sentiment_data = {
                    "symbol": symbol,
                    "date": latest_date,
                    "sentiment_score": sentiment_score,
                    "sentimiento": determinar_sentimiento(sentiment_score)
                }
                guardar_sentimiento(symbol, sentiment_data)
            else:
                log_info(f"No hay datos disponibles para {symbol}.")
        else:
            log_error(f"API error {response.status_code} para {symbol}.")
    except Exception as e:
        log_error(f"Error al obtener datos para {symbol}: {str(e)}")

def consultar_y_guardar_todos():
    global ultima_consulta
    if ultima_consulta and (datetime.now() - ultima_consulta).total_seconds() < 3 * 60 * 60:
        log_info("Consulta ya realizada recientemente. Esperando próximo ciclo.")
        return

    log_info("Iniciando consulta de sentimientos.")
    ultima_consulta = datetime.now()
    config = load_config()
    pairs = config.get("pairs", [])
    for pair in pairs:
        obtener_sentimiento(pair)

def proceso_principal():
    while True:
        if obtener_estado_mercado():
            consultar_y_guardar_todos()
            log_info("Proceso completado. Esperando 3 horas para el próximo ciclo.")
            sleep(3 * 60 * 60)
        else:
            log_info("Mercado cerrado. Reintentando en 5 minutos.")
            sleep(5 * 60)

@app.route('/get_market_sentiments', methods=['GET'])
def get_market_sentiments():
    consultar_y_guardar_todos()
    return jsonify({"message": "Proceso completado."}), 200

if __name__ == '__main__':
    log_info("Servidor iniciado.")
    if obtener_estado_mercado():
        consultar_y_guardar_todos()

    # Iniciar proceso principal sin hilos
    proceso_principal()
    app.run(host='0.0.0.0', port=2008)

