import os
import sys
import json
import logging
from flask import Flask, jsonify
from datetime import datetime
import psycopg2

# Agregar la raíz del proyecto al PYTHONPATH
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.insert(0, BASE_DIR)

# Configurar la carpeta y el archivo de logs en src/logs
LOG_DIR = os.path.join(BASE_DIR, 'src', 'logs')
os.makedirs(LOG_DIR, exist_ok=True)  # Crear la carpeta si no existe
LOG_FILE = os.path.join(LOG_DIR, 'market_sentiment_server.log')

# Configurar el logger
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Cargar configuración desde src/config/config.json
CONFIG_FILE = os.path.join(BASE_DIR, 'src', 'config', 'config.json')
if not os.path.exists(CONFIG_FILE):
    logger.error(f"Archivo de configuración no encontrado: {CONFIG_FILE}")
    raise FileNotFoundError(f"No se encontró el archivo de configuración: {CONFIG_FILE}")

with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
    config = json.load(f)

db_config = config.get('db_config', {})

# Crear la aplicación Flask
app = Flask(__name__)

def connect_db():
    """Conectar a la base de datos."""
    try:
        conn = psycopg2.connect(
            host=db_config["host"],
            database=db_config["database"],
            user=db_config["user"],
            password=db_config["password"]
        )
        logger.info("Conexión a la base de datos establecida para el servidor REST.")
        return conn
    except Exception as e:
        logger.error(f"Error al conectar con la base de datos: {e}", exc_info=True)
        raise

@app.route('/sentiment/<symbol>', methods=['GET'])
def get_sentiment(symbol):
    """Obtener el sentimiento de un símbolo específico."""
    try:
        conn = connect_db()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT sentiment_result FROM market_sentiment WHERE symbol = %s ORDER BY last_update DESC LIMIT 1",
            (symbol,)
        )
        result = cursor.fetchone()
        cursor.close()
        conn.close()

        if result:
            logger.info(f"Sentimiento devuelto para {symbol}: {result[0]}")
            return jsonify({symbol: result[0]})
        else:
            logger.warning(f"No se encontró el sentimiento para {symbol}.")
            return jsonify({"error": "Symbol not found"}), 404

    except Exception as e:
        logger.error(f"Error en la solicitud para {symbol}: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500

@app.route('/sentiment', methods=['GET'])
def get_all_sentiments():
    """Obtener el sentimiento de todos los símbolos."""
    try:
        conn = connect_db()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT symbol, sentiment_result FROM market_sentiment ORDER BY last_update DESC"
        )
        results = cursor.fetchall()
        cursor.close()
        conn.close()

        sentiments = {symbol: sentiment for symbol, sentiment in results}
        sentiments["last_update"] = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
        logger.info("Sentimientos devueltos para todos los símbolos.")
        return jsonify(sentiments)

    except Exception as e:
        logger.error(f"Error al obtener todos los sentimientos: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    logger.info("Iniciando servidor Flask para Market Sentiment...")
    app.run(host='0.0.0.0', port=5008, debug=False)
