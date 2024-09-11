import MetaTrader5 as mt5
import pandas as pd
import time
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

class MetaTrader5Executor:
    def __init__(self, close_conditions, atr_period=14, atr_factor=2.0, atr_timeframe=mt5.TIMEFRAME_M15, stop_loss_default=0.01):
        self.conectado = False
        self.operaciones_abiertas = {}  # Guardar las operaciones activas para monitoreo
        self.close_conditions = close_conditions  # Instancia de la clase TradeCloseConditions
        self.atr_period = atr_period  # Periodo para el cálculo del ATR
        self.atr_factor = atr_factor  # Factor multiplicador del ATR para el stop loss
        self.atr_timeframe = atr_timeframe  # Temporalidad para el cálculo del ATR
        self.stop_loss_default = stop_loss_default  # Stop Loss predeterminado si no se puede calcular el ATR
        
        # Intentar conectar a MetaTrader 5
        if not self.conectar_mt5():
            raise ConnectionError("No se pudo conectar con MetaTrader 5.")
        
        # Obtener el balance inicial
        account_info = mt5.account_info()
        if account_info is None:
            raise ValueError("No se pudo obtener la información de la cuenta.")
        
        self.balance_inicial = account_info.balance  # Guardar el balance inicial
        self.max_profit = 0  # Ganancia máxima alcanzada para el trailing stop

    def conectar_mt5(self):
        """ Establece la conexión con MetaTrader 5. """
        print("Intentando conectar con MetaTrader 5...")
        if not mt5.initialize():
            print(f"Error al conectar con MetaTrader 5, código de error = {mt5.last_error()}")
            return False
        self.conectado = True
        print("Conexión con MetaTrader 5 exitosa.")
        return True

    def sincronizar_operaciones_existentes(self):
        """ Sincroniza las posiciones abiertas de MetaTrader 5 con el diccionario 'operaciones_abiertas'. """
        posiciones = self.obtener_posiciones_abiertas()
        for posicion in posiciones:
            symbol = posicion['symbol']
            tipo_operacion = 'compra' if posicion['type'] == mt5.ORDER_TYPE_BUY else 'venta'
            price = posicion['price_open']

            # Recalcular el ATR para asignar un stop loss dinámico
            atr_value = self.obtener_atr(symbol)
            if atr_value:
                stop_loss = price - (self.atr_factor * atr_value) if tipo_operacion == 'compra' else price + (self.atr_factor * atr_value)
            else:
                # Si no se puede calcular el ATR, asignar un stop loss predeterminado
                stop_loss = price - (self.stop_loss_default * price) if tipo_operacion == 'compra' else price + (self.stop_loss_default * price)

            self.operaciones_abiertas[symbol] = {'id': posicion['ticket'], 'tipo': tipo_operacion, 'precio_entrada': price, 'stop_loss': stop_loss}

    def obtener_posiciones_abiertas(self):
        """ Devuelve una lista de las posiciones abiertas actualmente en formato de diccionario. """
        posiciones = mt5.positions_get()
        if posiciones is None:
            print("No hay posiciones abiertas.")
            return []
        return [{'symbol': posicion.symbol, 'ticket': posicion.ticket, 'type': posicion.type, 'price_open': posicion.price_open} for posicion in posiciones]

    def obtener_atr(self, symbol):
        """ Calcula el ATR (Average True Range) para un símbolo en la temporalidad configurada. """
        rates = mt5.copy_rates_from_pos(symbol, self.atr_timeframe, 0, self.atr_period + 1)
        if rates is None or len(rates) < self.atr_period:
            print(f"No se pudo obtener el ATR para {symbol}")
            return None
        
        df = pd.DataFrame(rates)
        df['tr'] = df['high'] - df['low']  # True Range
        atr = df['tr'].rolling(window=self.atr_period).mean().iloc[-1]
        return atr

    def ejecutar_orden(self, symbol, order_type):
        """ Ejecuta una orden de compra o venta en MetaTrader 5, solo si no hay una operación abierta del mismo tipo. """
        if not self.conectado or not self.seleccionar_simbolo(symbol):
            return

        if self.verificar_operacion_existente(symbol, order_type):
            print(f"Operación ya existente: {symbol}, {order_type}. No se abrirá otra operación.")
            return

        price = mt5.symbol_info_tick(symbol).ask if order_type == "buy" else mt5.symbol_info_tick(symbol).bid

        request = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": symbol,
            "volume": 1.0,
            "type": mt5.ORDER_TYPE_BUY if order_type == "buy" else mt5.ORDER_TYPE_SELL,
            "price": price,
            "deviation": 20,
            "magic": 234000,
            "comment": "Orden automática",
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": mt5.ORDER_FILLING_FOK,
        }

        result = mt5.order_send(request)
        if result.retcode != mt5.TRADE_RETCODE_DONE:
            print(f"Error al ejecutar orden: {mt5.last_error()}")
        else:
            print(f"Orden ejecutada con éxito: {symbol}, Tipo: {order_type}, Precio: {result.price}")

    def verificar_operacion_existente(self, symbol, order_type):
        """ Verifica si ya existe una operación del mismo tipo (compra/venta) para un símbolo. """
        if symbol in self.operaciones_abiertas:
            tipo_operacion = self.operaciones_abiertas[symbol]['tipo']
            if (tipo_operacion == 'compra' and order_type == 'buy') or (tipo_operacion == 'venta' and order_type == 'sell'):
                return True

        posiciones = self.obtener_posiciones_abiertas()
        for posicion in posiciones:
            if posicion['symbol'] == symbol and (
                (order_type == 'buy' and posicion['type'] == mt5.ORDER_TYPE_BUY) or 
                (order_type == 'sell' and posicion['type'] == mt5.ORDER_TYPE_SELL)):
                return True
        return False

    def monitorear_balance_global(self, max_loss_level, profit_trailing_level, trailing_stop_percentage):
        """ Monitorea las pérdidas y ganancias globales de la cuenta. """
        while True:
            account_info = mt5.account_info()
            if account_info is None:
                print("No se pudo obtener la información de la cuenta.")
                return

            balance_actual = account_info.balance
            ganancia_neta = balance_actual - self.balance_inicial

            # Comprobar si se alcanza el nivel de pérdida
            if ganancia_neta <= max_loss_level:
                print(f"Nivel de pérdida alcanzado: {ganancia_neta}. Cerrando todas las operaciones...")
                self.cerrar_todas_las_operaciones()
                self.enviar_correo(f"Pérdida alcanzada. Balance final: {balance_actual}")
                mt5.shutdown()
                break

            # Comprobar si se alcanza el nivel de ganancia para activar el trailing stop
            if ganancia_neta >= profit_trailing_level:
                self.max_profit = max(self.max_profit, ganancia_neta)
                trailing_stop_value = self.max_profit * (1 - trailing_stop_percentage / 100)

                if ganancia_neta <= trailing_stop_value:
                    print(f"*** Trailing Stop activado ***")
                    print(f"Ganancia máxima alcanzada: {self.max_profit}")
                    print(f"Las operaciones se cerrarán al alcanzar la ganancia de: {trailing_stop_value}")
                    
                    # Aquí se cierran todas las operaciones
                    self.cerrar_todas_las_operaciones()

                    # Enviar correo notificando la activación del trailing stop
                    self.enviar_correo(f"Ganancia alcanzada. Cierre activado con ganancia neta de {ganancia_neta}. Balance final: {balance_actual}")
                    mt5.shutdown()
                    break

            time.sleep(60)  # Intervalo de monitoreo

    def monitorear_stop_loss(self):
        """ Monitorea las operaciones abiertas y cierra las que alcancen el stop loss dinámico. """
        for symbol, data in list(self.operaciones_abiertas.items()):  # Usar list() para evitar RuntimeError
            try:
                # Obtener el precio actual
                symbol_info = mt5.symbol_info_tick(symbol)
                if symbol_info is None:
                    print(f"No se pudo obtener el precio actual para {symbol}. Saltando operación.")
                    continue

                price_actual = symbol_info.ask if data['tipo'] == 'compra' else symbol_info.bid
                
                # Si falta el stop loss, recalcularlo
                if 'stop_loss' not in data:
                    print(f"Recalculando stop loss para {symbol}.")
                    atr_value = self.obtener_atr(symbol)
                    if atr_value:
                        data['stop_loss'] = data['precio_entrada'] - (self.atr_factor * atr_value) if data['tipo'] == 'compra' else data['precio_entrada'] + (self.atr_factor * atr_value)
                        print(f"Nuevo stop loss para {symbol}: {data['stop_loss']}")
                    else:
                        # Si no se puede calcular el ATR, asignar un stop loss predeterminado
                        data['stop_loss'] = data['precio_entrada'] - (self.stop_loss_default * data['precio_entrada']) if data['tipo'] == 'compra' else data['precio_entrada'] + (self.stop_loss_default * data['precio_entrada'])
                        print(f"Error al recalcular ATR para {symbol}. Se asigna un Stop Loss predeterminado: {data['stop_loss']}")

                # Verificar si el precio actual ha alcanzado o excedido el stop loss
                if (data['tipo'] == 'compra' and price_actual <= data['stop_loss']) or (data['tipo'] == 'venta' and price_actual >= data['stop_loss']):
                    print(f"Stop loss alcanzado para {symbol}. Cerrando posición.")
                    self.cerrar_posicion(symbol, data['id'])
            except KeyError:
                print(f"Error: No se encontró el stop_loss para {symbol}. Saltando la operación.")

    def cerrar_todas_las_operaciones(self):
        """ Cierra todas las operaciones abiertas en MetaTrader 5. """
        posiciones = self.obtener_posiciones_abiertas()
        for posicion in posiciones:
            self.cerrar_posicion(posicion['symbol'], posicion['ticket'])

    def cerrar_posicion(self, symbol, ticket):
        """ Cierra una posición en MetaTrader 5. """
        posicion = mt5.positions_get(ticket=ticket)
        if posicion is None or len(posicion) == 0:
            return False

        precio = mt5.symbol_info_tick(symbol).bid if posicion[0].type == mt5.ORDER_TYPE_BUY else mt5.symbol_info_tick(symbol).ask
        request = {
            "action": mt5.TRADE_ACTION_DEAL,
            "position": ticket,
            "symbol": symbol,
            "volume": posicion[0].volume,
            "type": mt5.ORDER_TYPE_SELL if posicion[0].type == mt5.ORDER_TYPE_BUY else mt5.ORDER_TYPE_BUY,
            "price": precio,
            "deviation": 20,
            "magic": 234000,
            "comment": "Cierre automático"
        }
        result = mt5.order_send(request)
        if result.retcode != mt5.TRADE_RETCODE_DONE:
            print(f"No se pudo cerrar la posición: {symbol}, Ticket: {ticket}")
            return False
        else:
            del self.operaciones_abiertas[symbol]
            print(f"Posición cerrada: {symbol}, Ticket: {ticket}")
            return True

    def enviar_correo(self, mensaje):
        """ Envía un correo con el resultado cuando se cierran todas las operaciones. """
        remitente = "tu_correo@gmail.com"
        destinatario = "destinatario_correo@gmail.com"
        asunto = "Informe de Trading - MetaTrader5"
        cuerpo = mensaje

        msg = MIMEMultipart()
        msg['From'] = remitente
        msg['To'] = destinatario
        msg['Subject'] = asunto
        msg.attach(MIMEText(cuerpo, 'plain'))

        try:
            servidor = smtplib.SMTP('smtp.gmail.com', 587)
            servidor.starttls()
            servidor.login(remitente, "tu_contraseña")
            texto = msg.as_string()
            servidor.sendmail(remitente, destinatario, texto)
            servidor.quit()
            print("Correo enviado exitosamente.")
        except Exception as e:
            print(f"Error al enviar el correo: {str(e)}")

    def seleccionar_simbolo(self, symbol):
        """ Asegura que el símbolo esté seleccionado en MetaTrader 5. """
        if not mt5.symbol_select(symbol, True):
            print(f"No se pudo seleccionar el símbolo: {symbol}")
            return False
        return True

    def cerrar_conexion(self):
        """ Cierra la conexión con MetaTrader 5. """
        if mt5.shutdown():
            self.conectado = False
            print("Conexión con MetaTrader 5 cerrada.")
