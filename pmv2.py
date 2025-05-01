import paho.mqtt.client as mqtt
import mysql.connector
import time

# Configuración de la base de datos MySQL
MYSQL_CONFIG = {
    "host": "localhost",
    "user": "aireuser",
    "password": "airepassword",
    "database": "aire"
}

# Centrales y sus tópicos
centrales = {
    "munisclem": ["/munisclem/Aire/tt", "/munisclem/Aire/hh", "/munisclem/Aire/pm25", "/munisclem/Aire/pm10", "/munisclem/Aire/fecha"],
    "colegio11": ["/colegio11/Aire/tt", "/colegio11/Aire/hh", "/colegio11/Aire/pm25", "/colegio11/Aire/pm10", "/colegio11/Aire/fecha"]
}

# Estructura para almacenar datos temporalmente
data_store = {
    central: {"tt": None, "hh": None, "pm25": None, "pm10": None, "fecha": None}
    for central in centrales
}

def insertar_en_db(central, datos):
    """Inserta una fila en la base de datos MySQL"""
    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO mediciones (central, tt, hh, pm25, pm10, fecha)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (central, datos["tt"], datos["hh"], datos["pm25"], datos["pm10"], datos["fecha"]))
        conn.commit()
        cursor.close()
        conn.close()
        print(f" Insertado en DB: {central} - {datos}")
    except mysql.connector.Error as err:
        print(f" Error al insertar en DB: {err}")

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print(" Conectado al broker MQTT.")
        for central, topics in centrales.items():
            for topic in topics:
                client.subscribe(topic)
                print(f" Suscrito a: {topic}")
    else:
        print(f" Error al conectar al broker. Código: {rc}")

def on_message(client, userdata, msg):
    try:
        for central, topics in centrales.items():
            if msg.topic in topics:
                key = msg.topic.split("/")[-1]
                value = msg.payload.decode()
                if value:
                    data_store[central][key] = float(value) if key != "fecha" else value
                if all(data_store[central].values()):
                    nombre_central = "colegio1" if central == "colegio11" else "SC"
                    insertar_en_db(nombre_central, data_store[central])
                    data_store[central] = {k: None for k in data_store[central]}
    except ValueError as e:
        print(f" Error al procesar mensaje: {e}")

def iniciar_cliente():
    while True:
        try:
            client = mqtt.Client()
            client.on_connect = on_connect
            client.on_message = on_message
            client.connect(broker, port, 60)
            client.loop_forever()
        except Exception as e:
            print(f" Error en la conexión: {e}. Reintentando en 5 segundos...")
            time.sleep(5)

# Configuración del broker MQTT
broker = "test.mosquitto.org"
port = 1883

# Inicia el sistema
iniciar_cliente()
