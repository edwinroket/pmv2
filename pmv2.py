import paho.mqtt.client as mqtt
import pymysql
import time

# Configuración de la base de datos MySQL usando pymysql
MYSQL_CONFIG = {
    "host": "172.17.0.1",  # IP del host desde el contenedor
    "user": "aireuser",
    "password": "airepassword",
    "database": "aire",
    "port": 3306
}

# Centrales y sus tópicos
centrales = {
    "munisclem": ["/munisclem/Aire/tt", "/munisclem/Aire/hh", "/munisclem/Aire/pm25", "/munisclem/Aire/pm10", "/munisclem/Aire/fecha"],
    "LIA": ["/lia/Aire/tt", "/lia/Aire/hh", "/lia/Aire/pm25", "/lia/Aire/pm10", "/lia/Aire/fecha"],
    "colegio22": ["/colegio22/Aire/tt", "/colegio22/Aire/hh", "/colegio22/Aire/pm25", "/colegio22/Aire/pm10", "/colegio22/Aire/fecha"]
}

# Estructura para almacenar datos temporalmente
data_store = {
    central: {"tt": None, "hh": None, "pm25": None, "pm10": None, "fecha": None}
    for central in centrales
}

def insertar_en_db(central, datos):
    """Inserta una fila en la base de datos MySQL usando pymysql"""
    try:
        conn = pymysql.connect(**MYSQL_CONFIG)
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO mediciones (central, tt, hh, pm25, pm10, fecha)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (central, datos["tt"], datos["hh"], datos["pm25"], datos["pm10"], datos["fecha"]))
        conn.commit()
        cursor.close()
        conn.close()
        print(f" Insertado en DB: {central} - {datos}")
    except pymysql.MySQLError as err:
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
                    if central == "colegio11":
                        nombre_central = "colegio1"
                    elif central == "colegio22":
                        nombre_central = "colegio2"
                    else:
                        nombre_central = "SC"
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
broker = "broker.hivemq.com"
port = 1883

# Inicia el sistema
iniciar_cliente()
