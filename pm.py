import paho.mqtt.client as mqtt
import sqlite3
import os
import time

db_path = "/home/administrador/pm/aire.db"

centrales = {
    "munisclem": ["/munisclem/Aire/tt", "/munisclem/Aire/hh", "/munisclem/Aire/pm25", "/munisclem/Aire/pm10", "/munisclem/Aire/fecha"],
    "colegio11": ["/colegio11/Aire/tt", "/colegio11/Aire/hh", "/colegio11/Aire/pm25", "/colegio11/Aire/pm10", "/colegio11/Aire/fecha"]
}

data_store = {central: {"tt": None, "hh": None, "pm25": None, "pm10": None, "fecha": None} for central in centrales}

def crear_tabla():
    if not os.path.exists(db_path):
        open(db_path, "w").close()
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
        create table if not exists mediciones (
            id integer primary key autoincrement,
            central text,
            tt real,
            hh real,
            pm25 real,
            pm10 real,
            fecha text
        )
    """)
    conn.commit()
    conn.close()

def insertar_en_db(central, tt, hh, pm25, pm10, fecha):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
        insert into mediciones (central, tt, hh, pm25, pm10, fecha)
        values (?, ?, ?, ?, ?, ?)
    """, (central, tt, hh, pm25, pm10, fecha))
    conn.commit()
    conn.close()
    print(f"insertado en db: central={central}, tt={tt}, hh={hh}, pm25={pm25}, pm10={pm10}, fecha={fecha}")

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        for central, topics in centrales.items():
            for topic in topics:
                client.subscribe(topic)

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
                    insertar_en_db(
                        nombre_central,
                        data_store[central]["tt"],
                        data_store[central]["hh"],
                        data_store[central]["pm25"],
                        data_store[central]["pm10"],
                        data_store[central]["fecha"]
                    )
                    data_store[central] = {key: None for key in data_store[central]}
    except ValueError:
        pass

def iniciar_cliente():
    while True:
        try:
            client = mqtt.Client()
            client.on_connect = on_connect
            client.on_message = on_message
            client.connect(broker, port, 60)
            client.loop_forever()
        except Exception as e:
            print(f"error en la conexion: {e}. reintentando en 5 segundos...")
            time.sleep(5)

broker = "test.mosquitto.org"
port = 1883

crear_tabla()
iniciar_cliente()
