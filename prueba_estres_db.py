import sqlite3
import time
import random

db_path = "/home/admin/Desktop/pm/aire.db"

def crear_tabla():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
        create table if not exists mediciones (
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
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("""
            insert into mediciones (central, tt, hh, pm25, pm10, fecha)
            values (?, ?, ?, ?, ?, ?)
        """, (central, tt, hh, pm25, pm10, fecha))
        conn.commit()
    except sqlite3.Error as e:
        print(f"error al insertar en la base de datos: {e}")
    finally:
        conn.close()

def prueba_estres():
    crear_tabla()
    central = "test_central"
    fecha = time.strftime("%Y-%m-%d %H:%M:%S")
    intentos = 10000
    for i in range(intentos):
        tt = round(random.uniform(15.0, 30.0), 2)
        hh = round(random.uniform(40.0, 90.0), 2)
        pm25 = round(random.uniform(5.0, 50.0), 2)
        pm10 = round(random.uniform(10.0, 100.0), 2)
        insertar_en_db(central, tt, hh, pm25, pm10, fecha)
        if i % 100 == 0:
            print(f"insertados {i} registros exitosamente.")

    print(f"prueba de estres completada: {intentos} registros insertados.")

if __name__ == "__main__":
    inicio = time.time()
    prueba_estres()
    fin = time.time()
    print(f"tiempo total de ejecucion: {fin - inicio:.2f} segundos")
