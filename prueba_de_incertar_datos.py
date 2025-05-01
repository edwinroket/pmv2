import sqlite3

# Ruta de la base de datos
db_path = "/home/admin/Desktop/pm/aire.db"

# Datos de prueba
datos_prueba = [
    ("munisclem", 25.5, 60.2, 12.3, 20.8, "2024-12-19 10:00:00"),
    ("colegio1", 27.3, 58.9, 15.1, 22.5, "2024-12-19 11:00:00"),
    ("munisclem", 24.8, 62.5, 10.0, 19.2, "2024-12-19 12:00:00"),
    ("colegio1", 26.1, 59.3, 13.4, 21.0, "2024-12-19 13:00:00")
]

# Funcion para insertar datos de prueba
def insertar_datos_prueba():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.executemany("""
        INSERT INTO mediciones (central, tt, hh, pm25, pm10, fecha)
        VALUES (?, ?, ?, ?, ?, ?)
    """, datos_prueba)
    conn.commit()
    conn.close()

# Ejecutar la funcion
insertar_datos_prueba()
print("Datos de prueba insertados con exito.")
