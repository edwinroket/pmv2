import sqlite3
import pymysql

# Rutas y credenciales
sqlite_path = "/home/administrador/pm/aire.db"
mysql_host = "localhost"
mysql_user = "aireuser"
mysql_pass = "airepassword"
mysql_db   = "aire"

# Conectar a SQLite
conn_sqlite = sqlite3.connect(sqlite_path)
cur_sqlite = conn_sqlite.cursor()

# Conectar a MySQL con PyMySQL
conn_mysql = pymysql.connect(
    host=mysql_host,
    user=mysql_user,
    password=mysql_pass,
    database=mysql_db
)
cur_mysql = conn_mysql.cursor()

# Leer datos desde SQLite
cur_sqlite.execute("SELECT central, tt, hh, pm25, pm10, fecha FROM mediciones")
filas = cur_sqlite.fetchall()

# Insertar en MySQL
insert_sql = """
    INSERT INTO mediciones (central, tt, hh, pm25, pm10, fecha)
    VALUES (%s, %s, %s, %s, %s, %s)
"""

for fila in filas:
    cur_mysql.execute(insert_sql, fila)

conn_mysql.commit()

# Cerrar conexiones
cur_sqlite.close()
conn_sqlite.close()
cur_mysql.close()
conn_mysql.close()

print(f"{len(filas)} filas migradas correctamente de SQLite a MySQL.")

