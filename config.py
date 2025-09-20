import mysql.connector
from mysql.connector import Error

dbconfig = {
    "host": "localhost",
    "database": "green_database",
    "user": "root",
    "password": "Senha123***"
}

connection_pool = mysql.connector.pooling.MySQLConnectionPool(
    pool_name="mypool",
    pool_size=20,
    **dbconfig
)

def get_connection():
    try:
        connection = connection_pool.get_connection()
        if connection.is_connected():
            return connection
    except Error as e:
        print(f"Erro ao conectar ao MySQL: {e}")
        return None
