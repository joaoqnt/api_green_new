from mysql.connector import pooling, Error
import os

# Configuração fixa do banco de dados
dbconfig = {
    "host": "216.238.100.108",
    "database": "green_database",
    "user": "root",
    "password": "#z2W,5jb{#]$m4!q"
}

# Criando um pool de conexões único
connection_pool = pooling.MySQLConnectionPool(
    pool_name="mypool",
    pool_size=20,
    autocommit=True,
    **dbconfig
)


def get_connection(tenant_name):
    if(tenant_name != "green_database"):
        return None
    try:
        connection = connection_pool.get_connection()
        if connection.is_connected():
            return connection
    except Error as e:
        print(f"Erro ao conectar ao MySQL: {e}")
        return None


def create_directory(enterprise):
    diretorio_imagens = f"/var/www/images/{enterprise}"

    if not os.path.exists(diretorio_imagens):
        try:
            os.makedirs(diretorio_imagens)
            print(f"Diretório de imagens criado em: {diretorio_imagens}")
        except OSError as e:
            print(f"Falha ao criar diretório de imagens em: {diretorio_imagens}: {str(e)}")
    else:
        print(f"Diretório de imagens já existe em: {diretorio_imagens}")
