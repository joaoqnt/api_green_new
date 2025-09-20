import secrets
from config import get_connection
import bcrypt


def generate_token():
    return secrets.token_hex(16)


def register_user(nome, email, senha, documento, perfil, ativo, celular):
    connection = get_connection()
    cursor = connection.cursor()

    password_hash = bcrypt.hashpw(senha.encode('utf-8'), bcrypt.gensalt())
    token = generate_token()

    cursor.execute(
        """
        INSERT INTO usuario (nome, email, senha, documento,
                       perfil, ativo, data_registro , celular)
        VALUES (%s, %s, %s, %s, %s, %s, NOW(), %s)
        """,
        (nome, email, password_hash, documento, perfil,
         ativo, celular)
    )
    connection.commit()
    cursor.close()
    connection.close()

    return {"message": "Usuário registrado com sucesso", "token": token}

def verify_token(token):
    connection = get_connection()
    cursor = connection.cursor()

    cursor.execute("SELECT id FROM user WHERE token = %s", (token,))
    result = cursor.fetchone()
    cursor.close()
    connection.close()

    if result:
        return result[0]
    else:
        return None

def authenticate_user(email, password):
    connection = get_connection()
    cursor = connection.cursor()

    cursor.execute(
        "SELECT * FROM user WHERE email = %s", (email,))
    result = cursor.fetchone()

    if result:
        user_id, name, email, document, profile, actived, description, cellphone, photo_url, stored_password_hash,date_register, token = result
        if bcrypt.checkpw(password.encode('utf-8'), stored_password_hash.encode('utf-8')):
            connection.commit()
            cursor.close()
            connection.close()
            user_info = {
                "id": user_id,
                "name": name,
                "email": email,
                "document": document,
                "profile": profile,
                "actived": actived,
                "description": description,
                "cellphone": cellphone,
                "photo_url": photo_url,
                "date_register":date_register,
                "token": token
            }
            return {"user": user_info}, 200
        else:
            cursor.close()
            connection.close()
            return {"message": "Senha incorreta"}, 401
    else:
        cursor.close()
        connection.close()
        return {"message": "Usuário não encontrado"}, 404
    
def fetch_google_account(email):
    connection = get_connection()
    cursor = connection.cursor()

    cursor.execute(
        "SELECT id, name, email, document, profile, actived, description, cellphone, photo_url, token FROM user WHERE email = %s", (email,))
    result = cursor.fetchone()

    if result:
        user_id, name, email, document, profile, actived, description, cellphone, photo_url, token = result
        connection.commit()
        cursor.close()
        connection.close()
        user_info = {
            "id": user_id,
            "name": name,
            "email": email,
            "document": document,
            "profile": profile,
            "actived": actived,
            "description": description,
            "cellphone": cellphone,
            "photo_url": photo_url,
            "token": token
        }
        return {"user": user_info}, 200
        
    else:
        cursor.close()
        connection.close()
        return {"message": "Usuário não encontrado"}, 404