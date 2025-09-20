from flask import jsonify
from database import database_manager
from utils.format_record import format_record

def execute_query(tenant_name, stmt, values=None):
    connection = database_manager.get_connection(tenant_name)
    if connection == -1:
        return jsonify({tenant_name: "erro ao reconhecer empresa"}), 404

    try:
        cursor = connection.cursor()
        cursor.execute(stmt, values or [])
        results = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]

        response = [format_record(dict(zip(column_names, row))) for row in results]
        return jsonify(response)

    except Exception as e:
        return jsonify({"error": str(e)}), 500

    finally:
        cursor.close()
        connection.close()

def execute_modify_query(tenant_name, stmt, values):
    connection = database_manager.get_connection(tenant_name)
    if connection == -1:
        return jsonify({"error": f"{tenant_name} não encontrado"}), 404
    try:
        cursor = connection.cursor()
        cursor.execute(stmt, values)
        connection.commit()
        cursor.close()
        connection.close()
        return jsonify({"response": "operação realizada com sucesso"})
    except Exception as e:
        cursor.close()
        connection.close()
        return jsonify({"error": str(e)}), 500


def execute_insert_returning_id(tenant_name, stmt, values):
    connection = database_manager.get_connection(tenant_name)
    if connection == -1:
        return jsonify({"error": f"{tenant_name} não encontrado"}), 404
    try:
        cursor = connection.cursor()
        cursor.execute(stmt, values)
        connection.commit()
        last_id = cursor.lastrowid
        cursor.close()
        connection.close()
        return jsonify({"response": "inserido com sucesso", "id": last_id})
    except Exception as e:
        cursor.close()
        connection.close()
        return jsonify({"error": str(e)}), 500