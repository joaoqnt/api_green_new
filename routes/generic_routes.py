from flask import Blueprint, request, jsonify
import os
from services.query_builder import (
    build_select_query,
    build_delete_query,
    build_insert_query,
    build_update_query
)
from services.query_executor import (
    execute_query,
    execute_modify_query,
    execute_insert_returning_id
)

generic_bp = Blueprint('generic', __name__,
                       url_prefix='/api/<tenant_name>/<table_name>')


@generic_bp.route("/", methods=['GET'])
def get_table_results(tenant_name, table_name):
    filters = {}
    order_by = request.args.get("order_by", "").split(
        ",") if "order_by" in request.args else []
    limit = request.args.get("limit")
    offset = request.args.get("offset")

    for key, value in request.args.items():
        if key in ["order_by", "limit", "offset"]:
            continue
        # BETWEEN
        if key == "between":
            try:
                field, range_values = value.split("=")
                start, end = range_values.split(",")
                filters[field.strip("{}")] = ("between", (start, end))
            except:
                return {"error": "Formato inválido para BETWEEN"}, 400
        # IN
        elif key == "in":
            try:
                field, in_values = value.split("=")
                # remove espaços extras e valores duplicados
                lista = list(dict.fromkeys([v.strip()
                             for v in in_values.split(",")]))
                filters[field.strip("{}")] = ("in", lista)
            except:
                return {"error": "Formato inválido para IN"}, 400
        # LIKE
        elif key.startswith("like_"):
            field = key[5:]
            filters[field] = ("like", value)
        # LIKE com LOWER
        elif key.startswith("lower(") and key.endswith(")"):
            field = key[6:-1]
            filters[field] = ("like_lower", value)
        # igualdade simples
        else:
            filters[key] = ("eq", value)

    # Construir query usando placeholders corretos
    stmt, values = build_select_query(
        table_name, filters, order_by, limit, offset)
    return execute_query(tenant_name, stmt, values)


@generic_bp.route("/insert", methods=["POST"])
def insert_table(tenant_name, table_name):
    params = request.get_json()
    stmt, values = build_insert_query(table_name, params)
    print(stmt, values)
    return execute_modify_query(tenant_name, stmt, values)


@generic_bp.route("/insert_and_return_id", methods=["POST"])
def insert_and_return_id(tenant_name, table_name):
    params = request.get_json()
    stmt, values = build_insert_query(table_name, params)
    print(stmt, values)
    return execute_insert_returning_id(tenant_name, stmt, values)


@generic_bp.route("/update", methods=["POST"])
def update_table(tenant_name, table_name):
    params = request.get_json()
    stmt, values = build_update_query(table_name, params)
    print(stmt, values)
    return execute_modify_query(tenant_name, stmt, values)


@generic_bp.route("/delete", methods=["POST"])
def delete_table(tenant_name, table_name):
    params = request.get_json()
    stmt, values = build_delete_query(table_name, params)
    print(stmt, values)
    return execute_modify_query(tenant_name, stmt, values)


@generic_bp.route('/upload', methods=['POST'])
def file_upload(tenant_name, table_name):
    if 'file' not in request.files:
        return jsonify(message="Nenhum arquivo encontrado"), 400

    file = request.files['file']
    nome_empresa = tenant_name

    if file.filename == '':
        return jsonify(message="Arquivo vazio!"), 400

    if file and nome_empresa:
        directory = f"/var/www/images/{nome_empresa}/"
        os.makedirs(directory, exist_ok=True)

        # Caminho completo inicial do arquivo
        filepath = os.path.join(directory, file.filename)

        # Se for do tipo "assinaturas_cliente", evita sobrescrever
        if "assinaturas_cliente" in table_name:
            base_name, ext = os.path.splitext(file.filename)
            count = 1
            while os.path.exists(filepath):
                new_filename = f"{base_name}_{count}{ext}"
                filepath = os.path.join(directory, new_filename)
                count += 1

        try:
            file.save(filepath)
            return jsonify(message="Upload realizado com sucesso!", filename=os.path.basename(filepath)), 200
        except Exception as e:
            print(e)
            return jsonify(message=f"Erro durante o upload: {str(e)}"), 500
    else:
        return jsonify(message="Dados insuficientes fornecidos"), 400


@generic_bp.route('/delete_image', methods=['POST'])
def delete_image(tenant_name, table_name):
    # if 'filename' not in request.form:
    #     return jsonify({"error": "filename not found"}), 400

    body = request.get_json()
    filename = body["filename"]
    nome_empresa = tenant_name

    if filename == '':
        return jsonify({"error": "filename cannot be empty"}), 400

    directory = f"/var/www/images/{nome_empresa}/"
    filepath = os.path.join(directory, filename)

    try:
        # Verifica se o arquivo existe
        if os.path.exists(filepath):
            os.remove(filepath)
            return jsonify({"message": f"Image {filename} deleted successfully!"}), 200
        else:
            return jsonify({"error": "Image not found"}), 404
    except Exception as e:
        return jsonify({"error": f"Error deleting image: {str(e)}"}), 500
