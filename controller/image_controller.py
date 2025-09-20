from rembg import remove
from PIL import Image, ImageColor
from flask import request, Blueprint, jsonify
import io
import base64
import os
import subprocess

image_bp = Blueprint(
    'image', __name__, url_prefix='/api/<tenant_name>/<table_name>')


@image_bp.route('/upload', methods=['POST'])
def file_upload(tenant_name, table_name):
    # Verifica se há um arquivo no pedido
    if 'file' not in request.files:
        return jsonify(message="Nenhum arquivo encontrado"), 400

    file = request.files['file']
    nome_empresa = tenant_name

    if file.filename == '':
        return jsonify(message="Arquivo vazio!"), 400

    if file and nome_empresa:
        directory = f"/var/www/images/{nome_empresa}/"
        os.makedirs(directory, exist_ok=True)

        try:
            filepath = os.path.join(directory, file.filename)
            file.save(filepath)

            process = subprocess.run(['chmod', '-R', '777', directory],
                                     stdout=subprocess.PIPE,
                                     stderr=subprocess.PIPE)
            if process.returncode == 0:
                print(
                    f"Permissões definidas com sucesso para o diretório {directory}")
            else:
                print(
                    f"Erro ao definir permissões para o diretório {directory}: {process.stderr.decode()}")
                return jsonify(message="Erro ao definir permissões para o diretório"), 500

            return jsonify(message="Upload realizado com sucesso!"), 200
        except Exception as e:
            return jsonify(message=f"Erro durante o upload: {str(e)}"), 500
    else:
        return jsonify(message="Dados insuficientes fornecidos"), 400


@image_bp.route('/delete', methods=['POST'])
def delete_image(tenant_name, table_name):
    # if 'filename' not in request.form:
    #     return jsonify({"error": "filename not found"}), 400

    body =  request.get_json()
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
