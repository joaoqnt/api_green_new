#!/usr/bin/env python3

from flask import Flask, jsonify
from flask_cors import CORS
import requests

# Import do ProxyFix
from werkzeug.middleware.proxy_fix import ProxyFix

from routes import (
    generic_routes, ordem_servico_routes, grafico_roedores_routes,
    grafico_luminosas_routes, grafico_feromonios_routes,
    osarea_routes, produto_routes, cliente_routes,
    osroedor_routes, osinseto_routes, areaindustrial_routes
)

app = Flask(__name__)

# Aplica o CORS normalmente
cors = CORS(app)

# Ajusta o middleware ProxyFix para lidar com headers de proxy reverso (Nginx + Cloudflare)
# x_proto=1 indica que deve usar o header X-Forwarded-Proto para detectar o esquema (http/https)
# x_host=1 para corrigir o host também via X-Forwarded-Host
app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1)

# Registro dos blueprints
app.register_blueprint(ordem_servico_routes.ordem_servico_bp)
app.register_blueprint(grafico_roedores_routes.roedores_bp)
app.register_blueprint(grafico_luminosas_routes.luminosas_bp)
app.register_blueprint(grafico_feromonios_routes.feromonios_bp)
app.register_blueprint(produto_routes.produto_bp)
app.register_blueprint(cliente_routes.cliente_bp)
app.register_blueprint(osarea_routes.area_bp)
app.register_blueprint(osroedor_routes.roedor_bp)
app.register_blueprint(osinseto_routes.inseto_bp)
app.register_blueprint(areaindustrial_routes.areaindustrial_bp)
app.register_blueprint(generic_routes.generic_bp)


@app.route('/proxy/cnpj/<cpf>', methods=['GET'])
def proxy_cnpj(cpf):
    try:
        api_url = f"https://www.receitaws.com.br/v1/cnpj/{cpf}"
        response = requests.get(api_url)
        return jsonify(response.json())
    except requests.RequestException as e:
        return jsonify({"error": str(e)}), 500


if __name__ == '__main__':
    app.run(debug=True)
