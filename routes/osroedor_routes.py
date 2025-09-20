from flask import Blueprint, request, jsonify
from services.query_executor import execute_query

# Cria o Blueprint para as rotas de roedores
roedor_bp = Blueprint('roedor', __name__, url_prefix='/api/<tenant_name>/osroedor')

@roedor_bp.route("/roedor_os", methods=['GET'])
def get_roedor_os(tenant_name):
    """
    Endpoint que retorna os registros de roedores em OS filtrados por:
    - month: mês da OS (1-12)
    - year: ano da OS
    - acompanhante: identificador do acompanhante
    """
    # Obtém os parâmetros da URL
    year = request.args.get('year')
    month = request.args.get('month')
    acompanhante = request.args.get('acompanhante')

    # Validação dos parâmetros
    if not all([year, month, acompanhante]):
        return jsonify({
            "error": "Parâmetros obrigatórios ausentes",
            "required_params": {
                "year": "YYYY",
                "month": "MM (1-12)",
                "acompanhante": "valor"
            }
        }), 400

    # Consulta SQL
    stmt = """
        SELECT DISTINCT
            r.*
        FROM osroedor r
        INNER JOIN os o ON (o.id = r.os)
        WHERE EXTRACT(MONTH FROM o.data) = %s
          AND EXTRACT(YEAR FROM o.data) = %s
          AND o.servico_acompanhante = %s
    """

    # Executa a consulta usando a função compartilhada
    return execute_query(tenant_name, stmt, (month, year, acompanhante))