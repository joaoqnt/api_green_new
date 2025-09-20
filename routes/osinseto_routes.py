from flask import Blueprint, request, jsonify
from services.query_executor import execute_query

# Cria o Blueprint para as rotas de insetos
inseto_bp = Blueprint('inseto', __name__, url_prefix='/api/<tenant_name>/osinseto')

@inseto_bp.route("/inseto_os", methods=['GET'])
def get_inseto_os(tenant_name):
    """
    Endpoint que retorna os registros de insetos em OS filtrados por:
    - month: mês da OS (1-12)
    - year: ano da OS
    - acompanhante: identificador do acompanhante
    
    Retorna:
    - Lista de registros de insetos com todas as colunas da tabela osinseto
    """
    # Obtém os parâmetros da URL
    year = request.args.get('year')
    month = request.args.get('month')
    acompanhante = request.args.get('acompanhante')

    # Validação dos parâmetros
    if None in (year, month, acompanhante):
        return jsonify({
            "error": "Parâmetros obrigatórios ausentes",
            "required_params": [
                {"name": "year", "type": "integer", "description": "Ano da OS"},
                {"name": "month", "type": "integer", "description": "Mês da OS (1-12)"},
                {"name": "acompanhante", "type": "string", "description": "Identificador do acompanhante"}
            ]
        }), 400

    # Consulta SQL
    stmt = """
        SELECT DISTINCT
            i.*
        FROM osinseto i
        INNER JOIN os o ON (o.id = i.os)
        WHERE EXTRACT(MONTH FROM o.data) = %s
          AND EXTRACT(YEAR FROM o.data) = %s
          AND o.servico_acompanhante = %s
    """

    # Executa a consulta usando a função compartilhada
    return execute_query(tenant_name, stmt, (month, year, acompanhante))