from flask import Blueprint, request, jsonify
from services.query_executor import execute_query

# Cria o Blueprint para as rotas de área
area_bp = Blueprint('area', __name__, url_prefix='/api/<tenant_name>/osarea')

@area_bp.route("/area_os", methods=['GET'])
def get_area_os(tenant_name):
    """
    Endpoint que retorna as áreas de OS filtradas por mês, ano e acompanhante
    """
    # Obtém os parâmetros da URL
    year = request.args.get('year')
    month = request.args.get('month')
    acompanhante = request.args.get('acompanhante')

    # Validação dos parâmetros
    if not all([year, month, acompanhante]):
        return jsonify({
            "error": "Parâmetros obrigatórios ausentes",
            "required_params": ["year", "month", "acompanhante"]
        }), 400

    # Consulta SQL
    stmt = """
        SELECT DISTINCT
            a.*
        FROM os_area a
        INNER JOIN os o ON (o.id = a.os)
        WHERE EXTRACT(MONTH FROM o.data) = %s
            AND EXTRACT(YEAR FROM o.data) = %s
            AND o.servico_acompanhante = %s
    """

    # Executa a consulta usando a função compartilhada
    return execute_query(tenant_name, stmt, (month, year, acompanhante))