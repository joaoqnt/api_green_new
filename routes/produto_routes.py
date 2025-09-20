from flask import Blueprint, request, jsonify
from services.query_executor import execute_query
from datetime import datetime

produto_bp = Blueprint('produto', __name__,url_prefix='/api/<tenant_name>/produto')

@produto_bp.route("/get_products_by_date_and_client", methods=['GET'])
def get_products_by_date_and_client(tenant_name):
    # Obtendo os parâmetros da URL
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    cliente = request.args.get('cliente')

    # Validação das datas
    try:
        start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
        end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
    except (ValueError, TypeError):
        return jsonify({"error": "As datas devem estar no formato 'YYYY-MM-DD'."}), 400

    # Verificação se cliente foi fornecido
    if not cliente:
        return jsonify({"error": "Parâmetro 'cliente' é obrigatório."}), 400

    # Consulta SQL
    stmt = """
        SELECT DISTINCT
            pd.* 
        FROM produto pd
        INNER JOIN os_produto op ON op.produto = pd.id
        INNER JOIN os o ON o.id = op.os AND o.cliente = %s
        WHERE o.data BETWEEN %s AND %s
    """

    values = (cliente, start_date, end_date)
    return execute_query(tenant_name, stmt, values)
