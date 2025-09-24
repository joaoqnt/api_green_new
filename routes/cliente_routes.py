from flask import Blueprint, request, jsonify
from services.query_executor import execute_query

cliente_bp = Blueprint('cliente', __name__,
                       url_prefix='/api/<tenant_name>/cliente')


@cliente_bp.route("/login_cliente", methods=['GET'])
def login_cliente(tenant_name):
    email = request.args.get('email')
    senha = request.args.get('senha')

    # Validação dos parâmetros
    if not email or not senha:
        return jsonify({"error": "Parâmetros 'email' e 'senha' são obrigatórios."}), 400

    # Consulta SQL
    stmt = """
        SELECT 
            c.*,
            ce.nome as nome_email,
            ce.data_validacao as data_validacao
        FROM cliente c
        INNER JOIN cliente_email ce ON ce.cliente = c.id
        WHERE ce.email = %s AND ce.senha = %s
    """

    values = (email, senha)
    return execute_query(tenant_name, stmt, values)


@cliente_bp.route("/cliente_os", methods=['GET'])
def get_clientes_os(tenant_name):
    # Obtendo os parâmetros da URL
    year = request.args.get('year')
    month = request.args.get('month')
    acompanhante = request.args.get('acompanhante')

    # Validação dos parâmetros
    if not year or not month or not acompanhante:
        return jsonify({"error": "Parâmetros 'year', 'month' e 'acompanhante' são obrigatórios."}), 400

    # Consulta SQL
    stmt = """
        SELECT DISTINCT
            c.*
        FROM cliente c
        INNER JOIN os o ON (o.cliente = c.id)
        WHERE EXTRACT(MONTH FROM o.data) = %s
            AND EXTRACT(YEAR FROM o.data) = %s
            AND o.servico_acompanhante = %s
    """

    values = (month, year, acompanhante)
    return execute_query(tenant_name, stmt, values)
