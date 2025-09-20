from flask import Blueprint, request, jsonify
from services.query_executor import execute_query
from datetime import datetime

luminosas_bp = Blueprint('luminosas', __name__, url_prefix='/api/<tenant_name>/grafico_luminoso')

@luminosas_bp.route("/status_luminosas", methods=['GET'])
def get_status_luminosas(tenant_name):
    # Obtendo os parâmetros da URL
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    cliente = request.args.get('cliente')

    # Validação das datas
    try:
        start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
        end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
    except (ValueError, TypeError):
        return {"error": "As datas devem estar no formato 'YYYY-MM-DD'."}, 400

    # Validação do parâmetro cliente
    if not cliente:
        return {"error": "Parâmetro 'cliente' é obrigatório."}, 400

    # Consulta SQL
    stmt = """
        SELECT DISTINCT
            A.nome AS area,
            ro.armadilha AS armadilha,
            SUM(ro.moscas) AS moscas,
            SUM(ro.pernilongos) AS pernilongos,
            SUM(ro.besouros) AS besouros,
            SUM(ro.outrosluminosas) AS outrosluminosas
        FROM osinseto ro
        INNER JOIN os o ON o.id = ro.os
        INNER JOIN areaindustrial A ON A.cliente = o.cliente AND A.id = ro.area AND A.grafico_insetos = 'S'
        INNER JOIN cliente c ON c.id = o.cliente
        INNER JOIN armadilha_inseto ai ON ai.id = ro.armadilha AND ai.cliente = o.cliente AND ai.area = A.id
        WHERE STR_TO_DATE(o.data, '%Y-%m-%d') BETWEEN %s AND %s
          AND o.cliente = %s
          AND ai.tipo = 'luminosa'
        GROUP BY A.nome, ro.armadilha, ai.tipo;
    """

    values = (start_date, end_date, cliente)
    return execute_query(tenant_name, stmt, values)

@luminosas_bp.route("/status_luminosas_mensal", methods=['GET'])
def get_status_luminosas_mensal(tenant_name):
    # Obtendo os parâmetros da URL
    start_date = request.args.get('start_date')  # Formato 'YYYY-MM'
    end_date = request.args.get('end_date')      # Formato 'YYYY-MM'
    cliente = request.args.get('cliente')

    # Validação dos parâmetros obrigatórios
    if not start_date or not end_date:
        return {"error": "Parâmetros 'start_date' e 'end_date' são obrigatórios."}, 400
    if not cliente:
        return {"error": "Parâmetro 'cliente' é obrigatório."}, 400

    # Validação do formato das datas
    try:
        datetime.strptime(start_date, '%Y-%m')
        datetime.strptime(end_date, '%Y-%m')
    except ValueError:
        return {"error": "As datas devem estar no formato 'YYYY-MM'."}, 400

    # Consulta SQL
    stmt = f"""
        WITH RECURSIVE meses AS (
            SELECT '{start_date}' AS mes
            UNION ALL
            SELECT DATE_FORMAT(DATE_ADD(CONCAT(mes, '-01'), INTERVAL 1 MONTH), '%Y-%m')
            FROM meses
            WHERE mes < '{end_date}'
        )
        SELECT
            m.mes,
            a.nome AS area,
            COALESCE(SUM(ro.moscas), 0) AS total_moscas,
            COALESCE(SUM(ro.pernilongos), 0) AS total_pernilongos,
            COALESCE(SUM(ro.besouros), 0) AS total_besouros,
            COALESCE(SUM(ro.outrosluminosas), 0) AS total_outrosluminosas
        FROM meses m
        LEFT JOIN os o ON DATE_FORMAT(STR_TO_DATE(o.data, '%Y-%m-%d'), '%Y-%m') = m.mes
        LEFT JOIN osinseto ro ON o.id = ro.os
        LEFT JOIN areaindustrial a ON a.cliente = o.cliente AND a.id = ro.area AND a.grafico_insetos = 'S'
        INNER JOIN armadilha_inseto ai ON ai.id = ro.armadilha AND ai.cliente = o.cliente AND ai.area = a.id
        INNER JOIN cliente c ON c.id = %s AND DATE_FORMAT(STR_TO_DATE(c.datacadastro, '%Y-%m-%d'), '%Y-%m') <= m.mes
        WHERE o.cliente = %s
        AND ai.tipo = 'luminosa'
        GROUP BY m.mes, a.nome
        ORDER BY m.mes;
    """

    values = (cliente, cliente)
    return execute_query(tenant_name, stmt, values)



@luminosas_bp.route("/status_feromonios", methods=['GET'])
def get_status_feromonios(tenant_name, table_name):
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    cliente = request.args.get('cliente')

    try:
        start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
        end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
    except (ValueError, TypeError):
        return jsonify({"error": "As datas devem estar no formato 'YYYY-MM-DD'."}), 400

    if not cliente:
        return jsonify({"error": "Parâmetro 'cliente' é obrigatório."}), 400

    stmt = """
        SELECT DISTINCT
            A.nome AS area,
            ro.armadilha,
            SUM(ro.sitophilus) AS sitophilus,
            SUM(ro.rhizopertha) AS rhizopertha,
            SUM(ro.tribolium) AS tribolium,
            SUM(ro.lasioderma) AS lasioderma,
            SUM(ro.sitotroga) AS sitotroga,
            SUM(ro.ephestia) AS ephestia,
            SUM(ro.plodia) AS plodia,
            SUM(ro.periplanetaamericana) AS periplanetaamericana,
            SUM(ro.outrosferomonio) AS outrosferomonio,
            SUM(ro.battelagermanica) AS battelagermanica
        FROM osinseto ro
        INNER JOIN os o ON o.id = ro.os
        INNER JOIN areaindustrial A ON A.cliente = o.cliente AND A.id = ro.area AND A.grafico_insetos = 'S'
        INNER JOIN cliente c ON c.id = o.cliente
        INNER JOIN armadilha_inseto ai ON ai.id = ro.armadilha AND ai.cliente = o.cliente AND ai.area = A.id
        WHERE STR_TO_DATE(o.data, '%Y-%m-%d') BETWEEN %s AND %s
          AND o.cliente = %s
          AND ai.tipo = 'feromonio'
        GROUP BY A.nome, ro.armadilha, ai.tipo;
    """
    values = (start_date, end_date, cliente)
    return execute_query(tenant_name, stmt, values)
