from flask import Blueprint, request, jsonify
from services.query_executor import execute_query
from datetime import datetime

roedores_bp = Blueprint('roedores', __name__, url_prefix='/api/<tenant_name>/grafico_roedores')

@roedores_bp.route("/status_summary", methods=['GET'])
def get_status_summary(tenant_name):
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    cliente = request.args.get('cliente')

    # Validação das datas
    try:
        start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
        end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
    except (ValueError, TypeError):
        return {"error": "As datas devem estar no formato 'YYYY-MM-DD'."}, 400

    # Validação do cliente
    if not cliente:
        return {"error": "Parâmetro 'cliente' é obrigatório."}, 400

    # Query SQL ajustada com LEFT JOIN para PRM
    stmt = """
    SELECT 
        a.nome,
        (SELECT COUNT(*) 
         FROM armadilha_roedor ao 
         WHERE ao.area = a.id 
           AND ao.cliente = o.cliente 
           AND ao.ativo = 'S') AS PRM,
        SUM(CASE WHEN ro.status = 'R' THEN 1 ELSE 0 END) AS R,
        SUM(CASE WHEN ro.status = 'D' THEN 1 ELSE 0 END) AS D,
        SUM(CASE WHEN ro.status = 'A' THEN 1 ELSE 0 END) AS A,
        SUM(CASE WHEN ro.status = 'C' THEN 1 ELSE 0 END) AS C,
        SUM(CASE WHEN ro.status = 'M' THEN 1 ELSE 0 END) AS M
    FROM osroedor ro
    INNER JOIN os o ON o.id = ro.os
    INNER JOIN areaindustrial a ON a.cliente = o.cliente AND a.id = ro.area
    WHERE STR_TO_DATE(o.data, '%Y-%m-%d') BETWEEN %s AND %s
      AND o.cliente = %s
    GROUP BY a.nome, 2
    ORDER BY a.nome;
"""


    values = (start_date, end_date, cliente)
    return execute_query(tenant_name, stmt, values)



@roedores_bp.route("/status_year", methods=['GET'])
def get_status_year(tenant_name):
    start_date = request.args.get('start_date')  # Formato 'YYYY-MM'
    end_date = request.args.get('end_date')      # Formato 'YYYY-MM'
    cliente = request.args.get('cliente')

    # Validação dos parâmetros
    if not start_date or not end_date:
        return jsonify({"error": "Parâmetros 'start_date' e 'end_date' são obrigatórios."}), 400

    if not cliente:
        return jsonify({"error": "Parâmetro 'cliente' é obrigatório."}), 400

    try:
        # Validando formato 'YYYY-MM'
        datetime.strptime(start_date, '%Y-%m')
        datetime.strptime(end_date, '%Y-%m')
    except ValueError:
        return jsonify({"error": "As datas devem estar no formato 'YYYY-MM'."}), 400

    # Consulta SQL com CTE recursivo
    stmt = """
        WITH RECURSIVE meses AS (
            SELECT %s AS mes
            UNION ALL
            SELECT DATE_FORMAT(DATE_ADD(CONCAT(mes, '-01'), INTERVAL 1 MONTH), '%Y-%m')
            FROM meses
            WHERE mes < %s
        )
        SELECT
            m.mes,
            COALESCE((
                CASE 
                    WHEN m.mes < '2025-04' THEN 
                        (SELECT SUM(
                            COALESCE(CAST(p.penser AS UNSIGNED), 0) + 
                            COALESCE(CAST(p.pensem AS UNSIGNED), 0) + 
                            COALESCE(CAST(p.pensec AS UNSIGNED), 0)
                        )
                        FROM pense_monitoramento_roedores p
                        WHERE p.OS_ID IN (
                            SELECT o.id 
                            FROM os o
                            WHERE DATE_FORMAT(STR_TO_DATE(o.data, '%Y-%m-%d'), '%Y-%m') = m.mes
                            AND o.cliente = %s
                        ))
                    ELSE 
                        (SELECT COALESCE(SUM(
                            CASE WHEN ro.status = 'R' THEN 1 ELSE 0 END + 
                            CASE WHEN ro.status = 'C' THEN 1 ELSE 0 END
                        ), 0)
                        FROM osroedor ro
                        INNER JOIN os o ON o.id = ro.os
                        WHERE DATE_FORMAT(STR_TO_DATE(o.data, '%Y-%m-%d'), '%Y-%m') = m.mes
                        AND o.cliente = %s)
                END
            ), 0) AS total_status_R
        FROM meses m
        INNER JOIN cliente c ON c.id = %s
        AND DATE_FORMAT(STR_TO_DATE(c.datacadastro, '%Y-%m-%d'), '%Y-%m') <= m.mes
        ORDER BY m.mes;
    """

    values = (start_date, end_date, cliente, cliente, cliente)
    return execute_query(tenant_name, stmt, values)

