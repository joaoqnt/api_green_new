from flask import request, jsonify
from flask import Blueprint, jsonify, request
from datetime import datetime
from database.database_manager import get_connection
import os
import subprocess


generic_bp = Blueprint('generic', __name__,
                       url_prefix='/api/<tenant_name>/<table_name>')


@generic_bp.route("/allOs", methods=['GET'])
def get_os_join_clients(tenant_name, table_name):
    stmt = """SELECT o.*, c.nome, 
                     GROUP_CONCAT(ae.nome ORDER BY ae.nome SEPARATOR ', ') AS areas 
              FROM os o 
              INNER JOIN cliente c ON c.id = o.cliente 
              INNER JOIN os_area oa ON oa.os = o.id 
              INNER JOIN areaindustrial ae ON ae.id = oa.area AND ae.cliente = c.id"""

    where_clauses = []
    order_clauses = []
    limit_clauses = []
    offset_clauses = []
    between_clause = None  
    between_values = []  
    in_clause = None  
    in_values = []  

    connection = get_connection(tenant_name)
    if connection == -1:
        return jsonify({tenant_name: "erro ao reconhecer empresa"}), 404

    param = request.args
    for key, value in param.items():
        if key == "between":
            try:
                between_expr, between_values_str = value.split('=')
                field_expression = between_expr.strip('{}')  
                start_value, end_value = between_values_str.split(',')

                between_clause = f"{field_expression} BETWEEN %s AND %s"
                between_values = [start_value, end_value]
            except ValueError:
                return jsonify({"error": "Formato inválido para BETWEEN"}), 400

        elif key == "in":
            try:
                in_expr, in_values_str = value.split('=')
                field_expression = in_expr.strip('{}')  
                in_values_list = in_values_str.split(',')

                in_clause = f"{field_expression} IN ({', '.join(['%s'] * len(in_values_list))})"
                in_values = in_values_list
            except ValueError:
                return jsonify({"error": "Formato inválido para IN"}), 400

        elif key not in ("order_by", "limit", "offset"):
            where_clauses.append(f"{key} = '{value}'")
        else:
            if key == "order_by":
                order_clauses.extend(value.split(","))
            elif key == "limit":
                limit_clauses.append(f" LIMIT {value} ")
            elif key == "offset":
                offset_clauses.append(f" OFFSET {value} ")

    # Constrói a cláusula WHERE
    if where_clauses:
        stmt += " WHERE " + " AND ".join(where_clauses)

    # Adiciona a cláusula BETWEEN
    if between_clause:
        if where_clauses:
            stmt += " AND " + between_clause
        else:
            stmt += " WHERE " + between_clause

    # Adiciona a cláusula IN
    if in_clause:
        if where_clauses or between_clause:
            stmt += " AND " + in_clause
        else:
            stmt += " WHERE " + in_clause

    # Adiciona GROUP BY na posição correta
    stmt += " GROUP BY o.id, c.nome"

    # Adiciona cláusulas ORDER BY, LIMIT e OFFSET
    if order_clauses:
        stmt += " ORDER BY " + ", ".join(order_clauses)

    if limit_clauses:
        stmt += " ".join(limit_clauses)
    if offset_clauses:
        stmt += " ".join(offset_clauses)

    print(stmt)  

    try:
        cursor = connection.cursor()
        cursor.execute(stmt, between_values + in_values)
        results = cursor.fetchall()

        column_names = [desc[0] for desc in cursor.description]

        cursor.close()
        connection.close()

        response = [format_record(dict(zip(column_names, row))) for row in results]
        return jsonify(response)

    except Exception as e:
        cursor.close()
        connection.close()
        return jsonify({"error": str(e)}), 500



@generic_bp.route("", methods=['GET'])
def get_table_results(tenant_name, table_name):
    stmt = f"SELECT * FROM {table_name}"
    where_clauses = []
    order_clauses = []
    limit_clauses = []
    offset_clauses = []
    between_clause = None  # Para armazenar a cláusula BETWEEN
    between_values = []  # Para armazenar os valores do BETWEEN
    in_clause = None  # Para armazenar a cláusula IN
    in_values = []  # Para armazenar os valores do IN

    connection = get_connection(tenant_name)
    if connection == -1:
        return jsonify({tenant_name: "erro ao reconhecer empresa"}), 404

    param = request.args
    for key, value in param.items():
        if key == "between":
            try:
                between_expr, between_values_str = value.split('=')
                field_expression = between_expr.strip('{}')  # Remove as chaves
                start_value, end_value = between_values_str.split(',')

                between_clause = f"{field_expression} BETWEEN %s AND %s"
                between_values = [start_value, end_value]
            except ValueError:
                return jsonify({"error": "Formato inválido para BETWEEN"}), 400

        elif key == "in":
            try:
                in_expr, in_values_str = value.split('=')
                field_expression = in_expr.strip('{}')  # Remove as chaves
                in_values_list = in_values_str.split(',')

                in_clause = f"{field_expression} IN ({', '.join(['%s'] * len(in_values_list))})"
                in_values = in_values_list
            except ValueError:
                return jsonify({"error": "Formato inválido para IN"}), 400

        elif key not in ("order_by", "limit", "offset"):
            where_clauses.append(f"{key} = '{value}'")
        else:
            if key == "order_by":
                order_clauses.extend(value.split(","))
            elif key == "limit":
                limit_clauses.append(f" LIMIT {value} ")
            elif key == "offset":
                offset_clauses.append(f" OFFSET {value} ")

    # Constrói a cláusula WHERE
    if where_clauses:
        stmt += " WHERE " + " AND ".join(where_clauses)

    # Adiciona a cláusula BETWEEN
    if between_clause:
        if where_clauses:
            stmt += " AND " + between_clause
        else:
            stmt += " WHERE " + between_clause

    # Adiciona a cláusula IN
    if in_clause:
        if where_clauses or between_clause:
            stmt += " AND " + in_clause
        else:
            stmt += " WHERE " + in_clause

    # Adiciona cláusulas ORDER BY, LIMIT e OFFSET
    if order_clauses:
        stmt += " ORDER BY " + ", ".join(order_clauses)

    if limit_clauses:
        stmt += " ".join(limit_clauses)
    if offset_clauses:
        stmt += " ".join(offset_clauses)

    print(stmt)  # Apenas para depuração

    try:
        cursor = connection.cursor()
        # Executa a query com os valores do BETWEEN e IN
        cursor.execute(stmt, between_values + in_values)
        print(stmt, between_values + in_values)
        results = cursor.fetchall()

        column_names = [desc[0] for desc in cursor.description]

        cursor.close()
        connection.close()

        response = [format_record(dict(zip(column_names, row)))
                    for row in results]
        return jsonify(response)

    except Exception as e:
        cursor.close()
        connection.close()
        return jsonify({"error": str(e)}), 500


@generic_bp.route("/like", methods=['GET'])
def get_table_results_like(tenant_name, table_name):
    stmt = f"SELECT * FROM {table_name}"
    where_clauses = []
    order_clauses = []
    limit_clauses = []
    offset_clauses = []

    connection = get_connection(tenant_name)
    if connection == -1:
        return jsonify({tenant_name: "erro ao reconhecer empresa"}), 404

    param = request.args
    for key, value in param.items():
        if key not in ("order_by", "limit", "offset"):
            # Utiliza LIKE ao invés de =
            where_clauses.append(f"{key} LIKE '%{value}%'")
        else:
            if key == "order_by":
                order_clauses.extend(value.split(","))
            elif key == "limit":
                limit_clauses.append(f"{key} {value}")
            elif key == "offset":
                offset_clauses.append(f"{key} {value}")

    if where_clauses:
        stmt += " WHERE " + " AND ".join(where_clauses)

    if order_clauses:
        stmt += " ORDER BY " + ", ".join(order_clauses)

    if limit_clauses:
        stmt += " " + " ".join(limit_clauses)
    if offset_clauses:
        stmt += " " + " ".join(offset_clauses)

    print(stmt)  # Apenas para depuração

    try:
        cursor = connection.cursor()
        cursor.execute(stmt)
        results = cursor.fetchall()

        # Obtendo os nomes das colunas
        column_names = [desc[0] for desc in cursor.description]

        # Fechando cursor e conexão
        cursor.close()
        connection.close()

        # Montando a resposta com nomes das colunas e formatando datas
        response = [format_record(dict(zip(column_names, row)))
                    for row in results]
        return jsonify(response)

    except Exception as e:
        # Tratamento de erros, se necessário
        cursor.close()
        connection.close()
        return jsonify({"error": str(e)}), 500


@generic_bp.route("/between", methods=['GET'])
def get_table_with_between(tenant_name, table_name):
    stmt = f"SELECT * FROM {table_name}"
    between_clauses = []
    order_clauses = []
    values = []

    connection = get_connection(tenant_name)
    if connection == -1:
        return jsonify({tenant_name: "erro ao reconhecer empresa"}), 404

    param = request.args
    for key, value in param.items():
        if key == "order_by":
            order_clauses.extend(value.split(","))
        else:
            if "," in value:
                range_values = value.split(",")
                if len(range_values) == 2:
                    between_clauses.append(f"{key} BETWEEN %s AND %s")
                    values.extend(range_values)
                else:
                    return jsonify({"error": f"Valor inválido para {key} com BETWEEN. Use 'valor1,valor2'."}), 400
            else:
                return jsonify({"error": f"Parâmetro '{key}' não é válido para BETWEEN. Use 'campo=valor1,valor2'."}), 400

    if between_clauses:
        stmt += " WHERE " + " AND ".join(between_clauses)

    if order_clauses:
        stmt += " ORDER BY " + ", ".join(order_clauses)

    print(stmt)  # Apenas para depuração

    try:
        cursor = connection.cursor()
        cursor.execute(stmt, values)
        results = cursor.fetchall()

        # Obtendo os nomes das colunas
        column_names = [desc[0] for desc in cursor.description]

        # Fechando cursor e conexão
        cursor.close()
        connection.close()

        # Montando a resposta com nomes das colunas e formatando datas
        response = [format_record(dict(zip(column_names, row)))
                    for row in results]
        return jsonify(response)

    except Exception as e:
        # Tratamento de erros, se necessário
        cursor.close()
        connection.close()
        return jsonify({"error": str(e)}), 500


@generic_bp.route("/recent_clients", methods=['GET'])
def get_recent_clients(tenant_name, table_name):
    stmt = """
        SELECT DISTINCT * FROM cliente c
        JOIN (
            SELECT cliente 
            FROM os 
            ORDER BY data DESC 
            LIMIT 200
        ) AS recent_clients ON c.id = recent_clients.cliente
        order by c.nome
    """

    connection = get_connection(tenant_name)
    if connection == -1:
        return jsonify({tenant_name: "erro ao reconhecer empresa"}), 404

    try:
        cursor = connection.cursor()
        cursor.execute(stmt)
        results = cursor.fetchall()

        # Obtendo os nomes das colunas
        column_names = [desc[0] for desc in cursor.description]

        # Fechando cursor e conexão
        cursor.close()
        connection.close()

        # Montando a resposta com nomes das colunas e formatando datas
        response = [format_record(dict(zip(column_names, row)))
                    for row in results]
        return jsonify(response)

    except Exception as e:
        cursor.close()
        connection.close()
        return jsonify({"error": str(e)}), 500


@generic_bp.route("/in", methods=['GET'])
def get_table_with_in(tenant_name, table_name):
    stmt = f"SELECT * FROM {table_name}"
    in_clauses = []
    order_clauses = []
    values = []

    connection = get_connection(tenant_name)
    if connection == -1:
        return jsonify({tenant_name: "erro ao reconhecer empresa"}), 404

    param = request.args
    for key, value in param.items():
        if key == "order_by":
            order_clauses.extend(value.split(","))
        else:
            if "," in value:
                in_values = value.split(",")
                placeholders = ",".join(["%s"] * len(in_values))
                in_clauses.append(f"{key} IN ({placeholders})")
                values.extend(in_values)
            else:
                return jsonify({"error": f"Parâmetro '{key}' não é válido para IN. Use 'campo=valor1,valor2,valor3'."}), 400

    if in_clauses:
        stmt += " WHERE " + " AND ".join(in_clauses)

    if order_clauses:
        stmt += " ORDER BY " + ", ".join(order_clauses)

    print(stmt)  # Apenas para depuração

    try:
        cursor = connection.cursor()
        cursor.execute(stmt, values)
        results = cursor.fetchall()

        # Obtendo os nomes das colunas
        column_names = [desc[0] for desc in cursor.description]

        # Fechando cursor e conexão
        cursor.close()
        connection.close()

        # Montando a resposta com nomes das colunas e formatando datas
        response = [format_record(dict(zip(column_names, row)))
                    for row in results]
        return jsonify(response)

    except Exception as e:
        # Tratamento de erros, se necessário
        cursor.close()
        connection.close()
        return jsonify({"error": str(e)}), 500


@generic_bp.route("/status_summary", methods=['GET'])
def get_status_summary(tenant_name, table_name):
    # Obtendo as datas e cliente dos parâmetros da URL
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    cliente = request.args.get('cliente')

    # Validação das datas
    try:
        start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
        end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
    except (ValueError, TypeError):
        return jsonify({"error": "As datas devem estar no formato 'YYYY-MM-DD'."}), 400

    # Validação do parâmetro cliente
    if not cliente:
        return jsonify({"error": "Parâmetro 'cliente' é obrigatório."}), 400

    # Construindo a consulta SQL com parâmetros de data e cliente
    stmt = """
        SELECT 
            a.nome,
            (SELECT COUNT(*) FROM armadilha_roedor ao WHERE ao.area = a.id AND ao.cliente = o.cliente AND ao.ativo = 'S') AS PRM,
            SUM(CASE WHEN ro.status = 'R' THEN 1 ELSE 0 END) AS R,
            SUM(CASE WHEN ro.status = 'D' THEN 1 ELSE 0 END) AS D,
            SUM(CASE WHEN ro.status = 'A' THEN 1 ELSE 0 END) AS A,
            SUM(CASE WHEN ro.status = 'C' THEN 1 ELSE 0 END) AS C,
            COALESCE((SELECT SUM(oa.roedor_morto) 
                     FROM os_area oa 
                     WHERE oa.area = a.id 
                     AND oa.os IN (SELECT id FROM os 
                                   WHERE cliente = %s 
                                   AND STR_TO_DATE(data, '%Y-%m-%d') BETWEEN %s AND %s)), 0) AS M
        FROM osroedor ro
        INNER JOIN os o ON o.id = ro.os
        INNER JOIN areaindustrial a ON a.cliente = o.cliente AND a.id = ro.area
        WHERE STR_TO_DATE(o.data, '%Y-%m-%d') BETWEEN %s AND %s
          AND o.cliente = %s
        GROUP BY a.id, a.nome
        ORDER BY a.nome;
    """

    connection = get_connection(tenant_name)
    if connection == -1:
        return jsonify({tenant_name: "Erro ao reconhecer empresa"}), 404

    try:
        cursor = connection.cursor()
        # Executa a consulta com os parâmetros (cliente, start_date, end_date repetidos para o subselect)
        cursor.execute(stmt, (cliente, start_date, end_date, start_date, end_date, cliente))
        results = cursor.fetchall()

        # Obtendo os nomes das colunas
        column_names = [desc[0] for desc in cursor.description]

        # Fechando cursor e conexão
        cursor.close()
        connection.close()

        # Montando a resposta com nomes das colunas
        response = [dict(zip(column_names, row)) for row in results]
        return jsonify(response)

    except Exception as e:
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals():
            connection.close()
        return jsonify({"error": str(e)}), 500


@generic_bp.route("/status_luminosas", methods=['GET'])
def get_status_luminosas(tenant_name, table_name):
    # Obtendo as datas e cliente dos parâmetros da URL
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    cliente = request.args.get('cliente')

    # Validação das datas
    try:
        start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
        end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
    except (ValueError, TypeError):
        return jsonify({"error": "As datas devem estar no formato 'YYYY-MM-DD'."}), 400

    # Validação do parâmetro cliente
    if not cliente:
        return jsonify({"error": "Parâmetro 'cliente' é obrigatório."}), 400

    # Construindo a consulta SQL com parâmetros de data e cliente
    stmt = """
        SELECT distinct
            A.nome area,
            ro.armadilha armadilha,
            SUM(ro.moscas) AS moscas,
            SUM(ro.pernilongos) AS pernilongos,
            SUM(ro.besouros) AS besouros,
            SUM(ro.outrosluminosas) AS outrosluminosas/*,
            SUM(ro.sitophilus) AS sitophilus,
            SUM(ro.rhizopertha) AS rhizopertha,
            SUM(ro.tribolium) AS tribolium,
            SUM(ro.lasioderma) AS lasioderma,
            SUM(ro.sitotroga) AS sitotroga,
            SUM(ro.ephestia) AS ephestia,
            SUM(ro.ephestia) AS ephestia,
            SUM(ro.ephestia) AS ephestia,
            SUM(ro.ephestia) AS ephestia,*/
        FROM osinseto ro
            INNER JOIN os o ON o.id = ro.os
            INNER JOIN areaindustrial A ON (A.cliente = o.cliente and A.id = ro.area and A.grafico_insetos = 'S')
            inner join cliente c on c.id = o.cliente
            INNER JOIN armadilha_inseto ai ON ai.id = ro.armadilha AND ai.cliente = o.cliente AND ai.area = A.id
        WHERE STR_TO_DATE(o.data, '%Y-%m-%d') BETWEEN %s AND %s
            AND o.cliente = %s
            and ai.tipo = 'luminosa'
        group by A.nome, ro.armadilha, ai.tipo;
    """

    connection = get_connection(tenant_name)
    if connection == -1:
        return jsonify({tenant_name: "Erro ao reconhecer empresa"}), 404

    try:
        cursor = connection.cursor()
        # Executa a consulta com os parâmetros start_date, end_date e cliente
        cursor.execute(stmt, (start_date, end_date, cliente))
        results = cursor.fetchall()

        # Obtendo os nomes das colunas
        column_names = [desc[0] for desc in cursor.description]

        # Fechando cursor e conexão
        cursor.close()
        connection.close()

        # Montando a resposta com nomes das colunas
        response = [dict(zip(column_names, row)) for row in results]
        return jsonify(response)

    except Exception as e:
        cursor.close()
        connection.close()
        return jsonify({"error": str(e)}), 500
    
@generic_bp.route("/status_feromonios", methods=['GET'])
def get_status_feromonios(tenant_name, table_name):
    # Obtendo as datas e cliente dos parâmetros da URL
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    cliente = request.args.get('cliente')

    # Validação das datas
    try:
        start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
        end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
    except (ValueError, TypeError):
        return jsonify({"error": "As datas devem estar no formato 'YYYY-MM-DD'."}), 400

    # Validação do parâmetro cliente
    if not cliente:
        return jsonify({"error": "Parâmetro 'cliente' é obrigatório."}), 400

    # Construindo a consulta SQL com parâmetros de data e cliente
    stmt = """
        SELECT distinct
            A.nome area,
            ro.armadilha armadilha,
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
            INNER JOIN areaindustrial A ON (A.cliente = o.cliente and A.id = ro.area and A.grafico_insetos = 'S')
            inner join cliente c on c.id = o.cliente
            INNER JOIN armadilha_inseto ai ON ai.id = ro.armadilha AND ai.cliente = o.cliente AND ai.area = A.id
        WHERE STR_TO_DATE(o.data, '%Y-%m-%d') BETWEEN %s AND %s
            AND o.cliente = %s
            and ai.tipo = 'feromonio'
        group by A.nome, ro.armadilha, ai.tipo;
    """

    connection = get_connection(tenant_name)
    if connection == -1:
        return jsonify({tenant_name: "Erro ao reconhecer empresa"}), 404

    try:
        cursor = connection.cursor()
        # Executa a consulta com os parâmetros start_date, end_date e cliente
        cursor.execute(stmt, (start_date, end_date, cliente))
        results = cursor.fetchall()

        # Obtendo os nomes das colunas
        column_names = [desc[0] for desc in cursor.description]

        # Fechando cursor e conexão
        cursor.close()
        connection.close()

        # Montando a resposta com nomes das colunas
        response = [dict(zip(column_names, row)) for row in results]
        return jsonify(response)

    except Exception as e:
        cursor.close()
        connection.close()
        return jsonify({"error": str(e)}), 500
    
@generic_bp.route("/status_luminosas_mensal", methods=['GET'])
def get_status_luminosas_mensal(tenant_name, table_name):
    # Obtendo as datas e cliente dos parâmetros da URL
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

    # Construindo a consulta SQL com as datas embutidas no `WITH RECURSIVE`
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
            a.nome as area,
            COALESCE(SUM(ro.moscas), 0) AS total_moscas,
            COALESCE(SUM(ro.pernilongos), 0) AS total_pernilongos,
            COALESCE(SUM(ro.besouros), 0) AS total_besouros,
            COALESCE(SUM(ro.outrosluminosas), 0) AS total_outrosluminosas
        FROM meses m
            LEFT JOIN os o ON DATE_FORMAT(STR_TO_DATE(o.data, '%Y-%m-%d'), '%Y-%m') = m.mes
            LEFT JOIN osinseto ro ON o.id = ro.os
            LEFT JOIN areaindustrial a ON a.cliente = o.cliente AND a.id = ro.area AND a.grafico_insetos = 'S'
            INNER JOIN armadilha_inseto ai ON ai.id = ro.armadilha AND ai.cliente = o.cliente AND ai.area = a.id
            inner join cliente c on c.id = %s and DATE_FORMAT(STR_TO_DATE(c.datacadastro, '%Y-%m-%d'), '%Y-%m') <= m.mes
        WHERE o.cliente = %s
        and ai.tipo = 'luminosa'
        GROUP BY m.mes,ai.tipo,a.nome
        ORDER BY m.mes;
    """
    print(stmt)

    # Conexão com o banco de dados
    connection = get_connection(tenant_name)
    if connection == -1:
        return jsonify({tenant_name: "Erro ao reconhecer empresa"}), 404

    try:
        cursor = connection.cursor()
        # Executa a consulta com o parâmetro 'cliente'
        cursor.execute(stmt, (cliente,cliente))
        results = cursor.fetchall()

        # Obtendo os nomes das colunas
        column_names = [desc[0] for desc in cursor.description]

        # Fechando cursor e conexão
        cursor.close()
        connection.close()

        # Montando a resposta com nomes das colunas
        response = [dict(zip(column_names, row)) for row in results]
        return jsonify(response)

    except Exception as e:
        cursor.close()
        connection.close()
        return jsonify({"error": str(e)}), 500

@generic_bp.route("/status_feromonios_mensal", methods=['GET'])
def get_status_feromonios_mensal(tenant_name, table_name):
    # Obtendo as datas e cliente dos parâmetros da URL
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

    # Construindo a consulta SQL com as datas embutidas no `WITH RECURSIVE`
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
            a.nome as area,
            COALESCE(SUM(ro.sitophilus), 0) AS sitophilus,
            COALESCE(SUM(ro.rhizopertha), 0) AS rhizopertha,
            COALESCE(SUM(ro.tribolium), 0) AS tribolium,
            COALESCE(SUM(ro.lasioderma), 0) AS lasioderma,
            COALESCE(SUM(ro.sitotroga), 0) AS sitotroga,
            COALESCE(SUM(ro.ephestia), 0) AS ephestia,
            COALESCE(SUM(ro.plodia), 0) AS plodia,
            COALESCE(SUM(ro.periplanetaamericana), 0) AS periplanetaamericana,
            COALESCE(SUM(ro.outrosferomonio), 0) AS outrosferomonio,
            COALESCE(SUM(ro.battelagermanica), 0) AS battelagermanica
        FROM meses m
        LEFT JOIN os o ON DATE_FORMAT(STR_TO_DATE(o.data, '%Y-%m-%d'), '%Y-%m') = m.mes
        LEFT JOIN osinseto ro ON o.id = ro.os
        LEFT JOIN areaindustrial a ON a.cliente = o.cliente AND a.id = ro.area AND a.grafico_insetos = 'S'
        INNER JOIN armadilha_inseto ai ON ai.id = ro.armadilha AND ai.cliente = o.cliente AND ai.area = a.id
        inner join cliente c on c.id = %s and DATE_FORMAT(STR_TO_DATE(c.datacadastro, '%Y-%m-%d'), '%Y-%m') <= m.mes
        WHERE o.cliente = %s
        and ai.tipo = 'feromonio'
        GROUP BY m.mes,ai.tipo,a.nome
        ORDER BY m.mes;
    """

    # Conexão com o banco de dados
    connection = get_connection(tenant_name)
    if connection == -1:
        return jsonify({tenant_name: "Erro ao reconhecer empresa"}), 404

    try:
        cursor = connection.cursor()
        # Executa a consulta com o parâmetro 'cliente'
        cursor.execute(stmt, (cliente,cliente))
        results = cursor.fetchall()

        # Obtendo os nomes das colunas
        column_names = [desc[0] for desc in cursor.description]

        # Fechando cursor e conexão
        cursor.close()
        connection.close()

        # Montando a resposta com nomes das colunas
        response = [dict(zip(column_names, row)) for row in results]
        return jsonify(response)

    except Exception as e:
        cursor.close()
        connection.close()
        return jsonify({"error": str(e)}), 500


@generic_bp.route("/cliente_os", methods=['GET'])
def get_clientes_os(tenant_name, table_name):
    # Obtendo as datas e cliente dos parâmetros da URL
    year = request.args.get('year')
    month = request.args.get('month')
    acompanhante = request.args.get('acompanhante')

    # Construindo a consulta SQL com parâmetros de data e cliente
    stmt = """
        select distinct
            c.*
        from cliente c
            inner join os o on (o.cliente = c.id)
        where EXTRACT(MONTH FROM o.data) = %s
            and EXTRACT(YEAR FROM o.data) = %s
            and o.servico_acompanhante = %s
    """

    connection = get_connection(tenant_name)
    if connection == -1:
        return jsonify({tenant_name: "Erro ao reconhecer empresa"}), 404

    try:
        cursor = connection.cursor()
        # Executa a consulta com os parâmetros start_date, end_date e cliente
        cursor.execute(stmt, (month, year, acompanhante))
        results = cursor.fetchall()

        # Obtendo os nomes das colunas
        column_names = [desc[0] for desc in cursor.description]

        # Fechando cursor e conexão
        cursor.close()
        connection.close()

        # Montando a resposta com nomes das colunas
        response = [dict(zip(column_names, row)) for row in results]
        return jsonify(response)

    except Exception as e:
        cursor.close()
        connection.close()
        return jsonify({"error": str(e)}), 500
    
@generic_bp.route("/areaindustrial_os", methods=['GET'])
def get_areaindustrial_os(tenant_name, table_name):
    # Obtendo as datas e cliente dos parâmetros da URL
    year = request.args.get('year')
    month = request.args.get('month')
    acompanhante = request.args.get('acompanhante')

    # Construindo a consulta SQL com parâmetros de data e cliente
    stmt = """
        select distinct
            a.*
        from areaindustrial a
            inner join cliente c on (a.cliente = c.id)
            inner join os o on (o.cliente = c.id)
            inner join os_area oa on (oa.os = o.id)
        where EXTRACT(MONTH FROM o.data) = %s
            and EXTRACT(YEAR FROM o.data) = %s
            and o.servico_acompanhante = %s
    """

    connection = get_connection(tenant_name)
    if connection == -1:
        return jsonify({tenant_name: "Erro ao reconhecer empresa"}), 404

    try:
        cursor = connection.cursor()
        # Executa a consulta com os parâmetros start_date, end_date e cliente
        cursor.execute(stmt, (month, year, acompanhante))
        results = cursor.fetchall()

        # Obtendo os nomes das colunas
        column_names = [desc[0] for desc in cursor.description]

        # Fechando cursor e conexão
        cursor.close()
        connection.close()

        # Montando a resposta com nomes das colunas
        response = [dict(zip(column_names, row)) for row in results]
        return jsonify(response)

    except Exception as e:
        cursor.close()
        connection.close()
        return jsonify({"error": str(e)}), 500

@generic_bp.route("/area_os", methods=['GET'])
def get_area_os(tenant_name, table_name):
    # Obtendo as datas e cliente dos parâmetros da URL
    year = request.args.get('year')
    month = request.args.get('month')
    acompanhante = request.args.get('acompanhante')

    # Construindo a consulta SQL com parâmetros de data e cliente
    stmt = """
        select distinct
            a.*
        from os_area a
            inner join os o on (o.id =a.os)
        where EXTRACT(MONTH FROM o.data) = %s
            and EXTRACT(YEAR FROM o.data) = %s
            and o.servico_acompanhante = %s
    """

    connection = get_connection(tenant_name)
    if connection == -1:
        return jsonify({tenant_name: "Erro ao reconhecer empresa"}), 404

    try:
        cursor = connection.cursor()
        # Executa a consulta com os parâmetros start_date, end_date e cliente
        cursor.execute(stmt, (month, year, acompanhante))
        results = cursor.fetchall()

        # Obtendo os nomes das colunas
        column_names = [desc[0] for desc in cursor.description]

        # Fechando cursor e conexão
        cursor.close()
        connection.close()

        # Montando a resposta com nomes das colunas
        response = [dict(zip(column_names, row)) for row in results]
        return jsonify(response)

    except Exception as e:
        cursor.close()
        connection.close()
        return jsonify({"error": str(e)}), 500
    
@generic_bp.route("/roedor_os", methods=['GET'])
def get_roedor_os(tenant_name, table_name):
    # Obtendo as datas e cliente dos parâmetros da URL
    year = request.args.get('year')
    month = request.args.get('month')
    acompanhante = request.args.get('acompanhante')

    # Construindo a consulta SQL com parâmetros de data e cliente
    stmt = """
        select distinct
            a.*
        from osroedor a
            inner join os o on (o.id =a.os)
        where EXTRACT(MONTH FROM o.data) = %s
            and EXTRACT(YEAR FROM o.data) = %s
            and o.servico_acompanhante = %s
    """

    connection = get_connection(tenant_name)
    if connection == -1:
        return jsonify({tenant_name: "Erro ao reconhecer empresa"}), 404

    try:
        cursor = connection.cursor()
        # Executa a consulta com os parâmetros start_date, end_date e cliente
        cursor.execute(stmt, (month, year, acompanhante))
        results = cursor.fetchall()

        # Obtendo os nomes das colunas
        column_names = [desc[0] for desc in cursor.description]

        # Fechando cursor e conexão
        cursor.close()
        connection.close()

        # Montando a resposta com nomes das colunas
        response = [dict(zip(column_names, row)) for row in results]
        return jsonify(response)

    except Exception as e:
        cursor.close()
        connection.close()
        return jsonify({"error": str(e)}), 500
    
@generic_bp.route("/inseto_os", methods=['GET'])
def get_inseto_os(tenant_name, table_name):
    # Obtendo as datas e cliente dos parâmetros da URL
    year = request.args.get('year')
    month = request.args.get('month')
    acompanhante = request.args.get('acompanhante')

    # Construindo a consulta SQL com parâmetros de data e cliente
    stmt = """
        select distinct
            a.*
        from osinseto a
            inner join os o on (o.id =a.os)
        where EXTRACT(MONTH FROM o.data) = %s
            and EXTRACT(YEAR FROM o.data) = %s
            and o.servico_acompanhante = %s
    """

    connection = get_connection(tenant_name)
    if connection == -1:
        return jsonify({tenant_name: "Erro ao reconhecer empresa"}), 404

    try:
        cursor = connection.cursor()
        # Executa a consulta com os parâmetros start_date, end_date e cliente
        cursor.execute(stmt, (month, year, acompanhante))
        results = cursor.fetchall()

        # Obtendo os nomes das colunas
        column_names = [desc[0] for desc in cursor.description]

        # Fechando cursor e conexão
        cursor.close()
        connection.close()

        # Montando a resposta com nomes das colunas
        response = [dict(zip(column_names, row)) for row in results]
        return jsonify(response)

    except Exception as e:
        cursor.close()
        connection.close()
        return jsonify({"error": str(e)}), 500
    
@generic_bp.route("/produto_os", methods=['GET'])
def get_produto_os(tenant_name, table_name):
    # Obtendo as datas e cliente dos parâmetros da URL
    year = request.args.get('year')
    month = request.args.get('month')
    acompanhante = request.args.get('acompanhante')

    # Construindo a consulta SQL com parâmetros de data e cliente
    stmt = """
        select distinct
            a.*
        from os_produto a
            inner join os o on (o.id =a.os)
        where EXTRACT(MONTH FROM o.data) = %s
            and EXTRACT(YEAR FROM o.data) = %s
            and o.servico_acompanhante = %s
    """

    connection = get_connection(tenant_name)
    if connection == -1:
        return jsonify({tenant_name: "Erro ao reconhecer empresa"}), 404

    try:
        cursor = connection.cursor()
        # Executa a consulta com os parâmetros start_date, end_date e cliente
        cursor.execute(stmt, (month, year, acompanhante))
        results = cursor.fetchall()

        # Obtendo os nomes das colunas
        column_names = [desc[0] for desc in cursor.description]

        # Fechando cursor e conexão
        cursor.close()
        connection.close()

        # Montando a resposta com nomes das colunas
        response = [dict(zip(column_names, row)) for row in results]
        return jsonify(response)

    except Exception as e:
        cursor.close()
        connection.close()
        return jsonify({"error": str(e)}), 500

@generic_bp.route("/status_year", methods=['GET'])
def get_status_year(tenant_name, table_name):
    # Obtendo os parâmetros da URL
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

    # Construindo a consulta SQL com CTE recursivo para gerar os meses dinâmicos
    stmt = f"""
        WITH RECURSIVE meses AS (
            SELECT '{start_date}' AS mes
            UNION ALL
            SELECT DATE_FORMAT(DATE_ADD(STR_TO_DATE(CONCAT(mes, '-01'), '%Y-%m-%d'), INTERVAL 1 MONTH), '%Y-%m')
            FROM meses
            WHERE mes < '{end_date}'
        )
        SELECT
            m.mes,
            COALESCE(
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
                            AND o.cliente = '{cliente}'
                        ))
                    ELSE 
                        (SELECT COALESCE(SUM(
                            CASE WHEN ro.status = 'R' THEN 1 ELSE 0 END + 
                            CASE WHEN ro.status = 'C' THEN 1 ELSE 0 END
                        ), 0)
                        FROM osroedor ro
                        INNER JOIN os o ON o.id = ro.os
                        WHERE DATE_FORMAT(STR_TO_DATE(o.data, '%Y-%m-%d'), '%Y-%m') = m.mes
                        AND o.cliente = '{cliente}')
                        +
                        COALESCE((SELECT SUM(oa.roedor_morto) 
                                FROM os_area oa 
                                INNER JOIN os o2 ON o2.id = oa.os
                                WHERE DATE_FORMAT(STR_TO_DATE(o2.data, '%Y-%m-%d'), '%Y-%m') = m.mes
                                AND o2.cliente = '{cliente}'), 0)
                END, 0) AS total_status_R
        FROM meses m
            INNER JOIN cliente c ON c.id = '{cliente}' 
                AND DATE_FORMAT(STR_TO_DATE(c.datacadastro, '%Y-%m-%d'), '%Y-%m') <= m.mes
        ORDER BY m.mes;
        """

    print("SQL Statement:", stmt)
    # Conexão com o banco de dados
    connection = get_connection(tenant_name)
    if connection == -1:
        return jsonify({tenant_name: "Erro ao reconhecer empresa"}), 404

    try:
        cursor = connection.cursor()
        # Executa a consulta com o parâmetro cliente
        cursor.execute(stmt)
        results = cursor.fetchall()

        # Obtendo os nomes das colunas
        column_names = [desc[0] for desc in cursor.description]

        # Fechando cursor e conexão
        cursor.close()
        connection.close()

        # Montando a resposta com nomes das colunas
        response = [dict(zip(column_names, row)) for row in results]
        return jsonify(response)

    except Exception as e:
        cursor.close()
        connection.close()
        return jsonify({"error": str(e)}), 500


@generic_bp.route("/get_products_by_date_and_client", methods=['GET'])
def get_products_by_date_and_client(tenant_name, table_name):
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

    # Construindo a consulta SQL com parâmetros
    stmt = """
        SELECT distinct
            pd.* 
        FROM produto pd
        INNER JOIN os_produto op ON op.produto = pd.id
        INNER JOIN os o ON o.id = op.os AND o.cliente = %s
        WHERE o.data BETWEEN %s AND %s
    """

    connection = get_connection(tenant_name)
    if connection == -1:
        return jsonify({tenant_name: "Erro ao reconhecer empresa"}), 404

    try:
        # dictionary=True retorna resultados como dicionário
        cursor = connection.cursor(dictionary=True)
        # Executa a consulta com os parâmetros cliente, start_date e end_date
        cursor.execute(stmt, (cliente, start_date, end_date))
        results = cursor.fetchall()

        # Fechando cursor e conexão
        cursor.close()
        connection.close()

        # Retorna os resultados como JSON
        return jsonify(results)

    except Exception as e:
        cursor.close()
        connection.close()
        return jsonify({"error": str(e)}), 500


def format_record(record):
    """Formata os campos de data no registro."""
    for key, value in record.items():
        if isinstance(value, datetime):
            record[key] = value.strftime("%Y-%m-%d %H:%M:%S")
        elif isinstance(value, str):
            pass
    return record


@generic_bp.route("/insert", methods=['POST'])
def insert_table(tenant_name, table_name):
    column_clauses = []
    value_clauses = []
    params = request.get_json()
    stmt = f"insert into {table_name} "
    for key, value in params.items():
        column_clauses.append(f"{key}")
        value_clauses.append(f"'{value}'")
    stmt += "(" + ", ".join(column_clauses) + ")"
    stmt += " values (" + ", ".join(value_clauses) + ");"
    print(stmt)
    connection = get_connection(tenant_name)
    if connection == -1:
        return jsonify({tenant_name: "erro ao reconhecer empresa"}), 404
    try:
        cursor = connection.cursor()
        cursor.execute(stmt)
        cursor.close()
        connection.close()

        return jsonify({"response": "inserido com sucesso"})

    except Exception as e:
        cursor.close()
        connection.close()
        return jsonify({"error": str(e)}), 500


@generic_bp.route("/delete", methods=['POST'])
def delete_table(tenant_name, table_name):
    where_clauses = []
    params = request.get_json()
    stmt = f"delete from {table_name}"
    for key, value in params.items():
        where_clauses.append(f"{key} = '{value}'")
    stmt += " WHERE " + " AND ".join(where_clauses)
    print(stmt)
    connection = get_connection(tenant_name)
    if connection == -1:
        return jsonify({tenant_name: "erro ao reconhecer empresa"}), 404
    try:
        cursor = connection.cursor()
        cursor.execute(stmt)
        cursor.close()
        connection.close()

        return jsonify({"response": "removido com sucesso"})

    except Exception as e:
        cursor.close()
        connection.close()
        return jsonify({"error": str(e)}), 500


@generic_bp.route("/update", methods=['POST'])
def update_table(tenant_name, table_name):
    params = request.get_json()
    set_clauses = []
    where_clauses = []

    # Construção da parte SET
    for key, value in params.items():
        if key != "where":  # Ignorar a chave "where" para a parte SET
            set_clauses.append(f"{key} = '{value}'")

    # Construção da parte WHERE
    where_conditions = params.get("where", {})
    for key, value in where_conditions.items():
        where_clauses.append(f"{key} = '{value}'")

    # Adicionando a cláusula WHERE pelo campo "id" se existir
    if "id" in params:
        where_clauses.append(f"id = {params.get('id')}")

    # Montagem da query
    stmt = f"UPDATE {table_name} SET " + ", ".join(set_clauses)
    if where_clauses:
        stmt += " WHERE " + " AND ".join(where_clauses)

    print(stmt)  # Para depuração

    # Conectando ao banco de dados
    connection = get_connection(tenant_name)
    if connection == -1:
        return jsonify({tenant_name: "erro ao reconhecer empresa"}), 404

    try:
        cursor = connection.cursor()
        cursor.execute(stmt)
        connection.commit()  # Confirma a transação
        cursor.close()
        connection.close()

        return jsonify({"response": "atualizado com sucesso"})

    except Exception as e:
        cursor.close()
        connection.close()
        return jsonify({"error": str(e)}), 500


@generic_bp.route("/insert_and_return_id", methods=['POST'])
def insert_table_and_return_id(tenant_name, table_name):
    columns = []
    placeholders = []
    values = []
    params = request.get_json()
    stmt = f"INSERT INTO {table_name} "

    for key, value in params.items():
        columns.append(f"{key}")
        if value is None:
            placeholders.append("NULL")
        else:
            placeholders.append("%s")
            values.append(value)

    stmt += "(" + ", ".join(columns) + ")"
    stmt += " VALUES (" + ", ".join(placeholders) + ");"
    print(stmt)  # Apenas para depuração

    connection = get_connection(tenant_name)
    if connection == -1:
        return jsonify({tenant_name: "erro ao reconhecer empresa"}), 404

    try:
        cursor = connection.cursor()
        cursor.execute(stmt, values)
        connection.commit()

        # Obtendo o ID gerado
        last_id = cursor.lastrowid
        print(last_id)

        cursor.close()
        connection.close()

        return jsonify({"response": "inserido com sucesso", "id": last_id})

    except Exception as e:
        cursor.close()
        connection.close()
        return jsonify({"error": str(e)}), 500

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