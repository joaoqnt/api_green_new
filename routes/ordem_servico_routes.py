from flask import Blueprint, request, jsonify
from services.query_builder import build_select_query
from services.query_executor import execute_query

ordem_servico_bp = Blueprint(
    "ordem_servico", __name__, url_prefix="/api/<tenant_name>/os")


@ordem_servico_bp.route("/allOs", methods=["GET"])
def get_os_join_clients(tenant_name):
    filters = {k: v for k, v in request.args.items() if k not in [
        "order_by", "limit", "offset"]}
    order_by = request.args.get("order_by", "").split(
        ",") if "order_by" in request.args else []
    limit = request.args.get("limit")
    offset = request.args.get("offset")

    # Query base com JOIN
    base_stmt = """
        SELECT o.*, c.nome, 
               GROUP_CONCAT(ae.nome ORDER BY ae.nome SEPARATOR ', ') AS areas 
        FROM os o 
        INNER JOIN cliente c ON c.id = o.cliente 
        INNER JOIN os_area oa ON oa.os = o.id 
        INNER JOIN areaindustrial ae ON ae.id = oa.area AND ae.cliente = c.id
        GROUP BY o.id, c.nome
    """

    stmt, values = build_select_query(
        "os", filters, order_by=order_by, limit=limit, offset=offset, base_select=base_stmt
    )

    return execute_query(tenant_name, stmt, values)
