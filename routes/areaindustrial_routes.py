from flask import Blueprint, request, jsonify
from services.query_executor import execute_query

# Create Blueprint for industrial area routes
areaindustrial_bp = Blueprint('areaindustrial', __name__, url_prefix='/api/<tenant_name>/areaindustrial')

@areaindustrial_bp.route("/areaindustrial_os", methods=['GET'])
def get_areaindustrial_os(tenant_name):
    """
    Endpoint to retrieve industrial areas from OS filtered by:
    - year: OS year (YYYY)
    - month: OS month (1-12)
    - acompanhante: attendant identifier
    
    Returns:
    - List of industrial area records with all columns from areaindustrial table
    """
    # Get parameters from URL
    year = request.args.get('year')
    month = request.args.get('month')
    acompanhante = request.args.get('acompanhante')

    # Parameter validation
    if None in (year, month, acompanhante):
        return jsonify({
            "error": "Missing required parameters",
            "required_params": {
                "year": "YYYY format",
                "month": "MM format (1-12)",
                "acompanhante": "attendant identifier"
            },
            "example_request": "/api/tenant/table/areaindustrial_os?year=2023&month=5&acompanhante=123"
        }), 400

    # SQL query
    stmt = """
        SELECT DISTINCT
            a.*
        FROM areaindustrial a
        INNER JOIN cliente c ON (a.cliente = c.id)
        INNER JOIN os o ON (o.cliente = c.id)
        INNER JOIN os_area oa ON (oa.os = o.id)
        WHERE EXTRACT(MONTH FROM o.data) = %s
          AND EXTRACT(YEAR FROM o.data) = %s
          AND o.servico_acompanhante = %s
    """

    # Execute query using shared function
    return execute_query(tenant_name, stmt, (month, year, acompanhante))