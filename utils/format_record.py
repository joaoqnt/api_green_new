from datetime import datetime, date

def format_record(record):
    """
    Formata os valores de um dicionário retornado de uma consulta SQL.
    Converte datas e datetimes para string no formato ISO e remove campos com valor None.
    """
    formatted = {}
    for key, value in record.items():
        if value is None:
            continue  # Ignora campos com valor None
        elif isinstance(value, (datetime, date)):
            formatted[key] = value.isoformat()
        else:
            formatted[key] = value
    return formatted
