def build_select_query(table_name, filters, order_by=None, limit=None, offset=None, base_select=None):
    where_clauses = []
    values = []

    for key, val in filters.items():
        # Se for "between" ou "in" na URL, extrai o campo real
        if key in ["between", "in"]:
            content = val.strip("{}")
            field, data = content.split("=")
            field = field.strip()
            if key == "between":
                start, end = data.split(",")
                where_clauses.append(f"{field} BETWEEN %s AND %s")
                values.extend([start.strip(), end.strip()])
            elif key == "in":
                val_list = [v.strip() for v in data.split(",")]
                placeholders = ", ".join(["%s"] * len(val_list))
                where_clauses.append(f"{field} IN ({placeholders})")
                values.extend(val_list)
            continue

        # EQ / LIKE padrão
        filter_type, v = val if isinstance(val, tuple) else ('eq', val)
        if filter_type == 'eq':
            where_clauses.append(f"{key} = %s")
            values.append(v)
        elif filter_type == 'like':
            if not ('%' in v):
                v = f"%{v}%"
            where_clauses.append(f"{key} LIKE %s")
            values.append(v)

    where_stmt = "WHERE " + \
        " AND ".join(where_clauses) if where_clauses else ""
    order_stmt = f"ORDER BY {', '.join(order_by)}" if order_by else ""
    limit_stmt = f"LIMIT {limit}" if limit else ""
    offset_stmt = f"OFFSET {offset}" if offset else ""

    if base_select:
        parts = base_select.split("GROUP BY")
        select_part = parts[0].strip()
        group_by_part = "GROUP BY " + \
            parts[1].strip() if len(parts) > 1 else ""
        stmt = f"{select_part} {where_stmt} {group_by_part} {order_stmt} {limit_stmt} {offset_stmt}".strip()
    else:
        stmt = f"SELECT * FROM {table_name} {where_stmt} {order_stmt} {limit_stmt} {offset_stmt}".strip()

    return stmt, values


def build_insert_query(table_name, params):
    columns = []
    placeholders = []
    values = []

    for key, value in params.items():
        if value is not None:  # só insere se não for None
            columns.append(key)
            placeholders.append("%s")
            values.append(value)

    stmt = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(placeholders)});"
    return stmt, values


def build_update_query(table_name, params):
    set_clauses = []
    values = []

    primary_key = 'id'  # supondo que sempre seja 'id', ou você pode tornar dinâmico
    for key, value in params.items():
        if key == primary_key:
            continue
        set_clauses.append(f"{key} = %s")
        values.append(value)

    values.append(params[primary_key])  # adiciona a PK no final para o WHERE

    stmt = f"UPDATE {table_name} SET {', '.join(set_clauses)} WHERE {primary_key} = %s"
    return stmt, values


def build_delete_query(table_name, params):
    where_clauses = []
    values = []

    for key, value in params.items():
        where_clauses.append(f"{key} = %s")
        values.append(value)

    stmt = f"DELETE FROM {table_name} WHERE {' AND '.join(where_clauses)}"
    return stmt, values
