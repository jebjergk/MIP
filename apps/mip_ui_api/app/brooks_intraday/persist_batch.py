"""Snowflake multi-row INSERT helpers (INSERT … SELECT … FROM VALUES).

Snowflake forbids non-constant expressions (e.g. PARSE_JSON) in VALUES clauses and
cannot rewrite INSERT…SELECT+PARSE_JSON via connector executemany. Build one
statement per chunk instead.
"""

from __future__ import annotations

from typing import Any, Sequence


def execute_insert_select_from_values(
    cur: Any,
    *,
    table_and_columns: str,
    select_list_sql: str,
    row_params: Sequence[Sequence[Any]],
) -> None:
    """Run one multi-row insert.

    table_and_columns: ``MIP.APP.T (COL1, COL2, COL3)``
    select_list_sql: expressions over column1..columnN
    row_params: list of tuples, each with N bind values
    """
    if not row_params:
        return
    width = len(row_params[0])
    value_row = "(" + ", ".join(["%s"] * width) + ")"
    values_sql = ",\n".join(value_row for _ in row_params)
    sql = f"""
        INSERT INTO {table_and_columns}
        SELECT {select_list_sql}
        FROM VALUES
        {values_sql}
    """
    flat: list[Any] = []
    for row in row_params:
        if len(row) != width:
            raise ValueError(f"ragged batch row width {len(row)} != {width}")
        flat.extend(row)
    cur.execute(sql, tuple(flat))
