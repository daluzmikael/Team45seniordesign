import psycopg2
import pandas as pd
import re
import sqlglot
from sqlglot import parse_one
from sqlglot.errors import ParseError
import json

# 2. Connect to PostgreSQL (AWS RDS)
def get_connection():
    """Get a fresh database connection"""
    return psycopg2.connect(
        host="nba-sdp-project.cs1c0smw8vqa.us-east-1.rds.amazonaws.com",
        port=5432,
        dbname="NBA-STATS",
        user="VonLindenthal",
        password="Vlindenthal1!",
        sslmode="require"
    )


# 3. Read DB schema (for GPT prompt)
def get_db_schema(conn):
    cursor = conn.cursor()

    cursor.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public';
    """)

    tables = cursor.fetchall()
    schema_description = ""

    for table in tables:
        table_name = table[0]

        cursor.execute(f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = '{table_name}'
        """)

        columns = cursor.fetchall()
        column_list = ", ".join([col[0] for col in columns])
        schema_description += f"{table_name}({column_list})\n"

    return schema_description


# 4. SQL safety checker
# replaced the function is_sql_safe()
def validate_and_normalize_sql(sql_query: str) -> str:
    try:
        # parse SQL to ensure validity
        parsed = parse_one(sql_query, read="postgres")

    except ParseError as e:
        raise ValueError(f"SQL Syntax Error: {e}")

    #  only SELECT statement
    if parsed.key.upper() != "SELECT":
        raise ValueError("Only SELECT statements are allowed.")

    # no multiple statements
    if ";" in sql_query.strip().rstrip(";"):
        raise ValueError("Multiple SQL statements are not allowed.")

    # normalize SQL 
    normalized_sql = parsed.sql(dialect="postgres")

    return normalized_sql

# 5. Add LIMIT automatically
def limit_rows(sql_query, limit=50):
    sql_lower = sql_query.lower()

    if "limit" in sql_lower:
        return sql_query

    if "union" in sql_lower:
        sql_query = sql_query.rstrip(";")
        return f"SELECT * FROM ({sql_query}) AS combined_results LIMIT {limit};"

    sql_query = sql_query.rstrip(";")
    return f"{sql_query} LIMIT {limit};"

def set_query_timeout(conn, timeout_ms=3000):
    cursor = conn.cursor()
    cursor.execute(f"SET LOCAL statement_timeout = {timeout_ms};")

def check_query_cost(conn, sql_query, max_cost=100000):
    cursor = conn.cursor()

    explain_query = f"EXPLAIN (FORMAT JSON) {sql_query}"
    cursor.execute(explain_query)

    result = cursor.fetchone()
    explain_json = result[0][0]

    total_cost = explain_json["Plan"]["Total Cost"]

    print(f"[DEBUG] Estimated Query Cost: {total_cost}")

    if total_cost > max_cost:
        raise ValueError(
            f"Query blocked: Estimated cost {total_cost} exceeds threshold {max_cost}"
        )

    return total_cost

# Query execution function
def execute_query(conn, sql_query, max_cost=100000, timeout_ms=3000):
    cursor = conn.cursor()
    cursor.execute("BEGIN;")

    set_query_timeout(conn, timeout_ms)
    check_query_cost(conn, sql_query, max_cost)

    cursor.execute(sql_query)
    rows = cursor.fetchall()
    colnames = [desc[0] for desc in cursor.description]

    conn.commit()
    return pd.DataFrame(rows, columns=colnames)
