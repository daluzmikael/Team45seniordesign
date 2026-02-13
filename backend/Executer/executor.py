import psycopg2
import pandas as pd
import re

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
def is_safe_sql(sql_query):
    sql_lower = sql_query.strip().lower()
    sql_lower = re.sub(r"--.*?\n", "", sql_lower).strip()

    if "select" not in sql_lower:
        return False

    dangerous = ["insert", "update", "delete", "drop",
                 "alter", "create", "replace", "truncate"]

    for keyword in dangerous:
        if re.search(rf"\b{keyword}\b", sql_lower):
            return False

    return True


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


# Query execution function
def execute_query(conn, sql_query):
    cursor = conn.cursor()
    cursor.execute(sql_query)

    rows = cursor.fetchall()
    colnames = [desc[0] for desc in cursor.description]

    df = pd.DataFrame(rows, columns=colnames)
    return df
