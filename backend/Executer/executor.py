import psycopg2
import pandas as pd
import re
import sqlglot
from sqlglot import parse_one
from sqlglot.errors import ParseError
import json
import logging
import os
from typing import Optional

# Root logging is configured by the app entrypoint (e.g. Interpreter.interpreter before
# this import, or uvicorn). Avoid basicConfig here so we do not steal first configuration
# and hide DEBUG lines in the interpreter.
logger = logging.getLogger(__name__)
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
    logger.debug("Fetching database schema...")
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
    logger.debug("Validating SQL:\n%s", sql_query)
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
    logger.debug("Normalized SQL:\n%s", normalized_sql)
    return normalized_sql

# 5. Row cap — automatic LIMIT injection disabled (callers still pass through for compatibility).
def limit_rows(sql_query, limit=50):
    """Return SQL unchanged; do not append LIMIT. The model may still emit LIMIT."""
    return sql_query

def set_query_timeout(conn, timeout_ms=3000):
    logger.debug("Setting query timeout to %d ms", timeout_ms)
    cursor = conn.cursor()
    cursor.execute(f"SET LOCAL statement_timeout = {timeout_ms};")

def _query_plan_cost_disabled() -> bool:
    """Env: DISABLE_QUERY_COST_CHECK=1/true or QUERY_PLAN_COST_MAX=off|0|unlimited."""
    if os.getenv("DISABLE_QUERY_COST_CHECK", "").strip().lower() in ("1", "true", "yes"):
        return True
    raw = os.getenv("QUERY_PLAN_COST_MAX", "").strip().lower()
    return raw in ("off", "none", "disable", "0", "unlimited")


def _default_query_plan_cost_cap() -> float:
    """Planner cost ceiling (PostgreSQL estimated cost units). Set QUERY_PLAN_COST_MAX in .env to override."""
    return float(os.getenv("QUERY_PLAN_COST_MAX", "2000000").strip())


def check_query_cost(conn, sql_query, max_cost: Optional[float] = None):
    if _query_plan_cost_disabled():
        logger.debug("Skipping PostgreSQL planner cost check (disabled via env)")
        return 0.0

    if max_cost is None:
        max_cost = _default_query_plan_cost_cap()

    cursor = conn.cursor()

    explain_query = f"EXPLAIN (FORMAT JSON) {sql_query}"
    cursor.execute(explain_query)

    result = cursor.fetchone()
    explain_json = result[0][0]

    total_cost = explain_json["Plan"]["Total Cost"]

    logger.info("Estimated Query Cost: %s", total_cost)

    if total_cost > max_cost:
        raise ValueError(
            f"Query blocked: Estimated cost {total_cost} exceeds threshold {max_cost}"
        )

    return total_cost

# Query execution function
def execute_query(conn, sql_query, max_cost: Optional[float] = None, timeout_ms=60000):
    logger.info("Executing SQL Query:\n%s", sql_query)
    table_refs = sorted(
        set(
            re.findall(
                r'(?i)\b(?:from|join)\s+(?:public\.)?"?([a-zA-Z_][a-zA-Z0-9_]*)"?',
                sql_query or "",
            )
        )
    )
    if table_refs:
        logger.info("Source tables referenced: %s", ", ".join(table_refs))
    
    # CRITICAL: Clear any previous failed transactions before starting
    conn.rollback() 
    
    cursor = conn.cursor()
    try:
        cursor.execute("BEGIN;") # Start a fresh transaction
        set_query_timeout(conn, timeout_ms)

        # Check cost before running the full query
        total_cost = check_query_cost(conn, sql_query, max_cost)
        
        cursor.execute(sql_query)
        rows = cursor.fetchall()
        colnames = [desc[0] for desc in cursor.description]
        df_result = pd.DataFrame(rows, columns=colnames)
        
        conn.commit() # Save changes
        logger.info(
            "Query executed successfully | Rows: %d | Cost: %s",
            len(rows),
            total_cost
        )
        if df_result.empty:
            logger.info("Query result table: [empty]")
        else:
            logger.info("Query result table:\n%s", df_result.to_string(index=False))
        return df_result

    except Exception as e:
        conn.rollback() # Ensure we clean up if this attempt fails
        logger.error("Query execution failed: %s", e)
        raise
