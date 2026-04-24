import psycopg2
import pandas as pd
import re
import sqlglot
from sqlglot import parse_one
from sqlglot.errors import ParseError
import json
import logging
import os
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

# Root logging is configured by the app entrypoint (e.g. Interpreter.interpreter before
# this import, or uvicorn). Avoid basicConfig here so we do not steal first configuration
# and hide DEBUG lines in the interpreter.
logger = logging.getLogger(__name__)


def _load_backend_dotenv() -> None:
    """Load backend/.env so POSTGRES_* vars apply before connecting."""
    load_dotenv(Path(__file__).resolve().parent.parent / ".env")


# 2. Connect to PostgreSQL (AWS RDS)
def get_connection():
    """Get a fresh database connection (database name from POSTGRES_DB, default NBA-STATS)."""
    _load_backend_dotenv()
    password = os.getenv("POSTGRES_PASSWORD")
    if not password:
        raise RuntimeError(
            "POSTGRES_PASSWORD is not set. Add it to Team45seniordesign/backend/.env"
        )
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "nba-sdp-project.cs1c0smw8vqa.us-east-1.rds.amazonaws.com"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "NBA-STATS"),
        user=os.getenv("POSTGRES_USER", "VonLindenthal"),
        password=password,
        sslmode=os.getenv("POSTGRES_SSLMODE", "require"),
    )


# 3. Read DB schema (for GPT prompt) — COMPACT + WHITELISTED version
# Only includes tables the app actually queries. Groups repeated season tables
# by type and lists columns once. Cuts token usage by ~95% vs the original.

# Table patterns the app actually uses — everything else is excluded
_RELEVANT_TABLE_PATTERNS = [
    r"all_players_regular_\d{4}_\d{4}$",
    r"all_players_playoffs_\d{4}_\d{4}$",
    r"nba_advanced_season_\d{4}_\d{2}_season_type_(regular_season|playoffs)_per_mode_p",
    r"player_game_logs$",
    r"court_shots$",
]

def _is_relevant_table(table_name: str) -> bool:
    return any(re.match(p, table_name) for p in _RELEVANT_TABLE_PATTERNS)


def get_db_schema(conn):
    logger.debug("Fetching database schema (compact + whitelisted)...")
    cursor = conn.cursor()

    cursor.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
        ORDER BY table_name;
    """)

    all_tables = [row[0] for row in cursor.fetchall()]

    # Filter to only relevant tables
    tables = [t for t in all_tables if _is_relevant_table(t)]
    logger.debug("Schema: %d relevant tables out of %d total", len(tables), len(all_tables))

    # Categorize by type
    regular_tables = sorted([t for t in tables if re.match(r"all_players_regular_\d{4}_\d{4}$", t)])
    playoffs_tables = sorted([t for t in tables if re.match(r"all_players_playoffs_\d{4}_\d{4}$", t)])
    advanced_tables = sorted([t for t in tables if t.startswith("nba_advanced_season_")])
    other_tables = [t for t in tables if t not in regular_tables and t not in playoffs_tables and t not in advanced_tables]

    schema_parts = []

    # Regular season tables — columns once, enumerate years
    if regular_tables:
        cursor.execute(f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = '{regular_tables[-1]}'
            ORDER BY ordinal_position;
        """)
        columns = ", ".join([col[0] for col in cursor.fetchall()])

        years = []
        for t in regular_tables:
            m = re.match(r"all_players_regular_(\d{4})_(\d{4})$", t)
            if m:
                years.append(f"{m.group(1)}-{m.group(2)}")

        schema_parts.append(
            f"=== Regular Season Tables ===\n"
            f"Table pattern: all_players_regular_YYYY_YYYY\n"
            f"Columns (same for all): {columns}\n"
            f"Available tables ({len(regular_tables)}): {', '.join(years)}\n"
        )

    # Playoffs tables
    if playoffs_tables:
        cursor.execute(f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = '{playoffs_tables[-1]}'
            ORDER BY ordinal_position;
        """)
        columns = ", ".join([col[0] for col in cursor.fetchall()])

        years = []
        for t in playoffs_tables:
            m = re.match(r"all_players_playoffs_(\d{4})_(\d{4})$", t)
            if m:
                years.append(f"{m.group(1)}-{m.group(2)}")

        schema_parts.append(
            f"=== Playoffs Tables ===\n"
            f"Table pattern: all_players_playoffs_YYYY_YYYY\n"
            f"Columns (same for all): {columns}\n"
            f"Available tables ({len(playoffs_tables)}): {', '.join(years)}\n"
        )

    # Advanced season tables — columns once, list available
    if advanced_tables:
        cursor.execute(f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = '{advanced_tables[-1]}'
            ORDER BY ordinal_position;
        """)
        columns = ", ".join([col[0] for col in cursor.fetchall()])

        schema_parts.append(
            f"=== Advanced Season Tables ===\n"
            f"Table pattern: nba_advanced_season_YYYY_YY_season_type_TYPE_per_mode_p\n"
            f"Columns (same for all): {columns}\n"
            f"Available tables ({len(advanced_tables)}): {', '.join(advanced_tables[:5])}... and {len(advanced_tables) - 5} more\n"
        )

    # Other unique tables — list columns individually
    for table_name in other_tables:
        cursor.execute(f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = '{table_name}'
            ORDER BY ordinal_position;
        """)
        columns = ", ".join([col[0] for col in cursor.fetchall()])
        schema_parts.append(f"{table_name}({columns})")

    schema_description = "\n".join(schema_parts)
    logger.debug("Compact schema size: %d chars (%d tables included)", len(schema_description), len(tables))
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