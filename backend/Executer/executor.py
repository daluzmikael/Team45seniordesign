import psycopg2
import pandas as pd
import re
import sqlglot
from sqlglot import parse_one
from sqlglot import expressions as exp
from sqlglot.errors import ParseError
import json
import logging
import os
from typing import Optional
import time


_schema_cache: dict = {"value": None, "fetched_at": 0.0}
_SCHEMA_TTL_SECONDS = 600

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
        dbname="postgres",
        user="VonLindenthal",
        password="Vlindenthal1!",
        sslmode="require"
    )


# 3. Read DB schema (for GPT prompt) — COMPACT + WHITELISTED version
# Only includes tables the app actually queries. Groups repeated season tables
# by type and lists columns once. Cuts token usage by ~95% vs the original.



# Heavy-use families: show columns once (these are 95% of queries)
_HEAVY_USE_PATTERNS = [
    r"all_players_regular_\d{4}_\d{4}$",
    r"all_players_playoffs_\d{4}_\d{4}$",
    r"nba_advanced_season_\d{4}_\d{2}_season_type_(regular_season|playoffs)_per_mode_p",
    r"player_game_logs$",
    r"court_shots$",
]

# Other families: list patterns + counts only (cheap awareness)
_OTHER_FAMILY_PREFIXES = [
    "nba_hustle_season_",
    "nba_clutch_season_",
    "nba_lineups_group_",       # nba_lineups_group_5_season_*
    "nba_schedule_",
    "nba_standings_",
    "nba_player_tracking_pt_",  # catchshoot, drives, passing, defense, etc.
    "team_advanced_season_",     # team-level advanced stats
]

def _summarize_sql(sql: str, max_chars: int = 600) -> str:
    one_line = re.sub(r"\s+", " ", sql or "").strip()
    if len(one_line) <= max_chars:
        return one_line
    return one_line[:max_chars] + f"... [+{len(one_line) - max_chars} chars]"


def _is_heavy_use_table(table_name: str) -> bool:
    return any(re.match(p, table_name) for p in _HEAVY_USE_PATTERNS)


def get_db_schema(conn):

    now = time.time()
    if _schema_cache["value"] and (now - _schema_cache["fetched_at"]) < _SCHEMA_TTL_SECONDS:
        return _schema_cache["value"]
    
    logger.debug("Fetching database schema (tiered)...")
    cursor = conn.cursor()
    cursor.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'public' ORDER BY table_name;
    """)
    all_tables = [row[0] for row in cursor.fetchall()]

    heavy = [t for t in all_tables if _is_heavy_use_table(t)]

    regular_tables  = sorted(t for t in heavy if re.match(r"all_players_regular_\d{4}_\d{4}$", t))
    playoffs_tables = sorted(t for t in heavy if re.match(r"all_players_playoffs_\d{4}_\d{4}$", t))
    advanced_tables = sorted(t for t in heavy if t.startswith("nba_advanced_season_"))
    other_heavy     = [t for t in heavy if t not in regular_tables and t not in playoffs_tables and t not in advanced_tables]

    schema_parts = []

    def _columns_for(table):
        cursor.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = %s ORDER BY ordinal_position;",
            (table,),
        )
        return ", ".join(c[0] for c in cursor.fetchall())

    # ---- Heavy-use families: full column lists (single sample) ----
    if regular_tables:
        years = []
        for t in regular_tables:
            m = re.match(r"all_players_regular_(\d{4})_(\d{4})$", t)
            if m: years.append(f"{m.group(1)}-{m.group(2)}")
        schema_parts.append(
            "=== Regular Season Tables ===\n"
            "Pattern: all_players_regular_YYYY_YYYY\n"
            f"Columns: {_columns_for(regular_tables[-1])}\n"
            f"Available ({len(regular_tables)}): {', '.join(years)}\n"
        )

    if playoffs_tables:
        years = []
        for t in playoffs_tables:
            m = re.match(r"all_players_playoffs_(\d{4})_(\d{4})$", t)
            if m: years.append(f"{m.group(1)}-{m.group(2)}")
        schema_parts.append(
            "=== Playoffs Tables ===\n"
            "Pattern: all_players_playoffs_YYYY_YYYY\n"
            f"Columns: {_columns_for(playoffs_tables[-1])}\n"
            f"Available ({len(playoffs_tables)}): {', '.join(years)}\n"
        )

    if advanced_tables:
        schema_parts.append(
            "=== Advanced Season Tables ===\n"
            "Pattern: nba_advanced_season_YYYY_YY_season_type_TYPE_per_mode_p\n"
            f"Columns: {_columns_for(advanced_tables[-1])}\n"
            f"Available ({len(advanced_tables)}): {', '.join(advanced_tables)}\n"
        )

    for t in other_heavy:
        schema_parts.append(f"{t}({_columns_for(t)})")

    # ---- Other families: directory only (no columns) ----
    directory_lines = []
    for prefix in _OTHER_FAMILY_PREFIXES:
        family = sorted(t for t in all_tables if t.startswith(prefix))
        if not family:
            continue
        sample = family[0]
        directory_lines.append(f"  {prefix}* ({len(family)} tables, e.g. {sample})")

    if directory_lines:
        schema_parts.append(
            "=== Other Available Table Families (use intent routing in RULE 0) ===\n"
            "These exist but columns are not pre-loaded to save tokens.\n"
            "If you route here, use exact table names from this list and pick columns "
            "by name convention (player_name, team_abbreviation, season-style stats).\n"
            "If a column doesn't exist, the repair pass will surface a clear error.\n"
            + "\n".join(directory_lines) + "\n"
        )

    schema_description = "\n".join(schema_parts)
    logger.debug(
        "Schema: %d chars | %d heavy-use, %d other-family tables enumerated",
        len(schema_description), len(heavy), sum(1 for t in all_tables if any(t.startswith(p) for p in _OTHER_FAMILY_PREFIXES))
    )
    _schema_cache["value"] = schema_description
    _schema_cache["fetched_at"] = now
    return schema_description


# 4. SQL safety checker
# replaced the function is_sql_safe()
def _is_read_only_select_expression(expression) -> bool:
    """Allow SELECT queries and SELECT-only set operations such as UNION ALL."""
    if isinstance(expression, exp.Select):
        return True

    if isinstance(expression, exp.Union):
        return _is_read_only_select_expression(
            expression.this
        ) and _is_read_only_select_expression(expression.expression)

    return False


def validate_and_normalize_sql(sql_query: str) -> str:
    logger.debug("Validating SQL:\n%s", sql_query)
    try:
        # parse SQL to ensure validity
        parsed = parse_one(sql_query, read="postgres")

    except ParseError as e:
        raise ValueError(f"SQL Syntax Error: {e}")

    # Only read-only SELECT statements, including SELECT-only UNION/UNION ALL.
    if not _is_read_only_select_expression(parsed):
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
    logger.info("Executing SQL: %s", _summarize_sql(sql_query))
    logger.debug("Full SQL:\n%s", sql_query)
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
            logger.info("Query result: [empty]")
        else:
            logger.info(
                "Query result: %d rows × %d cols | columns: %s",
                len(df_result), len(df_result.columns),
                ", ".join(df_result.columns[:10]) + ("..." if len(df_result.columns) > 10 else ""),
            )
            logger.debug("Full result table:\n%s", df_result.to_string(index=False))
        return df_result

    except Exception as e:
        conn.rollback() # Ensure we clean up if this attempt fails
        logger.error("Query execution failed: %s", e)
        raise