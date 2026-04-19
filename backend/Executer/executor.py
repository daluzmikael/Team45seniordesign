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

from dotenv import load_dotenv

from sql_postprocess import normalize_game_log_wl_column

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

if not logging.getLogger().hasHandlers():
    logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# NBA Stats API team id for each 3-letter abbreviation (string keys uppercased).
# Used when generated SQL compares TEAM_ID to an abbreviation like 'DEN' instead of numeric id.
NBA_TEAM_ABBR_TO_TEAM_ID = {
    "ATL": "1610612737",
    "BOS": "1610612738",
    "BKN": "1610612751",
    "CHA": "1610612766",
    "CHI": "1610612741",
    "CLE": "1610612739",
    "DAL": "1610612742",
    "DEN": "1610612743",
    "DET": "1610612765",
    "GSW": "1610612744",
    "HOU": "1610612745",
    "IND": "1610612754",
    "LAC": "1610612746",
    "LAL": "1610612747",
    "MEM": "1610612763",
    "MIA": "1610612748",
    "MIL": "1610612749",
    "MIN": "1610612750",
    "NOP": "1610612740",
    "NO": "1610612740",
    "NYK": "1610612752",
    "OKC": "1610612760",
    "ORL": "1610612753",
    "PHI": "1610612755",
    "PHX": "1610612756",
    "POR": "1610612757",
    "SAC": "1610612758",
    "SAS": "1610612759",
    "TOR": "1610612761",
    "UTA": "1610612762",
    "WAS": "1610612764",
}


def _normalize_team_name_abbrev_mismatch(sql_query: str) -> str:
    """
    Generated SQL often uses city/nick abbrev in TEAM_NAME ILIKE (e.g. '%OKC%') but
    team_advanced.TEAM_NAME stores full names like 'Oklahoma City Thunder'. Map those
    to TEAM_ID equality when we recognize the pattern.
    """
    q = sql_query or ""
    if "team_advanced_" not in q.lower():
        return q

    # "TEAM_NAME" ILIKE '%XYZ%' where XYZ is a 2-3 letter team tag -> use TEAM_ID
    def repl_ilike(m):
        abbr = m.group(1).upper()
        tid = NBA_TEAM_ABBR_TO_TEAM_ID.get(abbr)
        if not tid:
            return m.group(0)
        return f"\"TEAM_ID\" = '{tid}'"

    q = re.sub(
        r'(?i)"TEAM_NAME"\s+ILIKE\s+\'%([A-Za-z]{2,3})%\'',
        repl_ilike,
        q,
    )
    q = re.sub(
        r'(?i)\bTEAM_NAME\s+ILIKE\s+\'%([A-Za-z]{2,3})%\'',
        repl_ilike,
        q,
    )
    return q


def _normalize_team_id_abbrev_to_numeric(sql_query: str) -> str:
    """Replace TEAM_ID = 'DEN'-style filters with numeric NBA team ids when applicable."""
    q = sql_query or ""

    def repl(m):
        abbr = m.group(1).upper()
        tid = NBA_TEAM_ABBR_TO_TEAM_ID.get(abbr)
        if not tid:
            return m.group(0)
        return f'"TEAM_ID" = \'{tid}\''

    def repl_in(m):
        inner = m.group(1)
        if "(" in inner:
            return m.group(0)
        parts = [p.strip() for p in inner.split(",")]
        out_parts = []
        for p in parts:
            mo = re.match(r"^'([A-Za-z]{2,3})'$", p)
            if mo:
                abbr = mo.group(1).upper()
                tid = NBA_TEAM_ABBR_TO_TEAM_ID.get(abbr)
                if tid:
                    out_parts.append(f"'{tid}'")
                    continue
            out_parts.append(p)
        return f'"TEAM_ID" IN ({", ".join(out_parts)})'

    q = re.sub(
        r'(?i)"TEAM_ID"\s*=\s*\'([A-Za-z]{2,3})\'',
        repl,
        q,
    )
    q = re.sub(
        r'(?i)\bTEAM_ID\s*=\s*\'([A-Za-z]{2,3})\'',
        repl,
        q,
    )
    # TEAM_ID IN ('DEN','LAL') style lists using abbreviations
    q = re.sub(r'(?i)"TEAM_ID"\s+IN\s*\(([^)]+)\)', repl_in, q)
    q = re.sub(r'(?i)\bTEAM_ID\s+IN\s*\(([^)]+)\)', repl_in, q)
    return q


def _normalize_advanced_column_case(sql_query: str) -> str:
    """
    Advanced-family tables in this database use uppercase column names.
    Normalize common lowercase advanced identifiers to quoted uppercase.
    """
    q = sql_query or ""
    if not re.search(r'(?i)\b(from|join)\s+(?:public\.)?"?nba_advanced_', q):
        return q

    cols = ["player_name", "team_abbreviation", "ts_pct", "fg_pct", "fg3_pct", "ft_pct", "gp", "min", "pts"]
    out = q
    for c in cols:
        out = re.sub(rf'(?i)(?<!")\b{re.escape(c)}\b(?!")', f'"{c.upper()}"', out)
    return out


def _normalize_team_advanced_and_standings_case(sql_query: str) -> str:
    """
    Normalize known case/value mismatches for team advanced and standings feeds.
    """
    q = sql_query or ""
    out = q

    if "team_advanced_" in out.lower():
        team_cols = [
            "TEAM_ID", "TEAM_NAME", "GP", "W", "L", "W_PCT", "MIN",
            "E_OFF_RATING", "OFF_RATING", "E_DEF_RATING", "DEF_RATING",
            "E_NET_RATING", "NET_RATING", "AST_PCT", "AST_TO", "AST_RATIO",
            "OREB_PCT", "DREB_PCT", "REB_PCT", "TM_TOV_PCT", "EFG_PCT",
            "TS_PCT", "E_PACE", "PACE", "PACE_PER40", "POSS", "PIE",
            "GP_RANK", "W_RANK", "L_RANK", "W_PCT_RANK", "MIN_RANK",
            "OFF_RATING_RANK", "DEF_RATING_RANK", "NET_RATING_RANK",
            "AST_PCT_RANK", "AST_TO_RANK", "AST_RATIO_RANK", "OREB_PCT_RANK",
            "DREB_PCT_RANK", "REB_PCT_RANK", "TM_TOV_PCT_RANK", "EFG_PCT_RANK",
            "TS_PCT_RANK", "PACE_RANK", "PIE_RANK"
        ]
        for c in team_cols:
            out = re.sub(rf'(?i)(?<!")\b{re.escape(c.lower())}\b(?!")', f'"{c}"', out)
            out = re.sub(rf'(?<!")\b{re.escape(c)}\b(?!")', f'"{c}"', out)
        # Team names are usually full strings like "Denver Nuggets"; broaden equals filters.
        out = re.sub(
            r'(?i)"TEAM_NAME"\s*=\s*\'([^\']+)\'',
            lambda m: f"\"TEAM_NAME\" ILIKE '%{m.group(1)}%'",
            out,
        )

    if "nba_standings_" in out.lower():
        standings_cols = ["TeamID", "TeamCity", "TeamName", "WINS", "LOSSES", "WinPCT", "Conference"]
        for c in standings_cols:
            out = re.sub(rf'(?i)(?<!")\b{re.escape(c)}\b(?!")', f'"{c}"', out)
        out = re.sub(r'(?i)(?<!")\bconference\b(?!")', '"Conference"', out)
        out = re.sub(r"(?i)'Eastern'", "'East'", out)
        out = re.sub(r"(?i)'Western'", "'West'", out)

    return out


def _normalize_team_id_full_name_literal_to_team_name(sql_query: str) -> str:
    """
    Generated SQL sometimes compares TEAM_ID to a full team name string.
    Rewrite to TEAM_NAME ILIKE when the RHS is clearly not a numeric id or 3-letter abbrev.
    """
    q = sql_query or ""
    if "team_advanced_" not in q.lower():
        return q

    def repl(m):
        raw = m.group(1)
        if re.fullmatch(r"\d+", raw):
            return m.group(0)
        if re.fullmatch(r"[A-Za-z]{2,3}", raw):
            return m.group(0)
        esc = raw.replace("'", "''")
        return f"\"TEAM_NAME\" ILIKE '%{esc}%'"

    q = re.sub(r'(?i)"TEAM_ID"\s*=\s*\'([^\']+)\'', repl, q)
    return q


def _normalize_all_players_order_by_cast(sql_query: str) -> str:
    """
    all_players_* feeds often store pct columns as text; ORDER BY without CAST sorts lexicographically
    (e.g. '0.412' > '0.9'), producing wrong leaders for FG3% and similar prompts.
    """
    q = sql_query or ""
    if not re.search(r"(?i)\ball_players_", q):
        return q
    # PostgreSQL: SELECT DISTINCT requires ORDER BY expressions to appear in the select list as-is.
    # ORDER BY CAST(fg3_pct ...) then fails; skip CAST so the query runs (sort may stay lexical if col is text).
    if re.search(r"(?is)\bselect\s+distinct\b", q):
        return q
    pct_cols = ("fg3_pct", "fg_pct", "ft_pct", "ts_pct")
    for c in pct_cols:
        q = re.sub(
            rf'(?i)\bORDER\s+BY\s+{re.escape(c)}\b',
            f"ORDER BY CAST({c} AS DOUBLE PRECISION)",
            q,
        )
    return q


def _normalize_known_subquery_and_aggregate_issues(sql_query: str) -> str:
    """
    Normalize recurrent generated-SQL issues:
    - TEAM_ID scalar subquery that returns multiple rows -> IN (subquery)
    - SUM(text_col) on clutch/uppercase feeds -> SUM(CAST(col AS DOUBLE PRECISION))
    """
    q = sql_query or ""

    # Multi-row TEAM_ID subquery should be set membership, not scalar equality.
    q = re.sub(
        r'(?is)"TEAM_ID"\s*=\s*\(\s*SELECT\s+"TEAM_ID"\s+FROM\s+team_advanced_staging',
        '"TEAM_ID" IN (SELECT "TEAM_ID" FROM team_advanced_staging',
        q,
    )
    q = re.sub(
        r'(?is)(?<!")\bteam_name\b(?!")',
        '"TEAM_NAME"',
        q,
    )

    # Cast common stat columns before SUM() when feeds store values as text.
    sum_cast_cols = ["PTS", "REB", "AST", "STL", "BLK", "TOV", "FGM", "FGA", "FG3M", "FG3A", "FTM", "FTA"]
    out = q
    for c in sum_cast_cols:
        out = re.sub(
            rf'(?i)\bSUM\s*\(\s*"{c}"\s*\)',
            f'SUM(CAST("{c}" AS DOUBLE PRECISION))',
            out,
        )
    return out



def get_connection():
    """Get a fresh database connection. Set POSTGRES_DB and POSTGRES_PASSWORD in backend/.env."""
    pw = os.getenv("POSTGRES_PASSWORD")
    if not pw:
        raise RuntimeError(
            "POSTGRES_PASSWORD is not set. Copy backend/.env.example to backend/.env and add credentials."
        )
    return psycopg2.connect(
        host="nba-sdp-project.cs1c0smw8vqa.us-east-1.rds.amazonaws.com",
        port=5432,
        dbname=os.getenv("POSTGRES_DB", "postgres"),
        user="VonLindenthal",
        password=pw,
        sslmode="require",
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

    # sqlglot's Postgres pretty-printer has been observed to collapse "__" inside
    # quoted identifiers in some queries, producing invalid relation names. Keep
    # the original SQL when NBA warehouse-style tables are referenced.
    fragile = (
        "nba__advanced__",
        "nba_advanced_",
        "nba__clutch__",
        "nba_clutch_",
        "nba__hustle__",
        "nba_hustle_",
        "nba__lineups__",
        "nba_lineups_",
        "nba__schedule__",
        "nba_schedule_",
        "nba__standings__",
        "nba_standings_",
    )
    if any(token in sql_query for token in fragile):
        logger.debug("Skipping sqlglot normalize (fragile nba__ identifiers)")
        return sql_query.strip()

    normalized_sql = parsed.sql(dialect="postgres")
    logger.debug("Normalized SQL:\n%s", normalized_sql)
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
    final_query = f"{sql_query} LIMIT {limit};"

    logger.debug("SQL after LIMIT enforcement:\n%s", final_query)

    return final_query

def set_query_timeout(conn, timeout_ms=3000):
    logger.debug("Setting query timeout to %d ms", timeout_ms)
    cursor = conn.cursor()
    cursor.execute(f"SET LOCAL statement_timeout = {timeout_ms};")

def check_query_cost(conn, sql_query, max_cost=100000):
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
def execute_query(conn, sql_query, max_cost=100000, timeout_ms=3000):
    sql_query = normalize_game_log_wl_column(sql_query)
    sql_query = _normalize_advanced_column_case(sql_query)
    sql_query = _normalize_team_id_abbrev_to_numeric(sql_query)
    sql_query = _normalize_team_advanced_and_standings_case(sql_query)
    sql_query = _normalize_team_name_abbrev_mismatch(sql_query)
    sql_query = _normalize_team_id_full_name_literal_to_team_name(sql_query)
    sql_query = _normalize_all_players_order_by_cast(sql_query)
    sql_query = _normalize_known_subquery_and_aggregate_issues(sql_query)
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
