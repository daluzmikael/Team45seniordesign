import os
import logging
import re
# from dotenv import load_dotenv
from openai import OpenAI

from Executer.executor import (
    get_connection,
    get_db_schema,
    #is_safe_sql,
    limit_rows,
    execute_query,
    validate_and_normalize_sql
)

# load_dotenv()
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)
logger.info("OpenAI API key loaded successfully")
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))


def _extract_current_question_text(user_input: str) -> str:
    raw = (user_input or "").strip()
    if not raw:
        return ""
    # If the backend wrapped chat history, use only the explicit current-question segment
    # for intent/routing safeguards.
    match = re.search(r"(?is)\bcurrent question\s*:\s*(.+)$", raw)
    if match:
        return match.group(1).strip()
    return raw


def _is_single_player_profile_request(user_input: str) -> bool:
    q = _extract_current_question_text(user_input).lower()
    has_profile_intent = any(
        k in q for k in ["what were", " stats", "stat ", "show", "profile", "season stats"]
    )
    has_exclusions = any(
        k in q
        for k in [
            "top ",
            "best ",
            "highest",
            "most ",
            "leading ",
            "compare",
            "versus",
            " vs ",
            "between",
            "leaderboard",
            "by season",
            "per season",
            "trend",
            "over time",
            "over the years",
            "through the years",
            "decade",
            "rookie year",
            "from ",
            " to ",
            "career",
        ]
    )
    return has_profile_intent and not has_exclusions


def _extract_bare_year_request(user_input: str):
    q = _extract_current_question_text(user_input).lower()
    # If explicit season range is provided (e.g., 2024-25), do not override.
    if re.search(r"\b(19\d{2}|20\d{2})\s*[-/]\s*(\d{2}|19\d{2}|20\d{2})\b", q):
        return None

    year_match = re.search(r"\b(19\d{2}|20\d{2})\b", q)
    if not year_match:
        return None

    year = int(year_match.group(1))
    is_playoffs = ("playoff" in q) or ("postseason" in q)
    return year, is_playoffs


def _enforce_start_year_table_mapping(sql_query: str, user_input: str) -> str:
    if not sql_query:
        return sql_query
    if " union " in sql_query.lower():
        return sql_query

    req = _extract_bare_year_request(user_input)
    if req is None:
        return sql_query
    year, is_playoffs = req

    if is_playoffs:
        start = year - 1
        end = year
        target = f"all_players_playoffs_{start}_{end}"
        replaced = re.sub(r"(?i)all_players_(regular|playoffs)_\d{4}_\d{4}", target, sql_query)
        return replaced

    start = year
    end = year + 1
    target = f"all_players_regular_{start}_{end}"
    replaced = re.sub(r"(?i)all_players_(regular|playoffs)_\d{4}_\d{4}", target, sql_query)
    return replaced


def _is_advanced_metrics_request(user_input: str) -> bool:
    q = _extract_current_question_text(user_input).lower()
    advanced_terms = [
        "true shooting",
        "ts%",
        "ts pct",
        "ts_pct",
        "efg",
        "efg%",
        "usage rate",
        "usg",
        "off rating",
        "def rating",
        "net rating",
        "pie",
        "advanced stat",
        "advanced metric",
    ]
    return any(term in q for term in advanced_terms)


def _extract_requested_season_window(user_input: str):
    q = _extract_current_question_text(user_input).lower()
    is_playoffs = ("playoff" in q) or ("postseason" in q)

    range_match = re.search(r"\b(19\d{2}|20\d{2})\s*[-/]\s*(\d{2}|19\d{2}|20\d{2})\b", q)
    if range_match:
        start = int(range_match.group(1))
        end_raw = range_match.group(2)
        end = int(f"{str(start)[:2]}{end_raw}") if len(end_raw) == 2 else int(end_raw)
        return start, end, is_playoffs

    bare = _extract_bare_year_request(user_input)
    if bare is not None:
        year, playoffs_flag = bare
        if playoffs_flag:
            return year - 1, year, True
        return year, year + 1, False

    if "this season" in q or "current season" in q or "last season" in q:
        return 2024, 2025, False
    if "this playoff" in q or "current playoff" in q or "last playoff" in q:
        return 2024, 2025, True

    return 2024, 2025, is_playoffs


def _advanced_table_name_for_window(start: int, end: int, is_playoffs: bool) -> str:
    yy = str(end)[-2:]
    if is_playoffs:
        return f"nba__advanced__season_{start}_{yy}__season_type_playoffs__per_mode_p"
    return f"nba__advanced__season_{start}_{yy}__season_type_regular_season__per_"


def _qualified_public_table_ref(table_name: str) -> str:
    # Quote the identifier and qualify schema to avoid search_path mismatches.
    safe = (table_name or "").replace('"', '""')
    return f'public."{safe}"'


def _pick_available_advanced_table(
    schema_description: str, start: int, end: int, is_playoffs: bool
) -> str:
    if not schema_description:
        return _advanced_table_name_for_window(start, end, is_playoffs)

    pattern = re.compile(
        r"nba__advanced__season_(\d{4})_(\d{2})__season_type_(regular_season|playoffs)__[a-z0-9_]+",
        re.IGNORECASE,
    )
    matches = pattern.findall(schema_description)
    if not matches:
        return _advanced_table_name_for_window(start, end, is_playoffs)

    wanted_type = "playoffs" if is_playoffs else "regular_season"
    candidates = []
    for start_s, end_yy_s, season_type in matches:
        if season_type.lower() != wanted_type:
            continue
        try:
            st = int(start_s)
            ed = int(f"{str(st)[:2]}{end_yy_s}")
        except Exception:
            continue
        candidates.append((st, ed))

    if not candidates:
        return _advanced_table_name_for_window(start, end, is_playoffs)

    for st, ed in candidates:
        if st == start and ed == end:
            return _advanced_table_name_for_window(st, ed, is_playoffs)

    older_or_equal = [c for c in candidates if c[0] <= start]
    if older_or_equal:
        best = max(older_or_equal, key=lambda c: c[0])
    else:
        best = max(candidates, key=lambda c: c[0])
    return _advanced_table_name_for_window(best[0], best[1], is_playoffs)


def _enforce_advanced_table_mapping(sql_query: str, user_input: str, schema_description: str = "") -> str:
    if not sql_query or not _is_advanced_metrics_request(user_input):
        return sql_query

    start, end, is_playoffs = _extract_requested_season_window(user_input)
    target = _pick_available_advanced_table(schema_description, start, end, is_playoffs)
    target_ref = _qualified_public_table_ref(target)
    q = sql_query

    # Route any season-summary/game-log source to advanced table family for advanced-stat intents.
    q = re.sub(r"(?i)all_players_(regular|playoffs)_\d{4}_\d{4}", target_ref, q)
    q = re.sub(r"(?i)\bplayer_game_logs\b", target_ref, q)
    q = re.sub(
        r"(?i)nba__advanced__season_\d{4}_\d{2}__season_type_(regular_season|playoffs)__[a-z0-9_]+",
        target_ref,
        q,
    )

    user_q = _extract_current_question_text(user_input).lower()
    is_true_shooting_request = any(k in user_q for k in ["true shooting", "ts%", "ts pct", "ts_pct"])
    if is_true_shooting_request:
        where_clause = None
        where_match = re.search(r"(?is)\bwhere\b(.*?)(\bgroup\s+by\b|\border\s+by\b|\blimit\b|$)", q)
        if where_match:
            maybe = where_match.group(1).strip()
            if "player_name" in maybe.lower():
                where_clause = maybe

        limit = 50
        lim_match = re.search(r"(?i)\blimit\s+(\d+)", q)
        if lim_match:
            try:
                limit = int(lim_match.group(1))
            except Exception:
                limit = 50

        parts = [
            "SELECT player_name, team_abbreviation, CAST(ts_pct AS DOUBLE PRECISION) AS true_shooting_pct",
            f"FROM {target_ref}",
        ]
        if where_clause:
            parts.append(f"WHERE {where_clause}")
        parts.append("ORDER BY true_shooting_pct DESC")
        parts.append(f"LIMIT {limit}")
        q = " ".join(parts) + ";"

    return q


def _columns_for_by_season_question(question_text: str) -> list[str]:
    q = (question_text or "").lower()
    if any(k in q for k in ["shoot", "percentage", "fg%", "3 point", "3p", "true shooting"]):
        return ["fg_pct", "fg3_pct", "ft_pct", "fgm", "fga", "fg3m", "fg3a", "ftm", "fta", "gp"]
    if any(k in q for k in ["rebound", "boards", "glass"]):
        return ["reb", "oreb", "dreb", "reb_rank", "oreb_rank", "dreb_rank", "gp"]
    if any(k in q for k in ["assist", "playmaking", "passing"]):
        return ["ast", "ast_rank", "tov", "tov_rank", "gp"]
    if any(k in q for k in ["block", "rim protection"]):
        return ["blk", "blk_rank", "gp"]
    if any(k in q for k in ["steal", "defense"]):
        return ["stl", "stl_rank", "gp"]
    if any(k in q for k in ["score", "points", "scor", "offense"]):
        return ["pts", "pts_rank", "fg_pct", "fg3_pct", "ft_pct", "gp"]
    return ["pts", "reb", "ast", "fg_pct", "fg3_pct", "ft_pct", "gp"]


def _is_over_time_request(question_text: str) -> bool:
    q = (question_text or "").lower()
    phrases = [
        "by season",
        "per season",
        "each season",
        "season by season",
        "over the years",
        "through the years",
        "across seasons",
        "year by year",
        "trend",
        "over time",
        "over his career",
        "over her career",
        "over their career",
        "throughout his career",
        "throughout her career",
        "throughout their career",
    ]
    if any(p in q for p in phrases):
        return True
    # Handle slight misspellings like "careeer" and flexible "over ... career" phrasing.
    if re.search(r"\bover\b.*\bcaree+r\b", q):
        return True
    if re.search(r"\bthroughout\b.*\bcaree+r\b", q):
        return True
    if re.search(r"\b(per\s+game|per-game)\b.*\bcaree+r\b", q) or re.search(r"\bcaree+r\b.*\b(per\s+game|per-game)\b", q):
        return True
    if "rookie year" in q:
        return True
    if re.search(r"\bfrom\b.+\bto\b\s*(19\d{2}|20\d{2})\b", q):
        return True
    if re.search(r"\b(19\d{2}|20\d{2})s\b", q) or "decade" in q:
        return True
    if re.search(r"\b(highest|best|most|peak)\b.+\bseason\b", q):
        return True
    return re.search(r"\b(19\d{2}|20\d{2})\s*(to|through|thru|-)\s*(19\d{2}|20\d{2})\b", q) is not None


def _is_explicit_total_request(question_text: str) -> bool:
    q = (question_text or "").lower()
    total_terms = [
        "total",
        "totals",
        "sum",
        "combined",
        "overall number",
        "career total",
        "how many in his career",
        "how many in her career",
        "how many in their career",
    ]
    return any(t in q for t in total_terms)


def _rewrite_career_aggregate_to_by_season(sql_query: str, user_input: str) -> str:
    question_text = _extract_current_question_text(user_input)
    q_input = question_text.lower()
    asks_over_time = _is_over_time_request(question_text)
    mentions_career = re.search(r"\bcaree+r\b", q_input) is not None
    wants_total = _is_explicit_total_request(question_text)
    q = sql_query or ""
    q_lower = q.lower()
    has_union_sum_rollup = ("union all" in q_lower) and ("sum(" in q_lower) and ("group by player_name" in q_lower)
    # Default non-total career asks to by-season rows rather than SUM rollups.
    if mentions_career and not wants_total:
        asks_over_time = True
    if has_union_sum_rollup and not wants_total:
        asks_over_time = True
    if not asks_over_time:
        return sql_query

    if "all_players_regular_" not in q_lower and "all_players_playoffs_" not in q_lower:
        return q

    # Build from detected season tables directly (no SUM/COUNT) for stable season-trend output.
    table_matches = re.findall(r"(?i)\b(all_players_(?:regular|playoffs)_(\d{4})_(\d{4}))\b", q)
    if not table_matches:
        return q
    unique_tables: dict[str, tuple[int, int]] = {}
    for table_name, start_s, end_s in table_matches:
        try:
            unique_tables[table_name] = (int(start_s), int(end_s))
        except Exception:
            continue
    if not unique_tables:
        return q

    where_match = re.search(
        r"(?is)\bwhere\b\s*(?P<where>.*?)(\bunion\s+all\b|\bgroup\s+by\b|\border\s+by\b|\blimit\b|$)",
        q,
    )
    if not where_match:
        return q
    where_clause = (where_match.group("where") or "").strip()
    if not where_clause:
        return q

    cols = _columns_for_by_season_question(question_text)
    col_sql = ", ".join(cols)
    sorted_tables = sorted(unique_tables.items(), key=lambda kv: kv[1][0])

    legs = []
    for table_name, (start, end) in sorted_tables:
        season_label = f"{start}-{str(end)[-2:]}"
        legs.append(
            f"SELECT {start} AS season_start, '{season_label}' AS season_label, "
            f"player_name, {col_sql} FROM {table_name} WHERE {where_clause}"
        )

    # For trend slices, always return enough seasons for analyzer to reason over period.
    limit = 50

    rebuilt = (
        f"SELECT DISTINCT season_start, season_label, player_name, {col_sql} "
        f"FROM ({' UNION ALL '.join(legs)}) AS by_season "
        f"ORDER BY season_start ASC LIMIT {limit};"
    )
    return rebuilt


def _ensure_rebounding_leaderboard_columns(sql_query: str, user_input: str) -> str:
    q_input = _extract_current_question_text(user_input).lower()
    asks_top = any(k in q_input for k in ["top ", "best ", "leading ", "leaders", "leaderboard"])
    asks_reb = any(k in q_input for k in ["rebound", "boards", "glass", "rebounding"])
    if not asks_top or not asks_reb:
        return sql_query

    q = sql_query or ""
    select_match = re.search(r"(?is)\bselect\b(?P<select_part>.*?)\bfrom\b", q)
    if not select_match:
        return q

    select_part = select_match.group("select_part")
    start, end = select_match.span("select_part")
    required = ["player_name", "team_abbreviation", "reb", "reb_rank", "oreb", "oreb_rank", "dreb", "dreb_rank", "gp"]
    missing = [col for col in required if not re.search(rf"(?i)\b{re.escape(col)}\b", select_part)]
    if not missing:
        return q

    injected = select_part.rstrip() + ", " + ", ".join(missing) + " "
    q = q[:start] + injected + q[end:]

    if "order by" not in q.lower():
        q = q.rstrip().rstrip(";") + " ORDER BY reb_rank ASC NULLS LAST;"
    return q


def _ensure_assist_leaderboard_columns(sql_query: str, user_input: str) -> str:
    q_input = _extract_current_question_text(user_input).lower()
    asks_top = any(k in q_input for k in ["top ", "best ", "leading ", "leaders", "leaderboard"])
    asks_ast = any(k in q_input for k in ["assist", "playmaker", "passing"])
    if not asks_top or not asks_ast:
        return sql_query

    q = sql_query or ""
    select_match = re.search(r"(?is)\bselect\b(?P<select_part>.*?)\bfrom\b", q)
    if not select_match:
        return q
    select_part = select_match.group("select_part")
    start, end = select_match.span("select_part")
    required = ["player_name", "team_abbreviation", "ast", "ast_rank", "tov", "tov_rank", "gp"]
    missing = [col for col in required if not re.search(rf"(?i)\b{re.escape(col)}\b", select_part)]
    if not missing:
        return q
    injected = select_part.rstrip() + ", " + ", ".join(missing) + " "
    q = q[:start] + injected + q[end:]
    if "order by" not in q.lower():
        q = q.rstrip().rstrip(";") + " ORDER BY ast_rank ASC NULLS LAST;"
    return q


def _ensure_all_players_broad_columns(sql_query: str, user_input: str) -> str:
    q = sql_query or ""
    q_lower = q.lower()
    # Restrict to simple single-season season-summary selects.
    if "all_players_regular_" not in q_lower and "all_players_playoffs_" not in q_lower:
        return q
    # Never inject base-table columns into derived/subquery shapes like by_season.
    if " as by_season" in q_lower or re.search(r"(?is)\bfrom\s*\(", q):
        return q
    if any(k in q_lower for k in [" sum(", " avg(", " count(", " group by ", " union all "]):
        return q

    select_match = re.search(r"(?is)\bselect\b(?P<select_part>.*?)\bfrom\b", q)
    if not select_match:
        return q
    select_part = select_match.group("select_part")
    start, end = select_match.span("select_part")
    required_columns = [
        "player_id", "player_name", "nickname", "team_id", "team_abbreviation", "age", "gp", "w", "l", "w_pct",
        "min", "fgm", "fga", "fg_pct", "fg3m", "fg3a", "fg3_pct", "ftm", "fta", "ft_pct",
        "oreb", "dreb", "reb", "ast", "tov", "stl", "blk", "blka", "pf", "pfd", "pts", "plus_minus",
        "gp_rank", "w_rank", "l_rank", "w_pct_rank", "min_rank", "fgm_rank", "fga_rank", "fg_pct_rank",
        "fg3m_rank", "fg3a_rank", "fg3_pct_rank", "ftm_rank", "fta_rank", "ft_pct_rank", "oreb_rank",
        "dreb_rank", "reb_rank", "ast_rank", "tov_rank", "stl_rank", "blk_rank", "blka_rank", "pf_rank",
        "pfd_rank", "pts_rank", "plus_minus_rank", "dd2", "td3", "dd2_rank", "td3_rank", "team_count",
    ]
    missing = [c for c in required_columns if not re.search(rf"(?i)\b{re.escape(c)}\b", select_part)]
    if not missing:
        return q
    injected = select_part.rstrip() + ", " + ", ".join(missing) + " "
    return q[:start] + injected + q[end:]


def _expand_player_name_filters_for_encoding(sql_query: str) -> str:
    """
    Expand exact full-name ILIKE filters with a robust fallback:
      player_name ILIKE '%First Last%'
    becomes:
      (player_name ILIKE '%First Last%' OR
       (player_name ILIKE '%First%' AND player_name ILIKE '%LastPrefix%'))

    This helps match mojibake/diacritics corruption in DB values
    (e.g., Jokić stored as JokiÄ) without modifying database data.
    """
    if not sql_query:
        return sql_query

    pattern = re.compile(r"(?i)player_name\s+ILIKE\s+'%([^%']+)%'")

    def repl(match: re.Match) -> str:
        raw_name = match.group(1).strip()
        parts = [p for p in raw_name.split() if p]
        if len(parts) < 2:
            return match.group(0)

        first = parts[0]
        last = parts[-1]

        # Keep only letters for prefix logic, but preserve original full-name match too.
        last_clean = re.sub(r"[^A-Za-z]", "", last)
        if len(last_clean) < 3:
            return match.group(0)

        last_prefix = last_clean[:4]
        full_clause = f"player_name ILIKE '%{raw_name}%'"
        fallback_clause = (
            f"(player_name ILIKE '%{first}%' AND player_name ILIKE '%{last_prefix}%')"
        )
        return f"({full_clause} OR {fallback_clause})"

    return pattern.sub(repl, sql_query)


def _ensure_profile_columns_in_sql(sql_query: str, user_input: str) -> str:
    if not _is_single_player_profile_request(user_input):
        return sql_query

    q = sql_query or ""
    q_lower = q.lower()
    # Restrict safeguard to simple single-season summary-table profile selects.
    if "all_players_regular_" not in q_lower and "all_players_playoffs_" not in q_lower:
        return q
    if "player_name ilike" not in q_lower:
        return q
    if any(k in q_lower for k in [" group by ", " union ", "sum("]):
        return q

    select_match = re.search(r"(?is)\bselect\b(?P<select_part>.*?)\bfrom\b", q)
    if not select_match:
        return q

    select_part = select_match.group("select_part")
    start, end = select_match.span("select_part")
    required_columns = [
        "min",
        "fgm",
        "fga",
        "fgm_rank",
        "fga_rank",
        "fg3m_rank",
        "fg3a_rank",
        "ftm_rank",
        "fta_rank",
        "min_rank",
    ]
    missing = []
    for col in required_columns:
        if not re.search(rf"(?i)\b{re.escape(col)}\b", select_part):
            missing.append(col)

    if not missing:
        return q

    injected = select_part.rstrip() + ", " + ", ".join(missing) + " "
    return q[:start] + injected + q[end:]


# 5.5 repair SQL error
def repair_sql_error(original_sql, error_message, schema_description, user_input):
    r_prompt = f"""
The following SQL query failed:

Database schema:
{schema_description}

User request:
"{user_input}"

Failed SQL:
{original_sql}

Database error:
{error_message}

Fix the SQL to match the schema exactly.
Return ONLY a valid PostgreSQL SELECT query.
Do NOT include any additional text or markdown.
"""

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "Return ONLY valid SQL."},
            {"role": "user", "content": r_prompt}
        ],
        temperature=0,
        max_tokens=1500
    )

    fixed_sql = response.choices[0].message.content.strip()
    fixed_sql = fixed_sql.replace("```sql", "").replace("```", "").strip()

    return fixed_sql


# 6. Convert natural language → SQL
def natural_language_to_sql(user_input_param: str):

    conn = get_connection()
    schema_description = get_db_schema(conn)

    prompt = f"""
You are a senior SQL data engineer specializing in NBA statistics databases.
Your ONLY task is to convert a natural language request into a VALID PostgreSQL SELECT query.
You MUST follow every rule below without exception. There is no ambiguity — if a rule applies, follow it exactly.

════════════════════════════════════════════════════════════════════════
SECTION 1: THE TWO TABLE TYPES — UNDERSTAND THEM COMPLETELY
════════════════════════════════════════════════════════════════════════

TYPE A — SEASON SUMMARY TABLES:
  Naming pattern: `all_players_regular_YYYY_YYYY` and `all_players_playoffs_YYYY_YYYY`
  What they contain: One row per player per season. Pre-aggregated season stats.
  IMPORTANT SCHEMA MEANING:
    - `pts`, `reb`, `ast`, `fg_pct`, `fg3_pct`, `ft_pct` are already season-level values for that player.
    - `gp` is games played in that season.
    - `_rank` columns (like `pts_rank`) are precomputed league ranks for that season.
    - Because each player is already one row in a season table, single-season leaderboards should NOT use SUM()+GROUP BY.
  Available regular season tables (oldest to newest):
    all_players_regular_1996_1997, all_players_regular_1997_1998, all_players_regular_1998_1999,
    all_players_regular_1999_2000, all_players_regular_2000_2001, all_players_regular_2001_2002,
    all_players_regular_2002_2003, all_players_regular_2003_2004, all_players_regular_2004_2005,
    all_players_regular_2005_2006, all_players_regular_2006_2007, all_players_regular_2007_2008,
    all_players_regular_2008_2009, all_players_regular_2009_2010, all_players_regular_2010_2011,
    all_players_regular_2011_2012, all_players_regular_2012_2013, all_players_regular_2013_2014,
    all_players_regular_2014_2015, all_players_regular_2015_2016, all_players_regular_2016_2017,
    all_players_regular_2017_2018, all_players_regular_2018_2019, all_players_regular_2019_2020,
    all_players_regular_2020_2021, all_players_regular_2021_2022, all_players_regular_2022_2023,
    all_players_regular_2023_2024, all_players_regular_2024_2025
  Available playoffs tables (oldest to newest):
    all_players_playoffs_2007_2008, all_players_playoffs_2008_2009, all_players_playoffs_2009_2010,
    all_players_playoffs_2010_2011, all_players_playoffs_2011_2012, all_players_playoffs_2012_2013,
    all_players_playoffs_2013_2014, all_players_playoffs_2014_2015, all_players_playoffs_2015_2016,
    all_players_playoffs_2016_2017, all_players_playoffs_2017_2018, all_players_playoffs_2018_2019,
    all_players_playoffs_2019_2020, all_players_playoffs_2020_2021, all_players_playoffs_2021_2022,
    all_players_playoffs_2022_2023, all_players_playoffs_2023_2024, all_players_playoffs_2024_2025

  COLUMNS THAT EXIST in all_players_regular_* AND all_players_playoffs_*:
    player_id, player_name, nickname, team_id, team_abbreviation, age, gp, w, l,
    w_pct, min, fgm, fga, fg_pct, fg3m, fg3a, fg3_pct, ftm, fta, ft_pct,
    oreb, dreb, reb, ast, tov, stl, blk, blka, pf, pfd, pts, plus_minus,
    nba_fantasy_pts, dd2, td3, gp_rank, w_rank, l_rank, w_pct_rank, min_rank,
    fgm_rank, fga_rank, fg_pct_rank, fg3m_rank, fg3a_rank, fg3_pct_rank,
    ftm_rank, fta_rank, ft_pct_rank, oreb_rank, dreb_rank, reb_rank, ast_rank,
    tov_rank, stl_rank, blk_rank, blka_rank, pf_rank, pfd_rank, pts_rank,
    plus_minus_rank, nba_fantasy_pts_rank, dd2_rank, td3_rank, wnba_fantasy_pts,
    wnba_fantasy_pts_rank, team_count

  COLUMNS THAT DO NOT EXIST in season summary tables — NEVER USE THEM:
    ❌ season_id       (the season is encoded in the TABLE NAME itself)
    ❌ game_date       (these are season summaries, not individual games)
    ❌ game_id         (no individual game tracking)
    ❌ matchup         (no opponent info)
    ❌ wl              (w and l are separated into two columns)
    ❌ season_type     (regular vs playoffs is encoded in the TABLE NAME)

TYPE B — GAME LOGS TABLE:
  Table name: `player_game_logs` (only ONE table, not split by year)
  What it contains: One row per player per game. Raw box score per game.
  IMPORTANT: This table is the MOST UP TO DATE data source, containing game logs
  through February 2026. It covers the full 2025-26 season currently in progress.
  Use this table whenever the user wants current, recent, or live-season data.

  COLUMNS THAT EXIST in player_game_logs:
    player_id, player_name, team_abbreviation, game_id, game_date, season_id,
    season_type, matchup, wl, pts, ast, reb, stl, blk, tov, fgm, fga,
    fg3m, fg3a, ftm, fta, min

  COLUMNS THAT DO NOT EXIST in player_game_logs — NEVER USE THEM:
    ❌ fg_pct          (must be calculated: CAST(SUM(fgm) AS DOUBLE PRECISION) / NULLIF(SUM(fga), 0))
    ❌ fg3_pct         (must be calculated: CAST(SUM(fg3m) AS DOUBLE PRECISION) / NULLIF(SUM(fg3a), 0))
    ❌ ft_pct          (must be calculated: CAST(SUM(ftm) AS DOUBLE PRECISION) / NULLIF(SUM(fta), 0))
    ❌ age             (not tracked per game)
    ❌ gp              (not a column — count rows instead: COUNT(*) AS games_played)
    ❌ oreb            (offensive rebounds not tracked separately)
    ❌ dreb            (defensive rebounds not tracked separately)
    ❌ nickname        (not in this table)
    ❌ plus_minus      (not in this table)
    ❌ dd2, td3        (not in this table)
    ❌ any _rank columns (none of the rank columns exist here)
    ❌ team_id         (not in this table)
    ❌ team_count      (not in this table)

TYPE C — EXTENDED NBA DATA FAMILIES (from current_working_data schema):
  These tables exist and should be used when user intent clearly matches them:
  - `nba__advanced__...`  (advanced metrics / impact context)
  - `nba__clutch__...`    (late-game / clutch situations)
  - `nba__hustle__...`    (hustle events: deflections, contested stats, etc.)
  - `nba__lineups__...`   (lineup combinations and lineup performance)
  - `nba__schedule__...`  (game schedules)
  - `nba__standings__...` (team standings / rank / records)

  IMPORTANT:
  - These tables are highly structured by name (season, season_type, per_mode, endpoint naming).
  - ALWAYS rely on DATABASE SCHEMA below to pick exact columns; never invent columns.
  - If user asks for clutch/hustle/lineup/schedule/standings, do NOT force all_players_regular_* or player_game_logs.
  - Use table-name pattern matching by intent first, then schema-confirmed columns.

════════════════════════════════════════════════════════════════════════
SECTION 2: TABLE SELECTION RULES — FOLLOW IN ORDER, FIRST MATCH WINS
════════════════════════════════════════════════════════════════════════

RULE 0 — INTENT ROUTING FOR EXTENDED TABLE FAMILIES:
  If question clearly targets one of these domains, use that family first:
  - "clutch", "in close games", "last 5 minutes"        → `nba__clutch__...`
  - "hustle", "deflections", "box outs", "contested"    → `nba__hustle__...`
  - "lineup", "5-man unit", "best lineup", "on/off 5"   → `nba__lineups__...`
  - "schedule", "next games", "calendar"                → `nba__schedule__...`
  - "standings", "seed", "conference rank", "record"    → `nba__standings__...`
  - "advanced metrics", "advanced stats profile"         → `nba__advanced__...`
  Use all_players_regular_*/playoffs_* only when the question is season-summary player stats.
  Use player_game_logs only when question is game-by-game recency/log context.

RULE 1 — MOST RECENT PLAYOFF PERFORMANCE (most common case):
  Trigger phrases: "playoff performance", "playoffs", "analyze playoffs", "postseason",
                   "how did X do in the playoffs", "X playoff stats", "X in the playoffs"
  With NO specific year or "all time" mentioned:
  → ALWAYS use: `all_players_playoffs_2024_2025`
  → ALWAYS use SUM() aggregation with GROUP BY player_name
  → NEVER use player_game_logs for this
  → NEVER guess an older year like 2018_2019 or 2023_2024
  Example question: "Analyze Giannis playoff performance"
  Example question: "How did Jayson Tatum do in the playoffs"
  Example question: "Show me Steph Curry's playoff stats"
  Correct table: all_players_playoffs_2024_2025

RULE 2 — MOST RECENT REGULAR SEASON PERFORMANCE (season summary):
  Trigger phrases: "season stats", "season averages", "how did X do this season",
                   "season performance", "season totals", top scorers, leaderboards,
                   rank-based questions, questions needing oreb/dreb/plus_minus/age/gp
  With NO specific year mentioned:
  → Use: `all_players_regular_2024_2025`
  → Use this table when rank columns (_rank) or columns like oreb, dreb, age, gp are needed
  → NEVER use player_game_logs for leaderboards or rank-based questions
  Example question: "Who are the top 10 scorers this season"
  Example question: "Show me the league leaders in assists"
  Correct table: all_players_regular_2024_2025

RULE 3 — CURRENT FORM / RECENT ACTIVITY (use player_game_logs):
  Trigger phrases: "lately", "recently", "how is X playing", "current form",
                   "this season so far", "how has X been", "is X hot", "is X cold",
                   "last X games", "past X games", "recent games", "game log",
                   "game by game", "hot streak", "cold streak", "this week",
                   "last week", "last night", "tonight", "last month",
                   "matchup history", "vs [team]", "against [team]"
  → ALWAYS use player_game_logs — it has data through February 2026
  → For 2025-26 season: WHERE season_id = '22025' AND season_type = 'Regular Season'
  → For 2025 playoffs: WHERE season_id = '22025' AND season_type = 'Playoffs'
  → ALWAYS ORDER BY game_date DESC when recency matters
  → ALWAYS calculate percentages — never reference fg_pct, fg3_pct, ft_pct directly
  → Use LIMIT to restrict to the number of games requested (e.g., last 10 → LIMIT 10)
  Example question: "Show me LeBron's last 10 games"
  Example question: "How has Steph been playing lately"
  Example question: "Is Giannis on a hot streak"
  Correct table: player_game_logs WHERE season_id = '22025'

RULE 4 — SPECIFIC YEAR OR SEASON REQUESTED:
  If user mentions a specific year like "2019", "2022-23", "last year", "2018 playoffs":
  → Map to the correct table using this logic:
      START-YEAR RULE (regular season): a bare year means the season that STARTS that year.
        "2020" or "2020 season"        → all_players_regular_2020_2021
        "2016 season"                  → all_players_regular_2016_2017
      PLAYOFF EXCEPTION: a playoff year refers to playoffs at the END of that season.
        "2016 playoffs"                → all_players_playoffs_2015_2016
        "2020 playoffs"                → all_players_playoffs_2019_2020
      Explicit ranges still map directly by start and end:
        "2018-19 playoffs"             → all_players_playoffs_2018_2019
        "2022-23 season"               → all_players_regular_2022_2023
      Relative references:
        "last season" (current year is 2026)   → all_players_regular_2024_2025
  → Table name format is ALWAYS: all_players_[regular|playoffs]_STARTYEAR_ENDYEAR
  → The ENDYEAR is always STARTYEAR + 1
  Example question: "How did Kobe do in the 2009 playoffs"
  Correct table: all_players_playoffs_2008_2009

RULE 5 — CAREER / ALL TIME STATS:
  Trigger phrases: "career", "all time", "entire career", "over his career",
                   "throughout his career", "historically", "all seasons"
  → Use UNION ALL across ALL available yearly tables for that player's era
  → NEVER use player_game_logs for career stats
  → Wrap in a subquery and aggregate with SUM() or AVG() and GROUP BY player_name
  Example question: "Show me LeBron's career regular season stats"
  → UNION ALL across all_players_regular_2003_2004 through all_players_regular_2024_2025

RULE 5B — OVER-TIME / BY-SEASON TRENDS (NOT CAREER AGGREGATE):
  Trigger phrases: "by season", "per season", "season by season", "over the years",
                   "through the years", "year by year", "across seasons", "trend over time",
                   "over his career" / "throughout his career" when asking for rate stats,
                   "rookie year to YYYY", "2010s decade", or explicit ranges like "from 2012 to 2024"
  → Use UNION ALL across relevant season tables, but return ONE ROW PER SEASON (no rollup).
  → Do NOT use SUM(), AVG(), COUNT(*), or GROUP BY player_name for this intent.
  → If model produced SUM()+GROUP BY over UNION for these asks, rewrite to per-season direct columns.
  → Pull per-season columns directly from each season table (including gp for games played).
  Example question: "Show me LeBron's blocks per season"
  → Return season_start, season_label, player_name, blk, blk_rank, gp by season order.

RULE 6 — COMPARING TWO OR MORE PLAYERS (same era):
  → Use the same season summary table for both players in one query
  → Use OR with ILIKE for multiple players:
     player_name ILIKE '%LeBron%' OR player_name ILIKE '%Curry%'
  → NEVER use player_game_logs for season-level comparisons
  Example question: "Compare LeBron and Curry this season"
  Correct table: all_players_regular_2024_2025

RULE 7 — COMPARING TWO PLAYERS (different eras / career):
  → Use UNION ALL — one SELECT per player from their respective era tables
  → Wrap in outer query to aggregate
  Example question: "Compare LeBron and Jordan career stats"
  → LeBron from all_players_regular_2003_2004 through 2024_2025
  → Jordan from all_players_regular_1996_1997 through 2002_2003

RULE 8 — TOP PLAYERS / LEADERBOARD QUESTIONS:
  → Use season summary tables only — they have pre-built rank columns
  → NEVER use player_game_logs for rankings
  → For single-season leaderboard questions, query ONE season table directly.
  → Do NOT use SUM(), GROUP BY, or UNION unless the user explicitly asks for career/all-time across seasons.
  → If the relevant _rank column exists, use it for ordering.
  → For top/best scorers, ALWAYS ORDER BY `pts_rank ASC NULLS LAST` (not by SUM(pts)).
  → If a _rank column is not selected, order by the relevant stat DESC (example: pts DESC).
  → For top scorers specifically, use `pts` (and optionally `pts_rank`) from that season table.
  → If the user asks for "top scorers", "best scorers", or "leading scorers":
      - SELECT from one `all_players_regular_YYYY_YYYY` table only
      - include `player_name, team_abbreviation, pts, pts_rank, gp, fg_pct, fg3_pct, fg3m, fg3a, ftm, fta, ft_pct`
      - use DISTINCT to prevent duplicate player rows when source data has repeats
      - ORDER BY `pts_rank ASC NULLS LAST`
      - use LIMIT requested by user; if no number is provided, default to LIMIT 5
      - IMPORTANT: These extra scoring-context columns are for follow-up questions;
        keep the primary response concise, but still return them in query results.
  Example question: "Who are the top 10 scorers this season"
  Correct: SELECT player_name, team_abbreviation, pts, pts_rank, gp, fg_pct, fg3_pct
           FROM all_players_regular_2024_2025
           ORDER BY pts_rank ASC NULLS LAST
           LIMIT 10

RULE 9 — PLAYER DID NOT PLAY / ZERO ROWS RETURNED:
  → Do NOT switch tables or guess a different year
  → Return the query as-is and let the application handle the empty result
  → A player not appearing in a playoffs table means they did not make the playoffs that year

RULE 10 — WHEN player_game_logs IS BETTER THAN SEASON SUMMARY TABLES:
  player_game_logs is updated through February 2026 and is the freshest data available.
  Prefer it over season summary tables when:
  - The user wants anything about the current 2025-26 season on a game-by-game level
  - The user uses words like "lately", "recently", "now", "currently", "this year so far"
  - The user wants "last X games" regardless of season
  - The user wants game dates, opponents, win/loss results
  HOWEVER, keep using all_players_regular_2024_2025 when:
  - The user wants season rankings or leaderboards (needs _rank columns)
  - The user needs oreb, dreb, plus_minus, age, or gp columns
  - The user asks for season shooting percentage columns directly (fg_pct etc.)

RULE 11 — SINGLE PLAYER GENERAL STATS PROFILE (season summary):
  Trigger phrases: "what were X stats", "X stats in 20YY", "show X season stats",
                   "player profile", "general stats"
  If this is for one player and one season table:
  → Use ONE `all_players_regular_YYYY_YYYY` table (or playoffs table only if user explicitly says playoffs)
  → Do NOT use SUM(), GROUP BY, or UNION
  → Return a broad stat set for downstream profile tables:
     player_name, team_abbreviation, age, gp, min, w_pct,
     pts, reb, ast, tov, stl, blk, pf, plus_minus, fgm, fga,
     fg_pct, fg3_pct, ft_pct, fg3m, fg3a, ftm, fta,
     pts_rank, fg_pct_rank, fg3_pct_rank, ft_pct_rank, fgm_rank, fga_rank, fg3m_rank, fg3a_rank, ftm_rank, fta_rank, min_rank,
     reb_rank, dreb_rank, oreb_rank, ast_rank, tov_rank, stl_rank, blk_rank, pf_rank,
     dreb, oreb, dd2, td3, dd2_rank, td3_rank
  → Use DISTINCT if needed to avoid duplicate player rows

════════════════════════════════════════════════════════════════════════
SECTION 3: MANDATORY QUERY CONSTRUCTION RULES
════════════════════════════════════════════════════════════════════════

PLAYER NAME MATCHING:
  - ALWAYS use ILIKE with wildcards on BOTH sides: player_name ILIKE '%Giannis%'
  - For full names use: player_name ILIKE '%LeBron James%'
  - NEVER use exact match (=) for player names
  - NEVER use ILIKE 'Jordan%' — this matches Jordan Poole, DeAndre Jordan, etc.
  - For last-name-only queries use a leading space: player_name ILIKE '% Harris%'
    to reduce false matches like "Gary Harris" when searching just "Harris"
  - Expand ALL nicknames to full names before searching:
      "Steph" or "Steph Curry"   → player_name ILIKE '%Stephen Curry%' OR player_name ILIKE '%Steph Curry%'
      "Bron" or "King James"     → player_name ILIKE '%LeBron James%'
      "Greek Freak"              → player_name ILIKE '%Giannis%'
      "KD"                       → player_name ILIKE '%Kevin Durant%'
      "AD"                       → player_name ILIKE '%Anthony Davis%'
      "Kawhi"                    → player_name ILIKE '%Kawhi Leonard%'
      "CP3"                      → player_name ILIKE '%Chris Paul%'
      "Dame"                     → player_name ILIKE '%Damian Lillard%'
      "Russ"                     → player_name ILIKE '%Russell Westbrook%'
      "PG" or "PG13"             → player_name ILIKE '%Paul George%'

AGGREGATION RULES for season summary tables:
  - ONLY use SUM()+GROUP BY when combining MULTIPLE rows per player
    (examples: UNION ALL across many seasons for career stats, or other explicit multi-season rollups).
  - For SINGLE-SEASON season-summary queries (one all_players_regular_YYYY_YYYY table),
    SELECT columns directly and DO NOT use SUM() or GROUP BY.
  - If percentages already exist in season summary tables (`fg_pct`, `fg3_pct`, `ft_pct`), select them directly.
  - Only calculate percentages via SUM() when aggregating multiple rows per player:
      (CAST(SUM(fgm) AS DOUBLE PRECISION) / NULLIF(SUM(fga), 0)) AS fg_pct
      (CAST(SUM(fg3m) AS DOUBLE PRECISION) / NULLIF(SUM(fg3a), 0)) AS fg3_pct
      (CAST(SUM(ftm) AS DOUBLE PRECISION) / NULLIF(SUM(fta), 0)) AS ft_pct

STANDARD PLAYER PERFORMANCE SELECT BLOCK:
  Use this exact block ONLY for career/all-time or explicit multi-season aggregation:
    player_name,
    SUM(pts)  AS total_pts,
    SUM(reb)  AS total_reb,
    SUM(ast)  AS total_ast,
    SUM(stl)  AS total_stl,
    SUM(blk)  AS total_blk,
    SUM(tov)  AS total_tov,
    SUM(fgm)  AS total_fgm,
    SUM(fga)  AS total_fga,
    SUM(fg3m) AS total_fg3m,
    SUM(fg3a) AS total_fg3a,
    SUM(ftm)  AS total_ftm,
    SUM(fta)  AS total_fta,
    (CAST(SUM(fgm)  AS DOUBLE PRECISION) / NULLIF(SUM(fga),  0)) AS fg_pct,
    (CAST(SUM(fg3m) AS DOUBLE PRECISION) / NULLIF(SUM(fg3a), 0)) AS fg3_pct,
    (CAST(SUM(ftm)  AS DOUBLE PRECISION) / NULLIF(SUM(fta),  0)) AS ft_pct

STANDARD SINGLE-SEASON LEADERBOARD BLOCK (NO AGGREGATION):
  Use this for questions like "top scorers in 2001-02", "best scorers this season", "league leaders in points":
    player_name,
    team_abbreviation,
    pts,
    pts_rank,
    gp,
    fg_pct,
    fg3_pct,
    fg3m,
    fg3a,
    ftm,
    fta,
    ft_pct
  FROM one season table only
  ORDER BY pts_rank ASC NULLS LAST (or pts DESC when rank unavailable)

STANDARD GAME LOG SELECT BLOCK:
  Use this exact block when querying player_game_logs for recent/current games:
    player_name,
    game_date,
    matchup,
    wl,
    pts,
    reb,
    ast,
    stl,
    blk,
    tov,
    fgm,
    fga,
    fg3m,
    fg3a,
    ftm,
    fta,
    min,
    (CAST(fgm  AS DOUBLE PRECISION) / NULLIF(fga,  0)) AS fg_pct,
    (CAST(fg3m AS DOUBLE PRECISION) / NULLIF(fg3a, 0)) AS fg3_pct,
    (CAST(ftm  AS DOUBLE PRECISION) / NULLIF(fta,  0)) AS ft_pct

DIVISION SAFETY:
  - ALWAYS wrap division denominators with NULLIF(..., 0)
  - ALWAYS cast numerator to DOUBLE PRECISION before dividing
  - NEVER do raw division like fgm / fga — always use the safe pattern above

GENERAL:
  - ALWAYS add LIMIT 50 to every query unless the user specifies a different number
  - NEVER use SELECT * — always name columns explicitly
  - NEVER invent column names that are not listed in this prompt
  - NEVER add ORDER BY game_date to season summary tables (game_date does not exist there)
  - NEVER add WHERE season_id = ... to season summary tables (season_id does not exist there)
  - NEVER add WHERE season_type = ... to season summary tables (season_type does not exist there)
  - If a question is ambiguous between recency and season summary, default to player_game_logs
    with season_id = '22025' since it is the most current data available

════════════════════════════════════════════════════════════════════════
SECTION 4: SEASON AND YEAR REFERENCE MAP
════════════════════════════════════════════════════════════════════════

  "current season" or no year specified (regular)  → all_players_regular_2024_2025
  "current playoffs" or no year specified (playoff) → all_players_playoffs_2024_2025
  Bare year uses START-YEAR mapping for regular season:
  "2020"                                            → all_players_regular_2020_2021
  "2018"                                            → all_players_regular_2018_2019
  "2016"                                            → all_players_regular_2016_2017

  Playoff year uses END-YEAR mapping:
  "2020 playoffs"                                   → all_players_playoffs_2019_2020
  "2016 playoffs"                                   → all_players_playoffs_2015_2016

  "last season" / "2024-25"                         → all_players_regular_2024_2025
  "2023-24" / "last year"                           → all_players_regular_2023_2024
  "2022-23"                                         → all_players_regular_2022_2023
  "2021-22"                                         → all_players_regular_2021_2022
  "2020-21"                                         → all_players_regular_2020_2021
  "bubble" / "2019-20"                              → all_players_regular_2019_2020
  "2018-19"                                         → all_players_regular_2018_2019
  "2017-18"                                         → all_players_regular_2017_2018
  "2016-17"                                         → all_players_regular_2016_2017
  "2015-16"                                         → all_players_regular_2015_2016

  season_id values inside player_game_logs:
    2025-26 season (CURRENT — use this by default): '22025'
    2024-25 season:                                 '22024'
    2023-24 season:                                 '22023'
    2022-23 season:                                 '22022'
  season_type values (EXACT strings, case-sensitive):
    'Regular Season'
    'Playoffs'

════════════════════════════════════════════════════════════════════════
SECTION 5: WORKED EXAMPLES OF CORRECT QUERIES
════════════════════════════════════════════════════════════════════════

Q: "Analyze Giannis playoff performance"
→ RULE 1. No year. Use all_players_playoffs_2024_2025.
SELECT player_name,
  SUM(pts) AS total_pts, SUM(reb) AS total_reb, SUM(ast) AS total_ast,
  SUM(stl) AS total_stl, SUM(blk) AS total_blk, SUM(tov) AS total_tov,
  SUM(fgm) AS total_fgm, SUM(fga) AS total_fga,
  SUM(fg3m) AS total_fg3m, SUM(fg3a) AS total_fg3a,
  SUM(ftm) AS total_ftm, SUM(fta) AS total_fta,
  (CAST(SUM(fgm)  AS DOUBLE PRECISION) / NULLIF(SUM(fga),  0)) AS fg_pct,
  (CAST(SUM(fg3m) AS DOUBLE PRECISION) / NULLIF(SUM(fg3a), 0)) AS fg3_pct,
  (CAST(SUM(ftm)  AS DOUBLE PRECISION) / NULLIF(SUM(fta),  0)) AS ft_pct
FROM all_players_playoffs_2024_2025
WHERE player_name ILIKE '%Giannis%'
GROUP BY player_name LIMIT 50;

Q: "Analyze Garry Harris playoff performance"
→ RULE 1. No year. Use all_players_playoffs_2024_2025.
SELECT player_name,
  SUM(pts) AS total_pts, SUM(reb) AS total_reb, SUM(ast) AS total_ast,
  SUM(stl) AS total_stl, SUM(blk) AS total_blk, SUM(tov) AS total_tov,
  SUM(fgm) AS total_fgm, SUM(fga) AS total_fga,
  SUM(fg3m) AS total_fg3m, SUM(fg3a) AS total_fg3a,
  SUM(ftm) AS total_ftm, SUM(fta) AS total_fta,
  (CAST(SUM(fgm)  AS DOUBLE PRECISION) / NULLIF(SUM(fga),  0)) AS fg_pct,
  (CAST(SUM(fg3m) AS DOUBLE PRECISION) / NULLIF(SUM(fg3a), 0)) AS fg3_pct,
  (CAST(SUM(ftm)  AS DOUBLE PRECISION) / NULLIF(SUM(fta),  0)) AS ft_pct
FROM all_players_playoffs_2024_2025
WHERE player_name ILIKE '%Garry Harris%'
GROUP BY player_name LIMIT 50;

Q: "How did Giannis do in the 2019 playoffs"
→ RULE 4. Year specified: 2019 → all_players_playoffs_2018_2019.
SELECT player_name,
  SUM(pts) AS total_pts, SUM(reb) AS total_reb, SUM(ast) AS total_ast,
  (CAST(SUM(fgm)  AS DOUBLE PRECISION) / NULLIF(SUM(fga),  0)) AS fg_pct,
  (CAST(SUM(fg3m) AS DOUBLE PRECISION) / NULLIF(SUM(fg3a), 0)) AS fg3_pct,
  (CAST(SUM(ftm)  AS DOUBLE PRECISION) / NULLIF(SUM(fta),  0)) AS ft_pct
FROM all_players_playoffs_2018_2019
WHERE player_name ILIKE '%Giannis%'
GROUP BY player_name LIMIT 50;

Q: "Show me LeBron's last 10 games"
→ RULE 3. Recency. Use player_game_logs, season_id = '22025', ORDER BY game_date DESC.
SELECT player_name, game_date, matchup, wl, pts, reb, ast, stl, blk, tov,
  fgm, fga, fg3m, fg3a, ftm, fta, min,
  (CAST(fgm  AS DOUBLE PRECISION) / NULLIF(fga,  0)) AS fg_pct,
  (CAST(fg3m AS DOUBLE PRECISION) / NULLIF(fg3a, 0)) AS fg3_pct,
  (CAST(ftm  AS DOUBLE PRECISION) / NULLIF(fta,  0)) AS ft_pct
FROM player_game_logs
WHERE player_name ILIKE '%LeBron James%'
  AND season_id = '22025'
ORDER BY game_date DESC LIMIT 10;

Q: "How has Steph been playing lately"
→ RULE 3 / RULE 10. Recency keyword. Use player_game_logs, season_id = '22025'.
SELECT player_name, game_date, matchup, wl, pts, reb, ast,
  fgm, fga, fg3m, fg3a,
  (CAST(fgm  AS DOUBLE PRECISION) / NULLIF(fga,  0)) AS fg_pct,
  (CAST(fg3m AS DOUBLE PRECISION) / NULLIF(fg3a, 0)) AS fg3_pct
FROM player_game_logs
WHERE (player_name ILIKE '%Stephen Curry%' OR player_name ILIKE '%Steph Curry%')
  AND season_id = '22025'
  AND season_type = 'Regular Season'
ORDER BY game_date DESC LIMIT 15;

Q: "Compare LeBron and Curry this season"
→ RULE 6. Two players, same era, season summary. Use all_players_regular_2024_2025.
SELECT player_name,
  SUM(pts) AS total_pts, SUM(reb) AS total_reb, SUM(ast) AS total_ast,
  (CAST(SUM(fgm)  AS DOUBLE PRECISION) / NULLIF(SUM(fga),  0)) AS fg_pct,
  (CAST(SUM(fg3m) AS DOUBLE PRECISION) / NULLIF(SUM(fg3a), 0)) AS fg3_pct,
  (CAST(SUM(ftm)  AS DOUBLE PRECISION) / NULLIF(SUM(fta),  0)) AS ft_pct
FROM all_players_regular_2024_2025
WHERE player_name ILIKE '%LeBron James%' OR player_name ILIKE '%Stephen Curry%'
GROUP BY player_name LIMIT 50;

Q: "What were Kevin Durant's stats 2015"
→ RULE 4 + RULE 11. Single-player season profile, no aggregation.
SELECT DISTINCT player_name, team_abbreviation, age, gp, min, w_pct,
  pts, reb, ast, tov, stl, blk, pf, plus_minus, fgm, fga,
  fg_pct, fg3_pct, ft_pct, fg3m, fg3a, ftm, fta,
  pts_rank, fg_pct_rank, fg3_pct_rank, ft_pct_rank, fgm_rank, fga_rank, fg3m_rank, fg3a_rank, ftm_rank, fta_rank, min_rank,
  reb_rank, dreb_rank, oreb_rank, ast_rank, tov_rank, stl_rank, blk_rank, pf_rank,
  dreb, oreb, dd2, td3, dd2_rank, td3_rank
FROM all_players_regular_2015_2016
WHERE player_name ILIKE '%Kevin Durant%'
LIMIT 50;

Q: "Who are the top 10 scorers this season"
→ RULE 8. Leaderboard. Use all_players_regular_2024_2025.
SELECT player_name, team_abbreviation, pts, pts_rank, gp, fg_pct, fg3_pct, fg3m, fg3a, ftm, fta, ft_pct
FROM all_players_regular_2024_2025
ORDER BY pts_rank ASC NULLS LAST LIMIT 10;

Q: "Who are the best scorers from 2000-2001"
→ RULE 4 + RULE 8. Specific season leaderboard from one table, no aggregation.
SELECT DISTINCT player_name, team_abbreviation, pts, pts_rank, gp, fg_pct, fg3_pct, fg3m, fg3a, ftm, fta, ft_pct
FROM all_players_regular_2000_2001
ORDER BY pts_rank ASC NULLS LAST LIMIT 5;

Q: "Show me Steph Curry's career stats"
→ RULE 5. Career = UNION ALL across all yearly tables.
SELECT player_name,
  SUM(pts) AS total_pts, SUM(reb) AS total_reb, SUM(ast) AS total_ast,
  (CAST(SUM(fgm)  AS DOUBLE PRECISION) / NULLIF(SUM(fga),  0)) AS fg_pct,
  (CAST(SUM(fg3m) AS DOUBLE PRECISION) / NULLIF(SUM(fg3a), 0)) AS fg3_pct
FROM (
  SELECT player_name, pts, reb, ast, fgm, fga, fg3m, fg3a FROM all_players_regular_2012_2013 WHERE player_name ILIKE '%Stephen Curry%'
  UNION ALL
  SELECT player_name, pts, reb, ast, fgm, fga, fg3m, fg3a FROM all_players_regular_2013_2014 WHERE player_name ILIKE '%Stephen Curry%'
  UNION ALL
  SELECT player_name, pts, reb, ast, fgm, fga, fg3m, fg3a FROM all_players_regular_2014_2015 WHERE player_name ILIKE '%Stephen Curry%'
  UNION ALL
  SELECT player_name, pts, reb, ast, fgm, fga, fg3m, fg3a FROM all_players_regular_2024_2025 WHERE player_name ILIKE '%Stephen Curry%'
) AS career GROUP BY player_name LIMIT 50;

Q: "Is Giannis on a hot streak"
→ RULE 3. Streak = game log recency. Use player_game_logs.
SELECT player_name, game_date, matchup, wl, pts, reb, ast,
  (CAST(fgm  AS DOUBLE PRECISION) / NULLIF(fga,  0)) AS fg_pct
FROM player_game_logs
WHERE player_name ILIKE '%Giannis%'
  AND season_id = '22025'
  AND season_type = 'Regular Season'
ORDER BY game_date DESC LIMIT 10;

Q: "Compare LeBron and Jordan career stats"
→ RULE 7. Different eras. UNION ALL per player.
SELECT player_name,
  SUM(pts) AS total_pts, SUM(reb) AS total_reb, SUM(ast) AS total_ast,
  (CAST(SUM(fgm)  AS DOUBLE PRECISION) / NULLIF(SUM(fga),  0)) AS fg_pct
FROM (
  SELECT player_name, pts, reb, ast, fgm, fga FROM all_players_regular_1996_1997 WHERE player_name ILIKE '%Michael Jordan%'
  UNION ALL
  SELECT player_name, pts, reb, ast, fgm, fga FROM all_players_regular_1997_1998 WHERE player_name ILIKE '%Michael Jordan%'
  UNION ALL
  SELECT player_name, pts, reb, ast, fgm, fga FROM all_players_regular_2003_2004 WHERE player_name ILIKE '%LeBron James%'
  UNION ALL
  SELECT player_name, pts, reb, ast, fgm, fga FROM all_players_regular_2024_2025 WHERE player_name ILIKE '%LeBron James%'
) AS combined GROUP BY player_name LIMIT 50;

════════════════════════════════════════════════════════════════════════
SECTION 6: COMMON MISTAKES — NEVER DO THESE
════════════════════════════════════════════════════════════════════════

❌ SELECT season_id FROM all_players_playoffs_2024_2025    -- does not exist in summary tables
❌ SELECT game_date FROM all_players_regular_2024_2025     -- does not exist in summary tables
❌ SELECT fg_pct FROM player_game_logs                     -- does not exist, must calculate
❌ SELECT oreb FROM player_game_logs                       -- does not exist in game logs
❌ WHERE season_type = 'Playoffs' on a summary table       -- column does not exist there
❌ WHERE season_id = '22025' on a summary table            -- column does not exist there
❌ player_name ILIKE 'Jordan%'                             -- matches wrong players
❌ player_name ILIKE 'Harris'                              -- missing wildcards
❌ fgm / fga                                               -- unsafe, use NULLIF
❌ SELECT * FROM any table                                  -- always name columns explicitly
❌ Using all_players_playoffs_2018_2019 when no year given  -- always default to 2024_2025
❌ Using player_game_logs for "analyze playoff performance" -- use season summary tables
❌ AVG(fg_pct) from season summary tables                  -- use SUM(fgm)/SUM(fga) instead
❌ ORDER BY game_date on a season summary table            -- game_date does not exist there
❌ WHERE season_id = '22024' for current 2025-26 games     -- current season is '22025'
❌ Using all_players_regular_2024_2025 for "last X games"  -- no game_date column there
❌ Assuming player_game_logs is outdated                   -- it has data through Feb 2026
❌ SELECT gp FROM player_game_logs                         -- use COUNT(*) AS games_played

════════════════════════════════════════════════════════════════════════
OUTPUT FORMAT
════════════════════════════════════════════════════════════════════════
Return ONLY the raw SQL query.
No explanation. No markdown. No backticks. No comments. No preamble.
The query must be directly executable in PostgreSQL as-is.

DATABASE SCHEMA:
{schema_description}

USER REQUEST:
{user_input_param}

Generate the SQL:"""

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are a SQL query generator. Return ONLY valid SQL queries."},
                {"role": "user", "content": prompt}
            ],
            temperature=0,
            max_tokens=1500
        )

        sql_query = response.choices[0].message.content.strip()
        sql_query = sql_query.replace("```sql", "").replace("```", "").strip()
        logger.debug("Generated SQL from model:\n%s", sql_query)

    except Exception as e:
        logger.error("OpenAI API error: %s", e)
        return None

    sql_query = _enforce_start_year_table_mapping(sql_query, user_input_param)
    sql_query = _enforce_advanced_table_mapping(sql_query, user_input_param, schema_description)
    sql_query = _rewrite_career_aggregate_to_by_season(sql_query, user_input_param)
    sql_query = _ensure_rebounding_leaderboard_columns(sql_query, user_input_param)
    sql_query = _ensure_assist_leaderboard_columns(sql_query, user_input_param)
    sql_query = _ensure_all_players_broad_columns(sql_query, user_input_param)
    sql_query = _expand_player_name_filters_for_encoding(sql_query)
    sql_query = _ensure_profile_columns_in_sql(sql_query, user_input_param)
    sql_query = limit_rows(sql_query)

    try:
        sql_query = validate_and_normalize_sql(sql_query)
    except ValueError as e:
        logger.error("Validation error: %s", e)
        return None

    max_attempts = 3

    for attempt in range(max_attempts):
        try:
            logger.debug("Attempt %d executing query...", attempt + 1)
            logger.info("Final SQL being executed:\n%s", sql_query)
            return execute_query(conn, sql_query)

        except Exception as e:
            error_message = str(e)
            logger.error("SQL execution error: %s", error_message)

            if any(keyword in error_message.lower()
                   for keyword in ["does not exist", "column", "relation"]):

                logger.debug("Attempting schema self-repair...")

                sql_query = repair_sql_error(
                    original_sql=sql_query,
                    error_message=error_message,
                    schema_description=schema_description,
                    user_input=user_input_param
                )

                sql_query = _enforce_start_year_table_mapping(sql_query, user_input_param)
                sql_query = _enforce_advanced_table_mapping(sql_query, user_input_param, schema_description)
                sql_query = _rewrite_career_aggregate_to_by_season(sql_query, user_input_param)
                sql_query = _ensure_rebounding_leaderboard_columns(sql_query, user_input_param)
                sql_query = _ensure_assist_leaderboard_columns(sql_query, user_input_param)
                sql_query = _ensure_all_players_broad_columns(sql_query, user_input_param)
                sql_query = _expand_player_name_filters_for_encoding(sql_query)
                sql_query = _ensure_profile_columns_in_sql(sql_query, user_input_param)
                sql_query = limit_rows(sql_query)

                try:
                    sql_query = validate_and_normalize_sql(sql_query)
                except ValueError as e:
                    logger.error("Repaired SQL is unsafe: %s", e)
                    return None

                continue
            else:
                logger.error("Non-repairable error.")
                return None

    logger.error("Max repair attempts reached.")
    return None


def run_query(question: str):
    return natural_language_to_sql(question)


def debug_query_routing(user_input: str, model_sql: str):
    """
    Debug helper to validate table routing and active DB identity without relying
    on a second OpenAI SQL generation pass.
    """
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT current_database(), current_user, current_schema(), current_setting('search_path'), inet_server_addr(), inet_server_port();"
        )
        db_identity = cursor.fetchone()

        sql_after_year = _enforce_start_year_table_mapping(model_sql or "", user_input or "")
        sql_after_advanced = _enforce_advanced_table_mapping(sql_after_year, user_input or "")
        sql_after_name = _expand_player_name_filters_for_encoding(sql_after_advanced)
        sql_after_profile = _ensure_profile_columns_in_sql(sql_after_name, user_input or "")
        final_sql = limit_rows(sql_after_profile)

        try:
            final_sql = validate_and_normalize_sql(final_sql)
        except Exception as e:
            final_sql = f"[INVALID SQL AFTER REWRITE] {e} | SQL: {final_sql}"

        start, end, is_playoffs = _extract_requested_season_window(user_input or "")
        expected_advanced_table = _advanced_table_name_for_window(start, end, is_playoffs)

        cursor.execute(
            """
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = %s
            );
            """,
            (expected_advanced_table,),
        )
        expected_advanced_table_exists = bool(cursor.fetchone()[0])

        cursor.execute("SELECT to_regclass(%s);", (f"public.{expected_advanced_table}",))
        regclass_public = cursor.fetchone()[0]
        cursor.execute("SELECT to_regclass(%s);", (expected_advanced_table,))
        regclass_unqualified = cursor.fetchone()[0]

        cursor.execute(
            """
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_name = %s
            ORDER BY table_schema, table_name;
            """,
            (expected_advanced_table,),
        )
        exact_table_matches = [
            {"table_schema": row[0], "table_name": row[1]} for row in cursor.fetchall()
        ]

        cursor.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_name ILIKE 'nba%%advanced%%'
            ORDER BY table_name
            LIMIT 100;
            """
        )
        advanced_table_candidates = [row[0] for row in cursor.fetchall()]

        return {
            "db_identity": {
                "database": db_identity[0],
                "user": db_identity[1],
                "schema": db_identity[2],
                "search_path": db_identity[3],
                "server_addr": str(db_identity[4]) if db_identity[4] is not None else None,
                "server_port": db_identity[5],
            },
            "routing": {
                "is_advanced_metrics_request": _is_advanced_metrics_request(user_input or ""),
                "requested_window": {
                    "start_year": start,
                    "end_year": end,
                    "is_playoffs": is_playoffs,
                },
                "expected_advanced_table": expected_advanced_table,
                "expected_advanced_table_exists": expected_advanced_table_exists,
                "regclass_public": str(regclass_public) if regclass_public is not None else None,
                "regclass_unqualified": str(regclass_unqualified) if regclass_unqualified is not None else None,
                "exact_table_matches": exact_table_matches,
            },
            "sql": {
                "input_model_sql": model_sql,
                "rewritten_sql": final_sql,
            },
            "advanced_table_candidates": advanced_table_candidates,
        }
    finally:
        cursor.close()
        conn.close()