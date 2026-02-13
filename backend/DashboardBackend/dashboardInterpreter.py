import os
import json
import psycopg2
from psycopg2.extras import RealDictCursor
from openai import OpenAI
from typing import Dict, List, Any, Tuple, Optional
from dotenv import load_dotenv
import re

load_dotenv()
# Setting up OpenAI client
print("API Key Loaded:", os.getenv("OPENAI_API_KEY"))
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# AWS postgres database info
DB_CONFIG = {
    "host": "nba-sdp-project.cs1c0smw8vqa.us-east-1.rds.amazonaws.com",
    "port": 5432,
    "dbname": "NBA-STATS",
    "user": "VonLindenthal",
    "password": "Vlindenthal1!",
    "sslmode": "require"
}

# Database schema info for GPT
DATABASE_SCHEMA = """
You have access to a PostgreSQL database on AWS RDS with two types of tables.

1. **Season Summaries** (`all_players_regular_YYYY_YYYY`):
   - **USE FOR:** "Trends" (Year-over-Year), "Career", "Averages", "Top Scorers", "Profiles".
   - **COLUMNS:** `pts`, `ast`, `reb`, `gp`, `min` (All are PER GAME averages).
   - **CRITICAL:** This table DOES NOT have a 'season' column. You must SELECT it as a string literal (e.g., `'2023-24'`).

2. **Game Logs** (`player_game_logs`):
   - **USE FOR:** "Last 10 games", "Vs Lakers", "March 2024", "Playoffs".
   - **COLUMNS:** `game_date`, `matchup`, `season_type` ('Regular Season' or 'Playoffs').
   - **STATS:** `pts`, `ast`, `reb` are TOTALS for that single game.

IMPORTANT SQL RULES:
1. **Ordering (CRITICAL)**:
   - Career Trends: `ORDER BY season ASC` (Must go Oldest -> Newest).
   - Game Trends: `ORDER BY game_date ASC`.
   - "Last 10 Games": Use subquery pattern: `SELECT * FROM (...) sub ORDER BY game_date ASC`.

2. **Dynamic Table Selection**:
   - Query mentions "Date", "Month", "Vs Team", "Last X Games" -> **USE `player_game_logs`**.
   - Query mentions "Season", "Year", "Trend" (Career) -> **USE `all_players_regular_...`**.

3. **Leaderboard Filters**:
   - "Top Scorer" / Averages -> `WHERE gp > 40` (Remove outliers).
   - "Total Points" -> `ORDER BY (pts * gp) DESC`.

4. **Name Matching**:
   - Always use `ILIKE '%First%Last%'` to be safe.

OUTPUT SHAPE RULES (VERY IMPORTANT):
- Always alias numeric y-values as `stat_value` for line/bar charts.
- For CompareStats, include a player identifier column as `full_name` (or `player_name`) PLUS a time column: `season` OR `game_date`.
- For SinglePlayerStat, include: `stat_value` and time column: `season` OR `game_date`.
- For Leaderboard, include: `player_name`, optional `team_abbreviation`, and `stat_value`.
- For CategoricalBreakdown / CompareCategoricalBreakdown (radar), select raw columns: `pts, ast, reb, stl, blk` (can be aliases), plus a `player_name`/`full_name` column for multi-player.
"""


def build_system_prompt() -> str:
    return f"""You are an NBA analytics assistant.
{DATABASE_SCHEMA}

Return JSON with this structure:
{{
  "chartType": "Leaderboard|CategoricalBreakdown|CompareCategoricalBreakdown|SinglePlayerStat|CompareStats",
  "sqlQuery": "SELECT ...",
  "chartConfig": {{
      "statKey": "stat_value",
      "playerNames": [],
      "xAxisKey": "season",
      "statDisplayName": "Points"
  }}
}}

EXAMPLES:

1. **"Show me Steph Curry's 3-point trend 2019-2024"** (Career Trend - Ordered ASC)
   - Type: "SinglePlayerStat"
   - SQL: "SELECT * FROM (SELECT '2023-24' as season, fg3_pct as stat_value FROM all_players_regular_2023_2024 WHERE player_name ILIKE '%Steph%Curry%' UNION ALL SELECT '2022-23' as season, fg3_pct as stat_value FROM all_players_regular_2022_2023 WHERE player_name ILIKE '%Steph%Curry%' UNION ALL SELECT '2021-22' as season, fg3_pct as stat_value FROM all_players_regular_2021_2022 WHERE player_name ILIKE '%Steph%Curry%' UNION ALL SELECT '2020-21' as season, fg3_pct as stat_value FROM all_players_regular_2020_2021 WHERE player_name ILIKE '%Steph%Curry%') as career_trend ORDER BY season ASC"

2. **"Compare LeBron and KD points in 2024"** (Comparison)
   - Type: "CompareStats"
   - SQL: "SELECT player_name as full_name, '2023-24' as season, pts as stat_value FROM all_players_regular_2023_2024 WHERE player_name ILIKE '%LeBron%' OR player_name ILIKE '%Durant%'"

3. **"How is Wembanyama performing in his last 10 games?"** (Recent Form)
   - Type: "SinglePlayerStat"
   - SQL: "SELECT * FROM (SELECT game_date, pts as stat_value FROM player_game_logs WHERE player_name ILIKE '%Wembanyama%' ORDER BY game_date DESC LIMIT 10) sub ORDER BY game_date ASC"
   - Config: {{ "xAxisKey": "game_date" }}

4. **"How many points did Curry score vs the Lakers in 2024?"** (Matchup)
   - Type: "SinglePlayerStat"
   - SQL: "SELECT game_date, pts as stat_value FROM player_game_logs WHERE player_name ILIKE '%Steph%Curry%' AND matchup ILIKE '%LAL%' AND game_date > '2023-10-01' ORDER BY game_date ASC"
   - Config: {{ "xAxisKey": "game_date" }}

5. **"Show me Jimmy Butler's points trend in the 2023 Playoffs"** (Playoffs)
   - Type: "SinglePlayerStat"
   - SQL: "SELECT game_date, pts as stat_value FROM player_game_logs WHERE player_name ILIKE '%Jimmy%Butler%' AND season_type = 'Playoffs' AND game_date BETWEEN '2023-04-01' AND '2023-07-01' ORDER BY game_date ASC"
   - Config: {{ "xAxisKey": "game_date" }}

6. **"Who are the top 5 scorers in 2024?"** (Leaderboard Average)
   - Type: "Leaderboard"
   - SQL: "SELECT player_name, team_abbreviation, pts as stat_value FROM all_players_regular_2023_2024 WHERE gp > 40 ORDER BY stat_value DESC LIMIT 5"

7. **"Who had the most total assists in 2024?"** (Leaderboard Total)
   - Type: "Leaderboard"
   - SQL: "SELECT player_name, team_abbreviation, (ast * gp) as stat_value FROM all_players_regular_2023_2024 ORDER BY stat_value DESC LIMIT 10"

8. **"Show me Luka's skill profile"** (Radar)
   - Type: "CategoricalBreakdown"
   - SQL: "SELECT pts, ast, reb, stl, blk FROM all_players_regular_2023_2024 WHERE player_name ILIKE '%Luka%Doncic%'"
   - Config: {{ "playerNames": ["Luka Doncic"] }}

9. **"Compare Luka vs Shai skill profiles"** (Radar - multi player)
   - Type: "CompareCategoricalBreakdown"
   - SQL: "SELECT player_name, pts, ast, reb, stl, blk FROM all_players_regular_2023_2024 WHERE player_name ILIKE '%Luka%Doncic%' OR player_name ILIKE '%Shai%Gilgeous%Alexander%'"
   - Config: {{ "playerNames": ["Luka Doncic", "Shai Gilgeous-Alexander"] }}
"""

# Max values for normalizing player stats to 0-100 scale
# Based on elite NBA performance benchmarks (obv subject to change, but this felt reasonable)
STAT_BENCHMARKS = {
    "PTS": 35.0,
    "AST": 11.0,
    "REB": 14.0,
    "STL": 2.5,
    "BLK": 2.5
}

def _safe_upper_keys(row: Dict[str, Any]) -> Dict[str, Any]:
    return {str(k).upper(): v for k, v in row.items()}

def process_comparison_data(raw_data: List[Dict]) -> List[Dict]:
    """
    Takes raw SQL results and reshapes them for the comparison chart.
    Each season/game gets its own object with all players as keys.
    """
    if not raw_data:
        return []

    # Group by time period (season or date)
    seasons: Dict[str, Dict[str, Any]] = {}
    for row in raw_data:
        player = row.get("full_name") or row.get("player_name") or "Unknown"
        val = row.get("stat_value", 0)
        season = row.get("season") or str(row.get("game_date")) or "Current"

        if season not in seasons:
            seasons[season] = {"season": season}
        seasons[season][player] = val

    # Sort chronologically
    return sorted(list(seasons.values()), key=lambda x: x["season"])


def process_categorical_data(raw_data: List[Dict], player_count: int) -> List[Dict]:
    """
    Converts player stats into radar chart format.
    Normalizes stats to 0-100 scale using benchmarks.
    """
    if not raw_data:
        return []

    categories = ["PTS", "AST", "REB", "STL", "BLK"]

    # Single player radar
    if player_count <= 1:
        first_row = _safe_upper_keys(raw_data[0])
        radar_data: List[Dict[str, Any]] = []
        for cat in categories:
            # Find the stat value
            raw_val = float(first_row.get(cat, 0) or 0)
            max_benchmark = STAT_BENCHMARKS.get(cat, 30)
            normalized = min(100, (raw_val / max_benchmark) * 100) if max_benchmark else 0
            
            radar_data.append({
                "category": cat,
                "value": int(normalized),
                "raw_value": round(raw_val, 1)
            })
        return radar_data

    # Multi player comparison radar
    radar_map: Dict[str, Dict[str, Any]] = {cat: {"category": cat} for cat in categories}
    for row in raw_data:
        player = row.get("full_name") or row.get("player_name") or "Unknown"
        upper = _safe_upper_keys(row)
        for cat in categories:
            raw_val = float(upper.get(cat, 0) or 0)
            max_benchmark = STAT_BENCHMARKS.get(cat, 30)
            normalized = min(100, (raw_val / max_benchmark) * 100) if max_benchmark else 0
            radar_map[cat][player] = int(normalized)

    return list(radar_map.values())


# Locking interpreter to known chart types to void error and future mismatches

ALLOWED_CHART_TYPES = {
    "Leaderboard",
    "CategoricalBreakdown",
    "CompareCategoricalBreakdown",
    "SinglePlayerStat",
    "CompareStats",
}

RADAR_KEYS = {"PTS", "AST", "REB", "STL", "BLK"}


def _extract_names_heuristic(q: str) -> List[str]:
    # This is just for hints; we do NOT depend on it being perfect.
    candidates = re.findall(r"\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,2}\b", q)
    # Remove common NBA words that are not player names
    blacklist = {"Top", "Show", "Compare", "Vs", "Versus", "Skill", "Profile", "Points", "Assists", "Rebounds",
                 "Playoffs", "Regular", "Season", "Last", "Games", "Trend", "Leaderboard"}
    cleaned = [c for c in candidates if c not in blacklist]
    seen = set()
    out = []
    for c in cleaned:
        if c not in seen:
            seen.add(c)
            out.append(c)
    return out


def _intent_hint(user_question: str) -> Dict[str, Any]:
    q = user_question.lower()
    hint: Dict[str, Any] = {}

    is_compare = any(w in q for w in [" compare ", " vs ", " versus ", "against "]) or " vs." in q
    is_leaderboard = any(w in q for w in ["top ", "leaders", "leaderboard", "most ", "rank", "best "])
    is_profile = any(w in q for w in ["skill profile", "profile", "radar", "breakdown", "categories", "categorical"])
    is_recent = any(w in q for w in ["last ", "past ", "recent", "game log", "gamelog", "vs the", "vs ", "playoffs", "march", "april", "january", "february", "december", "november", "october"])

    hint["suspectedCompare"] = is_compare
    hint["suspectedLeaderboard"] = is_leaderboard
    hint["suspectedProfile"] = is_profile
    hint["suspectedGameLog"] = is_recent

    # Prefer profile classification if asked explicitly
    if is_profile and is_compare:
        hint["preferredChartType"] = "CompareCategoricalBreakdown"
    elif is_profile:
        hint["preferredChartType"] = "CategoricalBreakdown"
    elif is_leaderboard:
        hint["preferredChartType"] = "Leaderboard"
    elif is_compare:
        hint["preferredChartType"] = "CompareStats"
    else:
        hint["preferredChartType"] = "SinglePlayerStat"

    # Names are only a hint
    hint["guessedNames"] = _extract_names_heuristic(user_question)[:4]
    return hint


def _columns_present(raw_data: List[Dict]) -> set:
    if not raw_data:
        return set()
    cols = set()
    for k in raw_data[0].keys():
        cols.add(str(k).lower())
    return cols


def _looks_like_radar(raw_data: List[Dict]) -> bool:
    if not raw_data:
        return False
    upper = set(_safe_upper_keys(raw_data[0]).keys())
    # If it has most radar keys, treat as radar
    return len(RADAR_KEYS.intersection(upper)) >= 3


def _validate_and_autofix(chart_type: str, raw_data: List[Dict], chart_config: Dict[str, Any]) -> Tuple[bool, str, str]:
    """
    Returns (ok, fixed_chart_type, reason).
    If it's obviously mismatched, we either fix chart_type or mark invalid.
    """
    if chart_type not in ALLOWED_CHART_TYPES:
        return False, chart_type, f"chartType '{chart_type}' not allowed"

    cols = _columns_present(raw_data)

    # Radar detection override (prevents 'leaderboard' charts with pts/ast/reb columns etc.)
    if _looks_like_radar(raw_data):
        player_names = chart_config.get("playerNames", []) or []
        expected = "CompareCategoricalBreakdown" if len(player_names) > 1 or len(raw_data) > 1 else "CategoricalBreakdown"
        if chart_type not in {"CategoricalBreakdown", "CompareCategoricalBreakdown"}:
            return True, expected, "Auto-corrected chartType based on radar-shaped SQL output"
        # If model picked the wrong radar subtype, fix it
        if expected != chart_type:
            return True, expected, "Auto-corrected radar subtype based on playerNames/raw rows"
        return True, chart_type, "ok"

    # Leaderboard requirements
    if chart_type == "Leaderboard":
        if "stat_value" not in cols or ("player_name" not in cols and "full_name" not in cols):
            return False, chart_type, "Leaderboard requires 'player_name' (or 'full_name') and 'stat_value'"
        return True, chart_type, "ok"

    # SinglePlayerStat requirements
    if chart_type == "SinglePlayerStat":
        if "stat_value" not in cols:
            return False, chart_type, "SinglePlayerStat requires 'stat_value'"
        if "season" not in cols and "game_date" not in cols:
            # Sometimes xAxisKey is a custom alias - accept if it exists
            xk = chart_config.get("xAxisKey", "season")
            if xk and str(xk).lower() in cols:
                return True, chart_type, "ok"
            return False, chart_type, "SinglePlayerStat requires 'season' or 'game_date' (or config.xAxisKey column)"
        return True, chart_type, "ok"

    # CompareStats requirements
    if chart_type == "CompareStats":
        if "stat_value" not in cols:
            return False, chart_type, "CompareStats requires 'stat_value'"
        if "season" not in cols and "game_date" not in cols:
            return False, chart_type, "CompareStats requires 'season' or 'game_date'"
        if "full_name" not in cols and "player_name" not in cols:
            return False, chart_type, "CompareStats requires 'full_name' or 'player_name'"
        return True, chart_type, "ok"

    # CategoricalBreakdown requires radar columns (handled above by _looks_like_radar)
    if chart_type in {"CategoricalBreakdown", "CompareCategoricalBreakdown"}:
        return False, chart_type, "Radar chart type selected but SQL output does not look like radar stats"

    return True, chart_type, "ok"


def _call_gpt_for_interpretation(user_question: str, hint: Dict[str, Any], repair_message: Optional[str] = None) -> Dict[str, Any]:
    messages = [{"role": "system", "content": build_system_prompt()}]

    # Provide a small hint message (does not force, just guides)
    messages.append({
        "role": "system",
        "content": f"""Extra hint (not mandatory, but try to follow it if it matches the question):
- preferredChartType: {hint.get('preferredChartType')}
- suspectedCompare: {hint.get('suspectedCompare')}
- suspectedLeaderboard: {hint.get('suspectedLeaderboard')}
- suspectedProfile: {hint.get('suspectedProfile')}
- guessedNames: {hint.get('guessedNames')}

Still follow the main schema exactly, and only output valid JSON."""
    })

    if repair_message:
        messages.append({"role": "system", "content": repair_message})

    messages.append({"role": "user", "content": user_question})

    response = client.chat.completions.create(
        model="gpt-4-turbo-preview",
        messages=messages,
        temperature=0.1,
        response_format={"type": "json_object"}
    )

    interpretation = json.loads(response.choices[0].message.content)

    # Basic sanitization / defaults
    chart_type = interpretation.get("chartType")
    if chart_type and isinstance(chart_type, str):
        interpretation["chartType"] = chart_type.strip()

    if "chartConfig" not in interpretation or not isinstance(interpretation["chartConfig"], dict):
        interpretation["chartConfig"] = {}

    # Ensure expected keys exist
    interpretation["chartConfig"].setdefault("statKey", "stat_value")
    interpretation["chartConfig"].setdefault("playerNames", [])
    interpretation["chartConfig"].setdefault("xAxisKey", "season")
    interpretation["chartConfig"].setdefault("statDisplayName", "Stat")

    return interpretation


def interpret_question(user_question: str) -> Dict[str, Any]:
    """
    Main function that:
    1. Sends question to GPT to get SQL + chart type
    2. Runs the SQL query
    3. Validates + auto-fixes the chart type when possible
    4. Formats the data for the frontend (with a single automatic retry on schema mismatch)
    """
    conn = None
    try:
        print(f"Analyzing question: {user_question}")

        hint = _intent_hint(user_question)

        # 1) First attempt
        interpretation = _call_gpt_for_interpretation(user_question, hint)

        chart_type = interpretation.get("chartType", "")
        sql_query = interpretation.get("sqlQuery", "")
        chart_config = interpretation.get("chartConfig", {})

        print(f"Chart Type: {chart_type}")
        print(f"Generated SQL: {sql_query}")

        # Connect to database and run the query
        conn = psycopg2.connect(**DB_CONFIG)
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(sql_query)
            raw_data = cursor.fetchall()

        # Validate + auto-fix chartType when we can
        ok, fixed_type, reason = _validate_and_autofix(chart_type, raw_data, chart_config)
        if fixed_type != chart_type:
            print(f"[AutoFix] chartType {chart_type} -> {fixed_type} ({reason})")
            chart_type = fixed_type

        # If invalid, do ONE repair attempt (re-ask GPT with concrete failure reason)
        if not ok:
            repair_msg = (
                "Your previous output caused a schema mismatch when executing the SQL. "
                f"Reason: {reason}. "
                "Return corrected JSON. You may change chartType and/or SQL so it matches the OUTPUT SHAPE RULES. "
                "Do NOT return explanations, only JSON."
            )
            interpretation = _call_gpt_for_interpretation(user_question, hint, repair_message=repair_msg)

            chart_type = interpretation.get("chartType", "")
            sql_query = interpretation.get("sqlQuery", "")
            chart_config = interpretation.get("chartConfig", {})

            print(f"[Retry] Chart Type: {chart_type}")
            print(f"[Retry] Generated SQL: {sql_query}")

            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(sql_query)
                raw_data = cursor.fetchall()

            ok, fixed_type, reason = _validate_and_autofix(chart_type, raw_data, chart_config)
            if fixed_type != chart_type:
                print(f"[AutoFix] chartType {chart_type} -> {fixed_type} ({reason})")
                chart_type = fixed_type

            if not ok:
                # Give up after one retry, but return a structured error
                return {"success": False, "error": f"Interpreter mismatch after retry: {reason}"}

        # Format data based on chart type
        final_data = raw_data

        if chart_type == "CompareStats":
            final_data = process_comparison_data(raw_data)
        elif chart_type in {"CategoricalBreakdown", "CompareCategoricalBreakdown"}:
            player_names = chart_config.get("playerNames", []) or []
            # If playerNames missing but SQL returned multiple rows, treat as multi player radar
            inferred_count = max(len(player_names), len(raw_data))
            # Normalize chart subtype
            if inferred_count > 1:
                chart_type = "CompareCategoricalBreakdown"
            else:
                chart_type = "CategoricalBreakdown"
            final_data = process_categorical_data(raw_data, inferred_count)

        
        # For SinglePlayerStat and Leaderboard we keep raw_data (frontend expects it)
        frontend_chart_type = "CategoricalBreakdown" if chart_type == "CompareCategoricalBreakdown" else chart_type
        return {
            "success": True,
            "chartType": frontend_chart_type,
            "data": final_data,
            "config": chart_config,
        }

    except Exception as e:
        print(f"Error occurred: {e}")
        return {"success": False, "error": str(e)}
    finally:
        if conn:
            conn.close()