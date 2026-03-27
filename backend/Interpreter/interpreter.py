import os
import logging
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
  What they contain: One row per player per season. Pre-aggregated season totals and averages.
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
    ❌ wl              (no win/loss per game)
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

════════════════════════════════════════════════════════════════════════
SECTION 2: TABLE SELECTION RULES — FOLLOW IN ORDER, FIRST MATCH WINS
════════════════════════════════════════════════════════════════════════

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
      "2019 playoffs" or "2018-19 playoffs" → all_players_playoffs_2018_2019
      "2023 playoffs" or "2022-23 playoffs" → all_players_playoffs_2022_2023
      "last season" (current year is 2026)   → all_players_regular_2024_2025
      "2020 regular season"                  → all_players_regular_2019_2020
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
  → Order by the relevant stat column DESC
  Example question: "Who are the top 10 scorers this season"
  Correct: SELECT player_name, team_abbreviation, pts FROM all_players_regular_2024_2025
           ORDER BY pts DESC LIMIT 10

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
  - ALWAYS use SUM() when selecting counting stats (pts, reb, ast, stl, blk, tov, fgm, fga, etc.)
  - ALWAYS use GROUP BY player_name (and team_abbreviation if needed)
  - ALWAYS calculate percentages via SUM() not AVG():
      (CAST(SUM(fgm) AS DOUBLE PRECISION) / NULLIF(SUM(fga), 0)) AS fg_pct
      (CAST(SUM(fg3m) AS DOUBLE PRECISION) / NULLIF(SUM(fg3a), 0)) AS fg3_pct
      (CAST(SUM(ftm) AS DOUBLE PRECISION) / NULLIF(SUM(fta), 0)) AS ft_pct

STANDARD PLAYER PERFORMANCE SELECT BLOCK:
  Use this exact block for any "analyze", "show stats", "performance" question
  when querying season summary tables:
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

Q: "Who are the top 10 scorers this season"
→ RULE 8. Leaderboard. Use all_players_regular_2024_2025.
SELECT player_name, team_abbreviation, pts, reb, ast, gp
FROM all_players_regular_2024_2025
ORDER BY pts DESC LIMIT 10;

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