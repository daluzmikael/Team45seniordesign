import logging
import os
import json
import psycopg2
from psycopg2.extras import RealDictCursor
from openai import OpenAI
from typing import Dict, List, Any, Tuple, Optional
from dotenv import load_dotenv
import re

logger = logging.getLogger(__name__)
load_dotenv()
# Never print or log the raw API key
api_key_env = os.getenv("OPENAI_API_KEY")
if api_key_env:
    logger.debug("OpenAI API key loaded from environment")
else:
    logger.warning("OPENAI_API_KEY is not set")
client = OpenAI(api_key=api_key_env)

# AWS postgres database info
DB_CONFIG = {
    "host": "nba-sdp-project.cs1c0smw8vqa.us-east-1.rds.amazonaws.com",
    "port": 5432,
    "dbname": "postgres",
    "user": "VonLindenthal",
    "password": "Vlindenthal1!",
    "sslmode": "require"
}

# Database schema info for GPT
DATABASE_SCHEMA = """
You have access to a PostgreSQL database on AWS RDS with two types of tables.

1. **Season Summaries** (`all_players_regular_YYYY_YYYY`):
   - **USE FOR:** "Trends" (Year-over-Year), "Career", "Averages", "Top Scorers", "Profiles".
   - **COLUMNS:** `player_name`, `pts`, `ast`, `reb`, `stl`, `blk`, `gp`, `min` (All are PER GAME averages).
   - **CRITICAL:** This table DOES NOT have a 'season' column. You must SELECT it as a string literal (e.g., `'2023-24'`).
   - **TABLE NAME FORMAT:** `all_players_regular_YYYY_YYYY` where the two years span the season.
     Examples: 2023-24 season → `all_players_regular_2023_2024`, 2016-17 season → `all_players_regular_2016_2017`.
   - **DUPLICATE ROWS:** Players traded mid-season may appear multiple times in the same table (once per team). For Leaderboards and profiles, ALWAYS deduplicate by grouping: `GROUP BY player_name` with aggregated stats, or use a subquery with `DISTINCT ON (player_name)` ordered by `gp DESC` to keep the row with the most games played.

2. **Game Logs** (`player_game_logs`):
   - **USE FOR:** "Last 10 games", "Vs Lakers", "March 2024", "Playoffs".
   - **COLUMNS:** `game_date`, `matchup`, `season_type` ('Regular Season' or 'Playoffs').
   - **STATS:** `pts`, `ast`, `reb` are TOTALS for that single game.

3. **Shot Chart Data** (`court_shots`):
   - **USE FOR:** "Heat map", "Shot chart", "Shot selection", "Shooting zones".
   - **COLUMNS:** `player_id`, `player_name`, `team_name`, `game_date`, `action_type`, `shot_type`,
     `shot_zone_basic`, `shot_zone_area`, `shot_zone_range`, `shot_distance`,
     `loc_x`, `loc_y`, `shot_attempted_flag`, `shot_made_flag`, `htm`, `vtm`, `period`.
   - **CRITICAL:** Always SELECT at minimum: `loc_x`, `loc_y`, `shot_made_flag`.
   - Optionally include `shot_attempted_flag`, `action_type`, `shot_type` for filtering.
   - `htm` = home team, `vtm` = visiting team. To filter vs a team, use: `htm ILIKE '%LAL%' OR vtm ILIKE '%LAL%'`.

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
   - **ALWAYS use DISTINCT ON (player_name) or GROUP BY to avoid duplicate rows for traded players.**

4. **Name Matching**:
   - Always use `ILIKE '%First%Last%'` to be safe.

5. **Retired / Historical Players**:
   - If the user asks about a player with NO specific season and the player may be retired (e.g., Kobe Bryant, Tim Duncan, Kevin Garnett, Manu Ginobili), do NOT just query the latest season table.
   - Instead, try their most likely peak/final season table. Common examples:
     * Kobe Bryant → `all_players_regular_2015_2016` (last season)
     * Tim Duncan → `all_players_regular_2015_2016` (last season)
     * Kevin Garnett → `all_players_regular_2015_2016` (last season)
     * Manu Ginobili → `all_players_regular_2017_2018` (last season)
   - If you are unsure of the exact season, pick a reasonable one from their career. It is better to try a season they likely played than to default to the current season and get no results.

OUTPUT SHAPE RULES (VERY IMPORTANT):
- Always alias numeric y-values as `stat_value` for line/bar charts.
- For CompareStats (bar chart), include a player identifier column as `full_name` (or `player_name`) PLUS a time column: `season` OR `game_date`.
- For SinglePlayerStat (line/area chart), include: `stat_value` and time column: `season` OR `game_date`.
- **Multi-player trend lines** also use CompareStats. When the user asks for a "trend line" or "trend" for MULTIPLE players over time, use CompareStats — it will show side-by-side bars per season.
- For Leaderboard, include: `player_name`, optional `team_abbreviation`, and `stat_value`. **Must have exactly one row per player.**
- For CategoricalBreakdown / CompareCategoricalBreakdown (radar):
  * ALWAYS include `player_name` in your SELECT (even for single player).
  * Select raw columns: `pts, ast, reb, stl, blk`.
  * If comparing across different seasons, include a `season` column as a string literal.
  * The radar labels will automatically combine player_name + season when season is present.
- For ShotChart, always select: `loc_x, loc_y, shot_made_flag` from `court_shots`.
- For Scatter (scatter plot, "X vs Y", or single-axis distribution):
  * The data must be ONE ROW PER PLAYER (deduplicate with DISTINCT ON when needed).
  * For two-axis scatter (e.g. "TS% vs PPG", "PPG vs age"): SELECT exactly three columns aliased as
    `player_name`, `x_value`, `y_value`. No other columns.
  * For single-axis distribution (e.g. "scatter of TS% in 2018-19"): SELECT exactly two columns
    aliased as `player_name` and `y_value`. No `x_value` column at all.
  * x_value and y_value MUST be numeric. For text-storage families (nba_advanced_*, nba_clutch_*,
    nba_hustle_*, nba_player_tracking_pt_*, nba_lineups_*, team_advanced_*, nba_standings_*),
    cast with `CAST(NULLIF("COL", '') AS numeric)`. For the `all_players_*` and `player_game_logs`
    tables, raw column references work directly.
  * Apply a meaningful sample filter so noise dots do not dominate:
      `WHERE gp >= 20` for `all_players_*`, or
      `WHERE CAST(NULLIF("GP", '') AS numeric) >= 20` for text-storage families.
  * Add `LIMIT 500` — scatter readability collapses past that.
  * NEVER use GROUP BY across (player, season) for Scatter unless explicitly aggregating
    career totals across multiple season tables (UNION ALL pattern, then SUM and GROUP BY player_name).
  * If x and y are stored in different tables, JOIN on player_name (case-insensitive ILIKE if needed).
  * statDisplayName, xAxisLabel, and yAxisLabel are REQUIRED in chartConfig for Scatter so the chart
    renders human-readable axes. For single-axis, set xAxisLabel to "" (empty string).
"""


def build_system_prompt() -> str:
    return f"""You are an NBA analytics assistant.
{DATABASE_SCHEMA}

Return JSON with this structure:
{{
  "chartType": "Leaderboard|CategoricalBreakdown|CompareCategoricalBreakdown|SinglePlayerStat|CompareStats|ShotChart|Scatter",
  "sqlQuery": "SELECT ...",
  "chartConfig": {{
      "statKey": "stat_value",
      "playerNames": [],
      "xAxisKey": "season",
      "statDisplayName": "Points",
      "mode": "volume|accuracy|hotspots|coldspots"
  }}
}}

EXAMPLES:

1. **"Show me Steph Curry's 3-point trend 2019-2024"** (Career Trend - Ordered ASC)
   - Type: "SinglePlayerStat"
   - SQL: "SELECT * FROM (SELECT '2023-24' as season, fg3_pct as stat_value FROM all_players_regular_2023_2024 WHERE player_name ILIKE '%Steph%Curry%' UNION ALL SELECT '2022-23' as season, fg3_pct as stat_value FROM all_players_regular_2022_2023 WHERE player_name ILIKE '%Steph%Curry%' UNION ALL SELECT '2021-22' as season, fg3_pct as stat_value FROM all_players_regular_2021_2022 WHERE player_name ILIKE '%Steph%Curry%' UNION ALL SELECT '2020-21' as season, fg3_pct as stat_value FROM all_players_regular_2020_2021 WHERE player_name ILIKE '%Steph%Curry%') as career_trend ORDER BY season ASC"

2. **"Compare LeBron and KD points in 2024"** (Comparison — single season bar chart)
   - Type: "CompareStats"
   - SQL: "SELECT player_name as full_name, '2023-24' as season, pts as stat_value FROM all_players_regular_2023_2024 WHERE player_name ILIKE '%LeBron%' OR player_name ILIKE '%Durant%'"

3. **"Show me Kyle Kuzma and Stephen Curry points trend from 2019 to 2024"** (Multi-player trend — bar chart over multiple seasons)
   - Type: "CompareStats"
   - SQL: "SELECT player_name as full_name, '2019-20' as season, pts as stat_value FROM all_players_regular_2019_2020 WHERE player_name ILIKE '%Kuzma%' OR player_name ILIKE '%Curry%' UNION ALL SELECT player_name as full_name, '2020-21' as season, pts as stat_value FROM all_players_regular_2020_2021 WHERE player_name ILIKE '%Kuzma%' OR player_name ILIKE '%Curry%' UNION ALL SELECT player_name as full_name, '2021-22' as season, pts as stat_value FROM all_players_regular_2021_2022 WHERE player_name ILIKE '%Kuzma%' OR player_name ILIKE '%Curry%' UNION ALL SELECT player_name as full_name, '2022-23' as season, pts as stat_value FROM all_players_regular_2022_2023 WHERE player_name ILIKE '%Kuzma%' OR player_name ILIKE '%Curry%' UNION ALL SELECT player_name as full_name, '2023-24' as season, pts as stat_value FROM all_players_regular_2023_2024 WHERE player_name ILIKE '%Kuzma%' OR player_name ILIKE '%Curry%' ORDER BY season ASC"

4. **"How is Wembanyama performing in his last 10 games?"** (Recent Form)
   - Type: "SinglePlayerStat"
   - SQL: "SELECT * FROM (SELECT game_date, pts as stat_value FROM player_game_logs WHERE player_name ILIKE '%Wembanyama%' ORDER BY game_date DESC LIMIT 10) sub ORDER BY game_date ASC"
   - Config: {{ "xAxisKey": "game_date" }}

5. **"How many points did Curry score vs the Lakers in 2024?"** (Matchup)
   - Type: "SinglePlayerStat"
   - SQL: "SELECT game_date, pts as stat_value FROM player_game_logs WHERE player_name ILIKE '%Steph%Curry%' AND matchup ILIKE '%LAL%' AND game_date > '2023-10-01' ORDER BY game_date ASC"
   - Config: {{ "xAxisKey": "game_date" }}

6. **"Show me Jimmy Butler's points trend in the 2023 Playoffs"** (Playoffs)
   - Type: "SinglePlayerStat"
   - SQL: "SELECT game_date, pts as stat_value FROM player_game_logs WHERE player_name ILIKE '%Jimmy%Butler%' AND season_type = 'Playoffs' AND game_date BETWEEN '2023-04-01' AND '2023-07-01' ORDER BY game_date ASC"
   - Config: {{ "xAxisKey": "game_date" }}

7. **"Who are the top 5 scorers in 2024?"** (Leaderboard Average — deduplicated)
   - Type: "Leaderboard"
   - SQL: "SELECT player_name, team_abbreviation, pts as stat_value FROM (SELECT DISTINCT ON (player_name) player_name, team_abbreviation, pts, gp FROM all_players_regular_2023_2024 WHERE gp > 40 ORDER BY player_name, gp DESC) sub ORDER BY stat_value DESC LIMIT 5"

8. **"Who had the most total assists in 2024?"** (Leaderboard Total — deduplicated)
   - Type: "Leaderboard"
   - SQL: "SELECT player_name, team_abbreviation, (ast * gp) as stat_value FROM (SELECT DISTINCT ON (player_name) player_name, team_abbreviation, ast, gp FROM all_players_regular_2023_2024 ORDER BY player_name, gp DESC) sub ORDER BY stat_value DESC LIMIT 10"

9. **"Show me the top 10 players with highest apg in 2008"** (Leaderboard — historical, deduplicated)
   - Type: "Leaderboard"
   - SQL: "SELECT player_name, team_abbreviation, ast as stat_value FROM (SELECT DISTINCT ON (player_name) player_name, team_abbreviation, ast, gp FROM all_players_regular_2007_2008 ORDER BY player_name, gp DESC) sub ORDER BY stat_value DESC LIMIT 10"

10. **"Show me Luka's skill profile"** (Radar — single player, current/latest season)
   - Type: "CategoricalBreakdown"
   - SQL: "SELECT player_name, pts, ast, reb, stl, blk FROM (SELECT DISTINCT ON (player_name) player_name, pts, ast, reb, stl, blk, gp FROM all_players_regular_2023_2024 WHERE player_name ILIKE '%Luka%Doncic%' ORDER BY player_name, gp DESC) sub"
   - Config: {{ "playerNames": ["Luka Doncic"] }}

11. **"Show me Stephen Curry's skill profile in the 2016-17 season"** (Radar — single player, specific season)
   - Type: "CategoricalBreakdown"
   - SQL: "SELECT player_name, '2016-17' as season, pts, ast, reb, stl, blk FROM (SELECT DISTINCT ON (player_name) player_name, pts, ast, reb, stl, blk, gp FROM all_players_regular_2016_2017 WHERE player_name ILIKE '%Stephen%Curry%' ORDER BY player_name, gp DESC) sub"
   - Config: {{ "playerNames": ["Stephen Curry"], "statDisplayName": "2016-17 Skill Profile" }}

12. **"Show me Kevin Garnett's skill profile"** (Radar — retired player, no year specified)
   - Type: "CategoricalBreakdown"
   - SQL: "SELECT player_name, pts, ast, reb, stl, blk FROM (SELECT DISTINCT ON (player_name) player_name, pts, ast, reb, stl, blk, gp FROM all_players_regular_2015_2016 WHERE player_name ILIKE '%Kevin%Garnett%' ORDER BY player_name, gp DESC) sub"
   - Config: {{ "playerNames": ["Kevin Garnett"], "statDisplayName": "Skill Profile" }}

13. **"Compare Luka vs Shai skill profiles"** (Radar — multi player, same season)
   - Type: "CompareCategoricalBreakdown"
   - SQL: "SELECT player_name, pts, ast, reb, stl, blk FROM (SELECT DISTINCT ON (player_name) player_name, pts, ast, reb, stl, blk, gp FROM all_players_regular_2023_2024 WHERE player_name ILIKE '%Luka%Doncic%' OR player_name ILIKE '%Shai%Gilgeous%Alexander%' ORDER BY player_name, gp DESC) sub"
   - Config: {{ "playerNames": ["Luka Doncic", "Shai Gilgeous-Alexander"] }}

14. **"Compare LeBron's 2012-13 skill profile to Giannis 2020-21 skill profile"** (Radar — cross-season comparison)
   - Type: "CompareCategoricalBreakdown"
   - SQL: "SELECT player_name, '2012-13' as season, pts, ast, reb, stl, blk FROM (SELECT DISTINCT ON (player_name) player_name, pts, ast, reb, stl, blk, gp FROM all_players_regular_2012_2013 WHERE player_name ILIKE '%LeBron%James%' ORDER BY player_name, gp DESC) sub1 UNION ALL SELECT player_name, '2020-21' as season, pts, ast, reb, stl, blk FROM (SELECT DISTINCT ON (player_name) player_name, pts, ast, reb, stl, blk, gp FROM all_players_regular_2020_2021 WHERE player_name ILIKE '%Giannis%Antetokounmpo%' ORDER BY player_name, gp DESC) sub2"
   - Config: {{ "playerNames": ["LeBron James", "Giannis Antetokounmpo"], "statDisplayName": "Cross-Season Comparison" }}

15. **"Compare Curry's 2015-16 profile to his 2023-24 profile"** (Radar — same player, two seasons)
   - Type: "CompareCategoricalBreakdown"
   - SQL: "SELECT player_name, '2015-16' as season, pts, ast, reb, stl, blk FROM (SELECT DISTINCT ON (player_name) player_name, pts, ast, reb, stl, blk, gp FROM all_players_regular_2015_2016 WHERE player_name ILIKE '%Stephen%Curry%' ORDER BY player_name, gp DESC) sub1 UNION ALL SELECT player_name, '2023-24' as season, pts, ast, reb, stl, blk FROM (SELECT DISTINCT ON (player_name) player_name, pts, ast, reb, stl, blk, gp FROM all_players_regular_2023_2024 WHERE player_name ILIKE '%Stephen%Curry%' ORDER BY player_name, gp DESC) sub2"
   - Config: {{ "playerNames": ["Stephen Curry"], "statDisplayName": "Season Comparison" }}

15b. **"Show me Trae Young 2021 skill profile vs LeBron James 2018 skill profile vs Stephen Curry 2017 skill profile"** (Radar — 3+ players, each from a different season)
   - Type: "CompareCategoricalBreakdown"
   - SQL: "SELECT player_name, '2020-21' as season, pts, ast, reb, stl, blk FROM (SELECT DISTINCT ON (player_name) player_name, pts, ast, reb, stl, blk, gp FROM all_players_regular_2020_2021 WHERE player_name ILIKE '%Trae%Young%' ORDER BY player_name, gp DESC) sub1 UNION ALL SELECT player_name, '2017-18' as season, pts, ast, reb, stl, blk FROM (SELECT DISTINCT ON (player_name) player_name, pts, ast, reb, stl, blk, gp FROM all_players_regular_2017_2018 WHERE player_name ILIKE '%LeBron%James%' ORDER BY player_name, gp DESC) sub2 UNION ALL SELECT player_name, '2016-17' as season, pts, ast, reb, stl, blk FROM (SELECT DISTINCT ON (player_name) player_name, pts, ast, reb, stl, blk, gp FROM all_players_regular_2016_2017 WHERE player_name ILIKE '%Stephen%Curry%' ORDER BY player_name, gp DESC) sub3"
   - Config: {{ "playerNames": ["Trae Young", "LeBron James", "Stephen Curry"], "statDisplayName": "Cross-Season Comparison" }}

15c. **"LeBron James' skill profile 2003 vs LeBron James' skill profile 2012"** (Radar — bare-name shape, no leading verb)
   - Type: "CompareCategoricalBreakdown"
   - SQL: "SELECT player_name, '2003-04' as season, pts, ast, reb, stl, blk FROM (SELECT DISTINCT ON (player_name) player_name, pts, ast, reb, stl, blk, gp FROM all_players_regular_2003_2004 WHERE player_name ILIKE '%LeBron%James%' ORDER BY player_name, gp DESC) sub1 UNION ALL SELECT player_name, '2012-13' as season, pts, ast, reb, stl, blk FROM (SELECT DISTINCT ON (player_name) player_name, pts, ast, reb, stl, blk, gp FROM all_players_regular_2012_2013 WHERE player_name ILIKE '%LeBron%James%' ORDER BY player_name, gp DESC) sub2"
   - Config: {{ "playerNames": ["LeBron James"], "statDisplayName": "Season Comparison" }}
   - NOTE: The user can phrase a comparison without any leading verb. Treat "X profile [year] vs X profile [year]" the same as "Compare X profile [year] vs profile [year]" — it is a CompareCategoricalBreakdown.

15d. **"Curry 2015 vs Curry 2023"** (Radar — minimal shape, no word "profile")
   - Type: "CompareCategoricalBreakdown"
   - SQL: "SELECT player_name, '2015-16' as season, pts, ast, reb, stl, blk FROM (SELECT DISTINCT ON (player_name) player_name, pts, ast, reb, stl, blk, gp FROM all_players_regular_2015_2016 WHERE player_name ILIKE '%Stephen%Curry%' ORDER BY player_name, gp DESC) sub1 UNION ALL SELECT player_name, '2023-24' as season, pts, ast, reb, stl, blk FROM (SELECT DISTINCT ON (player_name) player_name, pts, ast, reb, stl, blk, gp FROM all_players_regular_2023_2024 WHERE player_name ILIKE '%Stephen%Curry%' ORDER BY player_name, gp DESC) sub2"
   - Config: {{ "playerNames": ["Stephen Curry"], "statDisplayName": "Season Comparison" }}
   - NOTE: When the dashboard intent hint is set to a profile/radar type, treat "X year vs X year" as a season comparison radar.

16. **"Show me a heat map of LeBron's shot selection"** (Career Shot Chart)
   - Type: "ShotChart"
   - SQL: "SELECT loc_x, loc_y, shot_made_flag FROM court_shots WHERE player_name ILIKE '%LeBron%James%'"
   - Config: {{ "playerNames": ["LeBron James"], "statDisplayName": "Shot Chart", "mode": "volume" }}

17. **"Show me a heat map of Curry's shots against the Lakers"** (Vs Team)
   - Type: "ShotChart"
   - SQL: "SELECT loc_x, loc_y, shot_made_flag FROM court_shots WHERE player_name ILIKE '%Stephen%Curry%' AND (htm ILIKE '%LAL%' OR vtm ILIKE '%LAL%')"
   - Config: {{ "playerNames": ["Stephen Curry"], "statDisplayName": "Shot Chart vs LAL", "mode": "volume" }}

18. **"Show me a heat map of Curry's 3 point shot selection"** (Filtered by shot type)
   - Type: "ShotChart"
   - SQL: "SELECT loc_x, loc_y, shot_made_flag FROM court_shots WHERE player_name ILIKE '%Stephen%Curry%' AND shot_type = '3PT Field Goal'"
   - Config: {{ "playerNames": ["Stephen Curry"], "statDisplayName": "3PT Shot Chart", "mode": "volume" }}

19. **"Show me Kobe's layups heat map"** (Filtered by zone)
   - Type: "ShotChart"
   - SQL: "SELECT loc_x, loc_y, shot_made_flag FROM court_shots WHERE player_name ILIKE '%Kobe%Bryant%' AND shot_zone_basic = 'Restricted Area'"
   - Config: {{ "playerNames": ["Kobe Bryant"], "statDisplayName": "Layups", "mode": "volume" }}

20. **"Show me LeBron's shooting percentages heat map"** (Accuracy)
   - Type: "ShotChart"
   - SQL: "SELECT loc_x, loc_y, shot_made_flag FROM court_shots WHERE player_name ILIKE '%LeBron%James%'"
   - Config: {{ "playerNames": ["LeBron James"], "statDisplayName": "Shooting Accuracy", "mode": "accuracy" }}

21. **"Show me Curry's best shooting zones"** (Hotspots)
   - Type: "ShotChart"
   - SQL: "SELECT loc_x, loc_y, shot_made_flag FROM court_shots WHERE player_name ILIKE '%Stephen%Curry%'"
   - Config: {{ "playerNames": ["Stephen Curry"], "statDisplayName": "Hot Spots", "mode": "hotspots" }}

22. **"Show me Westbrook's worst shot areas"** (Coldspots)
   - Type: "ShotChart"
   - SQL: "SELECT loc_x, loc_y, shot_made_flag FROM court_shots WHERE player_name ILIKE '%Russell%Westbrook%'"
   - Config: {{ "playerNames": ["Russell Westbrook"], "statDisplayName": "Cold Spots", "mode": "coldspots" }}

23. **"Where does Curry shoot from the most?"** (Volume)
   - Type: "ShotChart"
   - SQL: "SELECT loc_x, loc_y, shot_made_flag FROM court_shots WHERE player_name ILIKE '%Stephen%Curry%'"
   - Config: {{ "playerNames": ["Stephen Curry"], "statDisplayName": "Shot Frequency", "mode": "volume" }}

24. **"Scatter plot of points per game vs age this season"** (Scatter — two numeric axes, single table)
   - Type: "Scatter"
   - SQL: "SELECT player_name, age AS x_value, pts AS y_value FROM all_players_regular_2024_2025 WHERE gp >= 20 LIMIT 500"
   - Config: {{ "statDisplayName": "PPG vs Age", "xAxisLabel": "Age", "yAxisLabel": "Points Per Game" }}

25. **"Scatter of true shooting percentage vs PPG for 2023-24"** (Scatter — cross-table JOIN with text-storage cast)
   - Type: "Scatter"
   - SQL: "SELECT a.player_name, a.pts AS x_value, CAST(NULLIF(b.\\"TS_PCT\\", '') AS numeric) AS y_value FROM all_players_regular_2023_2024 a JOIN nba_advanced_season_2023_24_season_type_regular_season_per b ON a.player_name = b.\\"PLAYER_NAME\\" WHERE a.gp >= 20 LIMIT 500"
   - Config: {{ "statDisplayName": "TS% vs PPG", "xAxisLabel": "Points Per Game", "yAxisLabel": "True Shooting %" }}

26. **"Show me a scatter of true shooting percentages in 2018-19"** (Scatter — single-axis distribution, text-storage table)
   - Type: "Scatter"
   - SQL: "SELECT \\"PLAYER_NAME\\" AS player_name, CAST(NULLIF(\\"TS_PCT\\", '') AS numeric) AS y_value FROM nba_advanced_season_2018_19_season_type_regular_season_per WHERE CAST(NULLIF(\\"GP\\", '') AS numeric) >= 20 LIMIT 500"
   - Config: {{ "statDisplayName": "TS% Distribution 2018-19", "xAxisLabel": "", "yAxisLabel": "True Shooting %" }}
   - NOTE: When the user only mentions ONE stat (no "vs"), produce single-axis scatter — SELECT only player_name and y_value, set xAxisLabel="".

27. **"Plot rebounds per game vs assists per game in 2022-23 playoffs"** (Scatter — two-axis, playoffs table)
   - Type: "Scatter"
   - SQL: "SELECT player_name, ast AS x_value, reb AS y_value FROM all_players_playoffs_2022_2023 WHERE gp >= 5 LIMIT 500"
   - Config: {{ "statDisplayName": "REB vs AST (2022-23 Playoffs)", "xAxisLabel": "Assists Per Game", "yAxisLabel": "Rebounds Per Game" }}
   - NOTE: For playoff scatters, drop the gp threshold to 5 since playoff samples are small.

28. **"Scatter of usage rate vs PER for 2023-24"** (Scatter — both axes from text-storage table)
   - Type: "Scatter"
   - SQL: "SELECT \\"PLAYER_NAME\\" AS player_name, CAST(NULLIF(\\"USG_PCT\\", '') AS numeric) AS x_value, CAST(NULLIF(\\"PIE\\", '') AS numeric) AS y_value FROM nba_advanced_season_2023_24_season_type_regular_season_per WHERE CAST(NULLIF(\\"GP\\", '') AS numeric) >= 20 LIMIT 500"
   - Config: {{ "statDisplayName": "PIE vs Usage Rate", "xAxisLabel": "Usage Rate", "yAxisLabel": "PIE (PER equivalent)" }}
   - NOTE: When the user asks for "PER", we substitute "PIE" (the closest equivalent in our schema). Reflect that in yAxisLabel.

CRITICAL RULES FOR RADAR CHARTS:
- ALWAYS include `player_name` in your SELECT — even for single player radars. Without it, multi-player comparisons break.
- When comparing players from DIFFERENT seasons, use UNION ALL across different season tables and include a `season` string literal column.
- When comparing the SAME player across seasons, do the same UNION ALL pattern. The season column will be used to distinguish them.
- The playerNames array in chartConfig should list the player names as they appear in the database.
- ALWAYS use DISTINCT ON (player_name) inside a subquery for EACH branch of a UNION ALL to avoid duplicate rows for traded players. Each UNION ALL branch must wrap its query like: SELECT player_name, 'YYYY-YY' as season, pts, ast, reb, stl, blk FROM (SELECT DISTINCT ON (player_name) player_name, pts, ast, reb, stl, blk, gp FROM table WHERE ... ORDER BY player_name, gp DESC) subN
- This pattern works for any number of players (2, 3, 4, etc.) — just add more UNION ALL branches.

CRITICAL RULES FOR LEADERBOARDS:
- ALWAYS deduplicate with DISTINCT ON (player_name) ordered by gp DESC inside a subquery.
- The outer query then sorts by stat_value DESC and applies the LIMIT.
- This prevents traded players from appearing multiple times.

CRITICAL RULES FOR SCATTER CHARTS:
- The output column aliases MUST BE EXACTLY `player_name`, `x_value`, `y_value` (or `player_name` and `y_value` for single-axis). No other column names. No extras like `season`, `team_abbreviation`, or `rank`.
- Both axes must be NUMERIC. For text-storage families, wrap with CAST(NULLIF("COL", '') AS numeric). For numeric-typed tables (all_players_*, player_game_logs), use the raw column.
- Always include a sample-size filter: `gp >= 20` (regular) or `gp >= 5` (playoffs) for numeric tables, or the casted equivalent for text-storage tables.
- Always include `LIMIT 500`.
- DEDUPLICATE traded players. If a single-table query, wrap the FROM clause in `(SELECT DISTINCT ON (player_name) ... ORDER BY player_name, gp DESC)` so each player appears at most once.
- For cross-table JOINs (e.g. all_players_regular_* JOIN nba_advanced_*), join on `a.player_name = b."PLAYER_NAME"` and dedupe BEFORE the join if either side has duplicates.
- Do NOT use ORDER BY for scatter — order doesn't matter visually.
- The user's question determines axis labels. "X vs Y" → x_value = X, y_value = Y. "Scatter of Y" alone → single-axis, only y_value.
"""

# Max values for normalizing player stats to 0-100 scale
STAT_BENCHMARKS = {
    "PTS": 35.0,
    "AST": 11.0,
    "REB": 14.0,
    "STL": 2.5,
    "BLK": 2.5
}

# Column aliases GPT might use instead of the standard short names
STAT_ALIASES = {
    "PTS": ["PTS", "POINTS", "PPG"],
    "AST": ["AST", "ASSISTS", "APG"],
    "REB": ["REB", "REBOUNDS", "RPG", "TOTAL_REB"],
    "STL": ["STL", "STEALS", "SPG"],
    "BLK": ["BLK", "BLOCKS", "BPG"],
}

def _safe_upper_keys(row: Dict[str, Any]) -> Dict[str, Any]:
    return {str(k).upper(): v for k, v in row.items()}


def _find_stat_value(row_upper: Dict[str, Any], category: str) -> float:
    """Try multiple aliases to find a stat value in the row."""
    for alias in STAT_ALIASES.get(category, [category]):
        if alias in row_upper:
            val = row_upper[alias]
            try:
                return float(val or 0)
            except (ValueError, TypeError):
                return 0.0
    return 0.0


def _deduplicate_leaderboard(raw_data: List[Dict]) -> List[Dict]:
    """
    Safety net: if GPT forgot DISTINCT ON, deduplicate leaderboard data by player_name,
    keeping the row with the highest gp (or first occurrence if gp not available).
    """
    seen: Dict[str, Dict] = {}
    for row in raw_data:
        name = (row.get("player_name") or row.get("full_name") or "").strip().lower()
        if not name:
            continue
        if name not in seen:
            seen[name] = row
        else:
            # Keep the row with more games played
            existing_gp = seen[name].get("gp", 0) or 0
            new_gp = row.get("gp", 0) or 0
            if new_gp > existing_gp:
                seen[name] = row
    return list(seen.values())


def process_comparison_data(raw_data: List[Dict]) -> List[Dict]:
    """
    Takes raw SQL results and reshapes them for the comparison chart.
    Each season/game gets its own object with all players as keys.
    """
    if not raw_data:
        return []

    seasons: Dict[str, Dict[str, Any]] = {}
    for row in raw_data:
        player = row.get("full_name") or row.get("player_name") or "Unknown"
        val = row.get("stat_value", 0)
        season = row.get("season") or str(row.get("game_date")) or "Current"

        if season not in seasons:
            seasons[season] = {"season": season}
        seasons[season][player] = val

    return sorted(list(seasons.values()), key=lambda x: x["season"])


def process_categorical_data(raw_data: List[Dict], player_count: int) -> List[Dict]:
    """
    Converts player stats into radar chart format.
    Normalizes stats to 0-100 scale using benchmarks.
    """
    if not raw_data:
        return []

    categories = ["PTS", "AST", "REB", "STL", "BLK"]

    def _build_label(row: Dict) -> str:
        # player_name is already formatted as "Name (season)" by interpret_question
        return row.get("full_name") or row.get("player_name") or "Unknown"

    # Single player radar (only when 1 row and no cross-season)
    if player_count <= 1 and len(raw_data) == 1:
        first_row = raw_data[0]
        upper = _safe_upper_keys(first_row)
        radar_data: List[Dict[str, Any]] = []
        for cat in categories:
            raw_val = _find_stat_value(upper, cat)
            max_benchmark = STAT_BENCHMARKS.get(cat, 30)
            normalized = min(100, (raw_val / max_benchmark) * 100) if max_benchmark else 0
            
            radar_data.append({
                "category": cat,
                "value": int(normalized),
                "raw_value": round(raw_val, 1)
            })
        return radar_data

    # Multi player (or multi-season) comparison radar
    radar_map: Dict[str, Dict[str, Any]] = {cat: {"category": cat} for cat in categories}
    seen_labels: List[str] = []

    for row in raw_data:
        label = _build_label(row)
        upper = _safe_upper_keys(row)

        if label not in seen_labels:
            seen_labels.append(label)

        for cat in categories:
            raw_val = _find_stat_value(upper, cat)
            max_benchmark = STAT_BENCHMARKS.get(cat, 30)
            normalized = min(100, (raw_val / max_benchmark) * 100) if max_benchmark else 0
            radar_map[cat][label] = int(normalized)

    return list(radar_map.values())


ALLOWED_CHART_TYPES = {
    "Leaderboard",
    "CategoricalBreakdown",
    "CompareCategoricalBreakdown",
    "SinglePlayerStat",
    "CompareStats",
    "ShotChart",
    "Scatter",
}

RADAR_KEYS = {"PTS", "AST", "REB", "STL", "BLK"}


def _extract_names_heuristic(q: str) -> List[str]:
    # Strip possessives so "LeBron's" doesn't truncate the next word.
    cleaned_q = re.sub(r"['\u2019]s\b", "", q)
    # Strip leading verb phrases like "Compare", "Show me", "Plot", "Tell me"
    # at the START of the question so they don't get glued to a real player name
    # (e.g. "Compare LeBron and KD" must extract "LeBron" not "Compare LeBron").
    # We do this only at sentence start to avoid mangling mid-sentence content.
    cleaned_q = re.sub(
        r"^\s*(?:please\s+)?"
        r"(?:can\s+you\s+|could\s+you\s+|would\s+you\s+)?"
        r"(?:please\s+)?"
        r"(?:show\s+me|show|tell\s+me|tell|give\s+me|give|"
        r"compare|plot|graph|chart|display|find|fetch|get|pull\s+up|pull|"
        r"how\s+(?:has|did|does|do)|how\s+many|how\s+much|"
        r"who(?:'s|\s+is|\s+was|\s+are|\s+were|\s+had)?|"
        r"what(?:'s|\s+is|\s+are|\s+were|\s+was)?|"
        r"where\s+(?:is|are|was|were|does|do|did)?|"
        r"when\s+(?:is|are|was|were|does|do|did)?|"
        r"which|let\s+me\s+see|i\s+want\s+to\s+see)\b",
        "",
        cleaned_q,
        flags=re.IGNORECASE,
    )
    # Allow internal capitals (LeBron, McGrady, DeRozan, O'Neal etc.).
    name_token = r"[A-Z][A-Za-z][A-Za-z\-']*"
    candidates = re.findall(rf"\b{name_token}(?:\s+{name_token}){{0,2}}\b", cleaned_q)
    blacklist = {
        "Top", "Show", "Compare", "Vs", "Versus", "Skill", "Profile", "Points",
        "Assists", "Rebounds", "Playoffs", "Regular", "Season", "Last", "Games",
        "Trend", "Leaderboard", "Heat", "Shot", "Chart", "Map", "Best", "Worst",
        "Shooting", "Zones", "Selection",
        # Common verbs / question-words at sentence start.
        "Tell", "Give", "Make", "Create", "Build", "Find",
        "When", "Where", "Why", "How", "What", "Who",
        # Acronyms / stat tokens that look like names but aren't
        "PPG", "RPG", "APG", "SPG", "BPG", "MPG",
        "FG", "FGM", "FGA", "FT", "FTM", "FTA",
        "TS", "EFG", "USG", "PER", "PIE", "VORP", "BPM",
        "PTS", "REB", "AST", "STL", "BLK", "TOV",
        "NBA", "MVP", "DPOY", "ROY",
        # Months (so "March 2024" doesn't become a "name")
        "January", "February", "March", "April", "May", "June",
        "July", "August", "September", "October", "November", "December",
        # NBA team mascots — so "vs the Celtics" doesn't extract "Celtics" as a player.
        "Hawks", "Celtics", "Nets", "Hornets", "Bulls", "Cavaliers", "Cavs",
        "Mavericks", "Mavs", "Nuggets", "Pistons", "Warriors", "Rockets",
        "Pacers", "Clippers", "Lakers", "Grizzlies", "Heat", "Bucks",
        "Timberwolves", "Wolves", "Pelicans", "Knicks", "Thunder",
        "Magic", "Sixers", "Suns", "Blazers", "Trail", "Kings",
        "Spurs", "Raptors", "Jazz", "Wizards",
        # Other noise tokens
        "Plot", "Graph", "Distribution", "Scatter",
    }
    out = []
    seen = set()
    for c in candidates:
        # Single-word candidates that exactly match a blacklist word are noise.
        if " " not in c and c in blacklist:
            continue
        if c in seen:
            continue
        seen.add(c)
        out.append(c)
    return out


def _intent_hint(user_question: str) -> Dict[str, Any]:
    q = user_question.lower()
    hint: Dict[str, Any] = {}

    # ─────────────────────────────────────────────────────────────────────
    # ShotChart detection — broadened to cover natural language users actually type.
    # Previously missed: "hot zones", "cold zones", "shooting locations",
    # "shooting areas", "shooting spots", "where does X shoot/score from",
    # "where is X most accurate", "where can't X shoot", "most efficient
    # shooting areas", "best/worst spots".
    # ─────────────────────────────────────────────────────────────────────
    _shotchart_phrases = [
        "heat map", "heatmap", "shot chart", "shot selection", "shot map",
        "shooting zones", "shooting zone", "shooting locations", "shooting location",
        "shooting areas", "shooting area", "shooting spots", "shooting spot",
        "shot locations", "shot location", "shot frequency", "shooting percentages",
        "shooting accuracy", "shooting %", "shooting heatmap", "shot heatmap",
        "hot zones", "hot zone", "hotzone", "cold zones", "cold zone", "coldzone",
        "hot spot", "hotspot", "cold spot", "coldspot",
        "best shooting", "worst shooting",
        "money spot", "where can he shoot", "where can she shoot",
        "where can't", "where cant",
    ]
    is_shotchart = any(p in q for p in _shotchart_phrases)

    # "where does X shoot/score from" / "where can X shoot" — strong shotchart signal.
    # Also handles "where X can't shoot" with a subject between "where" and the verb.
    if not is_shotchart and re.search(
        r"\bwhere\s+(?:\w+\s+)?(?:does|do|is|are|can|can't|cant|cannot)\b", q
    ):
        if any(w in q for w in (
            "shoot", "shooting", "score", "scoring", "best", "worst",
            "accurate", "efficient", "most", "least"
        )):
            is_shotchart = True

    # "most efficient shooting" / "most accurate" → shotchart accuracy mode
    if not is_shotchart and any(w in q for w in ("efficient shooting", "most accurate", "least accurate")):
        is_shotchart = True

    # Defer to scatter if the user explicitly said scatter — so
    # "scatter of shooting percentages" doesn't get hijacked by ShotChart.
    if is_shotchart and any(w in q for w in ("scatter", "scatterplot", "scatter plot", "distribution")):
        is_shotchart = False

    # If the user named 2+ players, "shooting percentages" is a player-vs-player
    # comparison, not a shot map. We require multi-word names ("LeBron James")
    # because single-token candidates may be artifacts of split possessives like
    # "Shaquille O'Neal" → ["Shaquille", "Neal"].
    def _multi_word_player_count(question: str) -> int:
        return sum(1 for n in _extract_names_heuristic(question) if " " in n.strip())

    if is_shotchart and _multi_word_player_count(user_question) >= 2:
        is_shotchart = False

    # Compare/comparison of shot stats with two players → CompareStats, not ShotChart.
    if is_shotchart and ("compare" in q or " versus " in q):
        if _multi_word_player_count(user_question) >= 2:
            is_shotchart = False

    if is_shotchart:
        hint["suspectedCompare"] = False
        hint["suspectedLeaderboard"] = False
        hint["suspectedProfile"] = False
        hint["suspectedShotChart"] = True
        hint["suspectedScatter"] = False
        hint["suspectedGameLog"] = False
        hint["preferredChartType"] = "ShotChart"

        # Mode detection — order matters: hot/cold spot wins over generic "best/worst",
        # accuracy wins over volume.
        if any(w in q for w in ["hot spot", "hotspot", "hot zone", "hotzone", "money spot", "best shooting", "best zone"]):
            hint["suggestedMode"] = "hotspots"
        elif any(w in q for w in ["cold spot", "coldspot", "cold zone", "coldzone", "worst shooting", "worst zone", "struggle", "where can't", "where cant", "least accurate", "inefficient"]):
            hint["suggestedMode"] = "coldspots"
        elif any(w in q for w in ["percentage", "accuracy", "accurate", "efficient", "efficiency", "shooting %"]):
            hint["suggestedMode"] = "accuracy"
        else:
            hint["suggestedMode"] = "volume"

        hint["guessedNames"] = _extract_names_heuristic(user_question)[:4]
        return hint

    # Not a shot chart
    is_profile = any(w in q for w in ["skill profile", "profile", "radar", "breakdown", "categories", "categorical"])

    # ─────────────────────────────────────────────────────────────────────
    # Scatter detection — runs BEFORE compare so "TS% vs PPG" doesn't get
    # routed to CompareStats. Triggers:
    #   1) explicit scatter/scatterplot/plot/distribution keywords
    #   2) "X vs Y" / "X against Y" between stat words (not player names)
    #   3) "X and Y across all players" type framing
    # ─────────────────────────────────────────────────────────────────────
    _SCATTER_TRIGGER_WORDS = (
        "scatter", "scatter plot", "scatterplot",
        "plot ", "distribution",
    )
    _STAT_WORDS = (
        "ppg", "points per game", "rpg", "rebounds per game",
        "apg", "assists per game", "spg", "steals per game", "bpg", "blocks per game",
        "true shooting", "ts%", "ts pct", "ts_pct", "efg", "efg%",
        "usage rate", "usg%", "usg_pct", "off rating", "def rating", "net rating",
        "per", "pie", "win shares", "ws", "vorp", "bpm",
        "age", "minutes", "min ", "mpg",
        "fg%", "fg_pct", "field goal", "3pt%", "fg3_pct", "three point", "3-point",
        "fta", "fga", "fgm", "fg3m", "fg3a",
        "drives", "deflections", "rebounds", "assists",
        "points", "blocks", "steals", "turnovers",
        "salary",
    )
    _CROSS_LEAGUE_PHRASES = (
        "across all players", "across players", "for all players",
        "for every player", "across the league", "all players in",
    )

    def _has_compare_verb_with_stats(question_lower: str) -> bool:
        """'compare X and Y' or 'compare X vs Y' where X, Y are stat words."""
        if "compare" not in question_lower:
            return False
        names = _extract_names_heuristic(user_question)
        # If we found 2+ player names, it's a player comparison, not scatter.
        if len(names) >= 2:
            return False
        # No real player names + 2+ stat tokens + "across players" framing → scatter
        stat_hits = sum(1 for s in _STAT_WORDS if s in question_lower)
        if stat_hits >= 2 and any(p in question_lower for p in _CROSS_LEAGUE_PHRASES):
            return True
        return False

    def _looks_like_scatter(question_lower: str) -> bool:
        # 1) Explicit keyword
        if any(w in question_lower for w in _SCATTER_TRIGGER_WORDS):
            return True
        # 2) "graph X against Y" — colloquial scatter
        if "graph " in question_lower and (" against " in question_lower or " vs " in question_lower):
            return True
        # 3) "X vs/against Y" with stat words on both sides (no players)
        names = _extract_names_heuristic(user_question)
        has_vs = (" vs " in question_lower or " versus " in question_lower
                  or " against " in question_lower)
        if has_vs and len(names) <= 1:
            stat_hits = sum(1 for s in _STAT_WORDS if s in question_lower)
            if stat_hits >= 2:
                return True
        # 4) "compare X and Y across all players" — stat-comparison framing
        if _has_compare_verb_with_stats(question_lower):
            return True
        return False

    is_scatter = _looks_like_scatter(q)

    # ─────────────────────────────────────────────────────────────────────
    # Trend detection — must run BEFORE compare detection because "from 2010
    # to 2023" looks like a year-vs-year compare otherwise. Also gates the
    # year-range regex so it only fires for real comparisons.
    # ─────────────────────────────────────────────────────────────────────
    _TREND_KEYWORDS = (
        "trend", "track", "over time", "over the years", "over his career",
        "over her career", "across seasons", "by season", "per season",
        "by year", "per year", "season-over-season", "season over season",
        "evolved", "evolution", "progression", "changed", "changing",
        "improved", "improvement", "history of",
    )
    is_trend = any(w in q for w in _TREND_KEYWORDS)

    # ─────────────────────────────────────────────────────────────────────
    # Compare detection — broadened to handle "compare X" at sentence start
    # (previously only matched " compare " with leading space). Year-range
    # patterns like "2015 vs 2023" only fire when they are NOT trends.
    # ─────────────────────────────────────────────────────────────────────
    is_compare = (
        " compare " in q
        or q.startswith("compare ")
        or " vs " in q
        or " versus " in q
        or " v " in q
        or " vs." in q
        or q.startswith("vs ")
    )
    # Year-vs-year and year-to-year only count as "compare" when the user is
    # NOT asking for a trend. "from 2010 to 2023" = trend; "Curry 2015 vs 2023" = compare.
    if not is_trend:
        if re.search(r"\b(19\d{2}|20\d{2})\b.*\bvs\b.*\b(19\d{2}|20\d{2})\b", q):
            is_compare = True
        if re.search(r"\b(19\d{2}|20\d{2})\b\s+to\s+\b(19\d{2}|20\d{2})\b", q):
            is_compare = True

    # Scatter takes precedence over compare when triggered.
    if is_scatter:
        is_compare = False
        is_profile = False

    # Same-player-two-seasons radar pattern: heuristic finds only one unique
    # name when the player is repeated. Treat it as a comparison if profile-y
    # language is present and at least two years are mentioned.
    repeated_name_radar = (
        is_profile
        and len(_extract_names_heuristic(user_question)) == 1
        and len(re.findall(r"\b(19\d{2}|20\d{2})\b", user_question)) >= 2
    )
    if repeated_name_radar:
        is_compare = True

    # ─────────────────────────────────────────────────────────────────────
    # Leaderboard detection — broadened with natural phrasings users try.
    # Previously missed: "who leads", "highest", "greatest", "scoring players".
    # ─────────────────────────────────────────────────────────────────────
    _LEADERBOARD_KEYWORDS = (
        "top ", "top-", "leaders ", "league leaders", "leaderboard",
        "most ", "highest", "greatest", "rank", "ranked",
        "who leads", "who led", "who's leading", "who is leading",
        "best ",
        "scoring leader", "scoring leaders", "scoring players",
    )
    is_leaderboard = any(w in q for w in _LEADERBOARD_KEYWORDS)
    # "show me the X scoring/rebounding/assist players" → leaderboard
    if re.search(r"\b(highest|top|best|greatest|leading)\s+\w*\s*(scoring|rebounding|assist|defensive|shooting)\s+players?\b", q):
        is_leaderboard = True
    # If a profile/radar is being asked for, "best" is descriptive, not a leaderboard.
    if is_profile:
        is_leaderboard = False

    is_recent = any(w in q for w in [
        "last ", "past ", "recent", "game log", "gamelog",
        "playoffs", "march", "april", "january", "february",
        "december", "november", "october"
    ])

    # Detect multi-player trend (2+ names + trend marker)
    # IMPORTANT: bare "from "/" to " no longer counts — that fires on
    # single-player trends too. Require an actual trend keyword.
    guessed_names = _extract_names_heuristic(user_question)[:4]
    is_multi_player_trend = len(guessed_names) >= 2 and is_trend

    if any(w in q for w in ["against ", "vs the"]):
        # "vs the Celtics" is a matchup question, handled elsewhere as a game log.
        if is_recent or "game" in q:
            pass
        else:
            is_compare = True

    hint["suspectedCompare"] = is_compare or is_multi_player_trend
    hint["suspectedLeaderboard"] = is_leaderboard
    hint["suspectedProfile"] = is_profile
    hint["suspectedShotChart"] = False
    hint["suspectedScatter"] = is_scatter
    hint["suspectedGameLog"] = is_recent
    hint["suspectedTrend"] = is_trend

    if is_scatter:
        hint["preferredChartType"] = "Scatter"
    elif is_profile and is_compare:
        hint["preferredChartType"] = "CompareCategoricalBreakdown"
    elif is_profile:
        hint["preferredChartType"] = "CategoricalBreakdown"
    elif is_leaderboard:
        hint["preferredChartType"] = "Leaderboard"
    elif is_compare or is_multi_player_trend:
        hint["preferredChartType"] = "CompareStats"
    else:
        hint["preferredChartType"] = "SinglePlayerStat"

    hint["guessedNames"] = guessed_names
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
    # Check column shape first — radar data has raw stat columns, not stat_value
    upper = set(_safe_upper_keys(raw_data[0]).keys())
    if "STAT_VALUE" in upper:
        return False
    if len(RADAR_KEYS.intersection(upper)) < 3:
        return False
    # Allow up to ~20 rows to accommodate traded-player duplicates across
    # multiple players/seasons (e.g. 5 players × up to 4 team stints each)
    if len(raw_data) > 20:
        return False
    return True


def _validate_and_autofix(chart_type: str, raw_data: List[Dict], chart_config: Dict[str, Any]) -> Tuple[bool, str, str]:
    if chart_type not in ALLOWED_CHART_TYPES:
        return False, chart_type, f"chartType '{chart_type}' not allowed"

    cols = _columns_present(raw_data)

    # ShotChart validation
    if chart_type == "ShotChart":
        if "loc_x" in cols and "loc_y" in cols and "shot_made_flag" in cols:
            return True, chart_type, "ok"
        return False, chart_type, "ShotChart requires loc_x, loc_y, shot_made_flag columns"

    # Scatter validation (two-axis or single-axis)
    if chart_type == "Scatter":
        has_player = ("player_name" in cols) or ("full_name" in cols)
        has_y = "y_value" in cols
        has_x = "x_value" in cols
        if not has_player:
            return False, chart_type, "Scatter requires 'player_name' (or 'full_name')"
        if not has_y:
            return False, chart_type, "Scatter requires 'y_value' column"
        # Numeric sanity check on a sample of rows. We accept ints, floats, numeric strings.
        def _is_numericish(v):
            if v is None:
                return True  # nulls allowed; renderer drops them
            if isinstance(v, (int, float)):
                return True
            try:
                float(str(v).strip())
                return True
            except (ValueError, TypeError):
                return False
        sample = raw_data[:50]
        if not all(_is_numericish(r.get("y_value")) for r in sample):
            return False, chart_type, "Scatter y_value must be numeric — add CAST(NULLIF(\"COL\", '') AS numeric) for text-storage tables"
        if has_x and not all(_is_numericish(r.get("x_value")) for r in sample):
            return False, chart_type, "Scatter x_value must be numeric — add CAST(NULLIF(\"COL\", '') AS numeric) for text-storage tables"
        # Hard cap to keep the chart readable
        if len(raw_data) > 1000:
            return False, chart_type, f"Scatter returned {len(raw_data)} rows — add a tighter sample filter (e.g. WHERE gp >= 20) and LIMIT 500"
        return True, chart_type, "ok"

    # Auto-detect shot chart data
    if "loc_x" in cols and "loc_y" in cols and "shot_made_flag" in cols:
        return True, "ShotChart", "Auto-corrected to ShotChart based on loc_x/loc_y columns"

    # Radar detection override
    if _looks_like_radar(raw_data):
        player_names = chart_config.get("playerNames", []) or []
        unique_players = set()
        for row in raw_data:
            p = row.get("player_name") or row.get("full_name")
            if p:
                unique_players.add(p.strip().lower())

        is_multi = len(unique_players) > 1 or len(raw_data) > 1 or len(player_names) > 1
        expected = "CompareCategoricalBreakdown" if is_multi else "CategoricalBreakdown"

        if chart_type not in {"CategoricalBreakdown", "CompareCategoricalBreakdown"}:
            return True, expected, "Auto-corrected chartType based on radar-shaped SQL output"
        if expected != chart_type:
            return True, expected, "Auto-corrected radar subtype based on data shape"
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

    # CategoricalBreakdown requires radar columns
    if chart_type in {"CategoricalBreakdown", "CompareCategoricalBreakdown"}:
        if "stat_value" in cols:
            if ("season" in cols or "game_date" in cols):
                if "full_name" in cols or "player_name" in cols:
                    return True, "CompareStats", "Auto-recovered: radar requested but data shaped for CompareStats"
                return True, "SinglePlayerStat", "Auto-recovered: radar requested but data shaped for SinglePlayerStat"
            if "player_name" in cols or "full_name" in cols:
                return True, "Leaderboard", "Auto-recovered: radar requested but data shaped for Leaderboard"
        return False, chart_type, "Radar chart type selected but SQL output does not look like radar stats"

    return True, chart_type, "ok"


def _call_gpt_for_interpretation(user_question: str, hint: Dict[str, Any], repair_message: Optional[str] = None) -> Dict[str, Any]:
    messages = [{"role": "system", "content": build_system_prompt()}]

    messages.append({
        "role": "system",
        "content": f"""Extra hint (not mandatory, but try to follow it if it matches the question):
- preferredChartType: {hint.get('preferredChartType')}
- suspectedCompare: {hint.get('suspectedCompare')}
- suspectedLeaderboard: {hint.get('suspectedLeaderboard')}
- suspectedProfile: {hint.get('suspectedProfile')}
- suspectedShotChart: {hint.get('suspectedShotChart')}
- suspectedScatter: {hint.get('suspectedScatter')}
- suggestedMode: {hint.get('suggestedMode', 'N/A')}
- guessedNames: {hint.get('guessedNames')}

Still follow the main schema exactly, and only output valid JSON."""
    })

    if repair_message:
        messages.append({"role": "system", "content": repair_message})

    messages.append({"role": "user", "content": user_question})

    response = client.chat.completions.create(
        model="gpt-4.1-mini",
        messages=messages,
        temperature=0.1,
        response_format={"type": "json_object"}
    )

    interpretation = json.loads(response.choices[0].message.content)

    chart_type = interpretation.get("chartType")
    if chart_type and isinstance(chart_type, str):
        interpretation["chartType"] = chart_type.strip()

    if "chartConfig" not in interpretation or not isinstance(interpretation["chartConfig"], dict):
        interpretation["chartConfig"] = {}

    interpretation["chartConfig"].setdefault("statKey", "stat_value")
    interpretation["chartConfig"].setdefault("playerNames", [])
    interpretation["chartConfig"].setdefault("xAxisKey", "season")
    interpretation["chartConfig"].setdefault("statDisplayName", "Stat")

    return interpretation


def interpret_question(user_question: str) -> Dict[str, Any]:
    conn = None
    try:
        print(f"Analyzing question: {user_question}")

        hint = _intent_hint(user_question)

        interpretation = _call_gpt_for_interpretation(user_question, hint)

        chart_type = interpretation.get("chartType", "")
        sql_query = interpretation.get("sqlQuery", "")
        chart_config = interpretation.get("chartConfig", {})

        print(f"Chart Type: {chart_type}")
        print(f"Generated SQL: {sql_query}")

        conn = psycopg2.connect(**DB_CONFIG)
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(sql_query)
            raw_data = cursor.fetchall()

        if not raw_data:
            return {
                "success": False,
                "error": "No data found. The player may not exist in the selected table or time period. Try checking the spelling or adjusting the season/year."
            }

        ok, fixed_type, reason = _validate_and_autofix(chart_type, raw_data, chart_config)
        if fixed_type != chart_type:
            print(f"[AutoFix] chartType {chart_type} -> {fixed_type} ({reason})")
            chart_type = fixed_type

        if not ok:
            repair_msg = (
                "Your previous output caused a schema mismatch when executing the SQL. "
                f"Reason: {reason}. "
                "Return corrected JSON. You may change chartType and/or SQL so it matches the OUTPUT SHAPE RULES.\n\n"
                "If the user asked for a 'skill profile' or 'radar' comparison across seasons "
                "(including bare-name shapes like \"X profile YEAR vs X profile YEAR\" or \"X YEAR vs X YEAR\"), "
                "the chartType MUST be 'CompareCategoricalBreakdown' and the SQL MUST use the "
                "UNION ALL pattern from examples 15, 15b, 15c, and 15d — one branch per (player, season) "
                "with a string-literal `season` column and DISTINCT ON (player_name) inside each branch.\n\n"
                "If the user asked for a 'scatter' / 'scatter plot' / 'plot X vs Y' / 'distribution', "
                "the chartType MUST be 'Scatter'. The SQL MUST output exactly `player_name`, `x_value`, "
                "`y_value` (or just `player_name`, `y_value` for single-axis distributions). "
                "Both axes must be numeric — wrap text-storage columns with CAST(NULLIF(\"COL\", '') AS numeric). "
                "Apply WHERE gp >= 20 (or the casted equivalent) and LIMIT 500. "
                "Use examples 24, 25, 26, 27, and 28 as the templates.\n\n"
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

            if not raw_data:
                return {
                    "success": False,
                    "error": "No data found after retry. The player may not exist in the database or the time period is incorrect."
                }

            ok, fixed_type, reason = _validate_and_autofix(chart_type, raw_data, chart_config)
            if fixed_type != chart_type:
                print(f"[AutoFix] chartType {chart_type} -> {fixed_type} ({reason})")
                chart_type = fixed_type

            if not ok:
                return {"success": False, "error": f"Interpreter mismatch after retry: {reason}"}

        # Format data based on chart type
        final_data = raw_data

        if chart_type == "Leaderboard":
            # Safety net: deduplicate even if GPT forgot DISTINCT ON
            final_data = _deduplicate_leaderboard(raw_data)

        elif chart_type == "CompareStats":
            final_data = process_comparison_data(raw_data)

        elif chart_type in {"CategoricalBreakdown", "CompareCategoricalBreakdown"}:
            # Use an ordered list instead of a set to keep colors consistent
            unique_labels = []
            for row in raw_data:
                name = row.get("player_name") or row.get("full_name") or "Unknown"
                season = row.get("season")
                label = f"{name} ({season})" if season else name
                
                if label not in unique_labels:
                    unique_labels.append(label)
                
                # Assign the unique label back to the row for pivoting
                if "player_name" in row:
                    row["player_name"] = label
                elif "full_name" in row:
                    row["full_name"] = label

            inferred_count = len(unique_labels)

            if inferred_count > 1:
                chart_type = "CompareCategoricalBreakdown"
            else:
                chart_type = "CategoricalBreakdown"

            chart_config["playerNames"] = unique_labels

            final_data = process_categorical_data(raw_data, inferred_count)

        elif chart_type == "ShotChart":
            final_data = raw_data

        if not final_data:
            return {
                "success": False,
                "error": "Query returned data but it could not be formatted for the requested chart type. Try rephrasing your question."
            }
            
        return {
            "success": True,
            "chartType": chart_type,
            "data": final_data,
            "config": chart_config,
        }

    except Exception as e:
        print(f"Error occurred: {e}")
        return {"success": False, "error": str(e)}
    finally:
        if conn:
            conn.close()