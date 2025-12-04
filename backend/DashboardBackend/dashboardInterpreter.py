import os
import json
import psycopg2
from psycopg2.extras import RealDictCursor
from openai import OpenAI
from typing import Dict, List, Any
from dotenv import load_dotenv

load_dotenv()
# Setting up OpenAI client
print("API Key Loaded:", os.getenv("OPENAI_API_KEY"))
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# AWS postgres database inmo
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
"""

def build_system_prompt() -> str:
    return f"""You are an NBA analytics assistant.
{DATABASE_SCHEMA}

Return JSON with this structure:
{{
  "chartType": "Leaderboard|CategoricalBreakdown|SinglePlayerStat|CompareStats", 
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

def process_comparison_data(raw_data: List[Dict]) -> List[Dict]:
    """
    Takes raw SQL results and reshapes them for the comparison chart.
    Each season/game gets its own object with all players as keys.
    """
    if not raw_data: 
        return []
    
    # Group by time period (season or date)
    seasons = {} 
    for row in raw_data:
        player = row.get('full_name') or row.get('player_name') or 'Unknown'
        val = row.get('stat_value', 0)
        season = row.get('season') or str(row.get('game_date')) or 'Current'
        
        if season not in seasons: 
            seasons[season] = {"season": season}
        seasons[season][player] = val
    
    # Sort chronologically
    return sorted(list(seasons.values()), key=lambda x: x['season'])

def process_categorical_data(raw_data: List[Dict], player_count: int) -> List[Dict]:
    """
    Converts player stats into radar chart format.
    Normalizes stats to 0-100 scale using benchmarks.
    """
    if not raw_data: 
        return []
    
    first_row = raw_data[0] 
    categories = ["PTS", "AST", "REB", "STL", "BLK"]
    
    # Single player radar
    if player_count <= 1:
        radar_data = []
        for cat in categories:
            # Find the stat value
            raw_val = next((v for k, v in first_row.items() if k.upper() == cat), 0)
            max_benchmark = STAT_BENCHMARKS.get(cat, 30)
            normalized = min(100, (raw_val / max_benchmark) * 100)
            
            radar_data.append({
                "category": cat,
                "value": int(normalized),
                "raw_value": round(raw_val, 1)
            })
        return radar_data

    # Multi player comparison radar
    radar_map = {cat: {"category": cat} for cat in categories}
    for row in raw_data:
        player = row.get('full_name') or row.get('player_name') or 'Unknown'
        for cat in categories:
            raw_val = next((v for k, v in row.items() if k.upper() == cat), 0)
            max_benchmark = STAT_BENCHMARKS.get(cat, 30)
            normalized = min(100, (raw_val / max_benchmark) * 100)
            radar_map[cat][player] = int(normalized) 

    return list(radar_map.values())

def interpret_question(user_question: str) -> Dict[str, Any]:
    """
    Main function that:
    1. Sends question to GPT to get SQL + chart type
    2. Runs the SQL query
    3. Formats the data for the frontend
    """
    conn = None
    try:
        print(f"Analyzing question: {user_question}")
        
        # Ask GPT to interpret the question
        response = client.chat.completions.create(
            model="gpt-4-turbo-preview", 
            messages=[
                {"role": "system", "content": build_system_prompt()},
                {"role": "user", "content": user_question}
            ],
            temperature=0.1,
            response_format={"type": "json_object"} 
        )
        
        interpretation = json.loads(response.choices[0].message.content)
        print(f"Chart Type: {interpretation['chartType']}")
        print(f"Generated SQL: {interpretation['sqlQuery']}")
        
        # Connect to database and run the query
        conn = psycopg2.connect(**DB_CONFIG)
        
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(interpretation['sqlQuery'])
            raw_data = cursor.fetchall()

        # Format data based on chart type
        final_data = raw_data
        
        if interpretation['chartType'] == "CompareStats":
            final_data = process_comparison_data(raw_data)
        elif interpretation['chartType'] == "CategoricalBreakdown":
            player_names = interpretation['chartConfig'].get('playerNames', [])
            final_data = process_categorical_data(raw_data, len(player_names))
        
        return {
            "success": True,
            "chartType": interpretation['chartType'],
            "data": final_data,
            "config": interpretation['chartConfig'],
        }
        
    except Exception as e:
        print(f"Error occurred: {e}")
        return {
            "success": False, 
            "error": str(e)
        }
    finally:
        if conn:
            conn.close()