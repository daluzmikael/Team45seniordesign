import os
import sys
import re
from dataclasses import dataclass
from typing import Optional, Any, List, Set, Dict
from pathlib import Path
import importlib.util

import numpy as np
import pandas as pd
from Interpreter.interpreter import run_query

from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()
api_key = os.getenv("OPENAI_API_KEY")
client = OpenAI(
    api_key=api_key,
    base_url="https://us.api.openai.com/v1"
)


def _resolve_user_input(module: Optional[Any]) -> Optional[str]:
    if module is not None and hasattr(module, "user_input"):
        return getattr(module, "user_input")
    return None



# -------------------------
# Display column renames
# -------------------------
# Map confusing or non-existent stat columns to friendlier display names so
# the analyzer narrative + table never confuse users. Add new mappings here.
_DISPLAY_COLUMN_RENAMES = {
    "PIE":  "PIE (PER equivalent)",
    "pie":  "PIE (PER equivalent)",
}


def _rename_columns_for_display(df):
    """Return a shallow-copied df with confusing column names rewritten for display."""
    if df is None or df.empty:
        return df
    rename_map = {c: _DISPLAY_COLUMN_RENAMES[c] for c in df.columns if c in _DISPLAY_COLUMN_RENAMES}
    if not rename_map:
        return df
    return df.rename(columns=rename_map)


def _user_asked_for_per(question: str) -> bool:
    """True only when the user explicitly asked for classic PER (Hollinger).

    Avoids matching 'per game', 'per minute', 'per possession', 'performance'.
    """
    q = (question or "").lower()
    return bool(re.search(r"\b(player efficiency rating|per)\b(?!\s*game|formance|\s*minute|\s*possession)", q))


# -------------------------
# Composite scoring utilities
# -------------------------
@dataclass
class ScoreConfig:
    weights: Dict[str, float]
    invert: Set[str]
    tiebreakers: List[str]

# 0.02 → Barely matters

# 0.05 → Minor contributor

# 0.08–0.12 → More meaningful

# 0.15+ → Core pillar of the domain

DEFENSE_CONFIG = ScoreConfig(
    weights={
        # --- overall / impact ---
        "defensive_impact": 0.26,          
        "def_rtg": 0.08,                   
        "def_ws": 0.03,                    

        # --- rim protection ---
        "rim_fg_pct_allowed": 0.14,        
        "rim_shots_contested": 0.10,       
        "blk_per_game": 0.03,
        "blk_pct": 0.06,

        # --- on-ball / matchup ---
        "opp_fg_pct_as_primary_defender": 0.09,  
        "matchup_difficulty": 0.05,              

        # --- disruption / activity ---
        "deflections_per_game": 0.07,
        "loose_balls_recovered": 0.04,
        "charges_drawn": 0.02,
        "stl_per_game": 0.02,
        "stl_pct": 0.04,

        # --- versatility / mistake reduction ---
        "versatility_index": 0.05,
        "fouls_per_game": 0.02,            
        "dreb_pct": 0.07,                  
    },
    invert={
        "rim_fg_pct_allowed",
        "opp_fg_pct_as_primary_defender",
        "def_rtg",
        "fouls_per_game",
    },
    tiebreakers=[
        "defensive_impact",
        "rim_fg_pct_allowed",
        "opp_fg_pct_as_primary_defender",
        "rim_shots_contested",
        "blk_pct",
        "stl_pct",
        "deflections_per_game",
        "versatility_index",
    ],
)


SHOOTING_CONFIG = ScoreConfig(
    weights={
        "three_pt_pct": 0.28,
        "three_pm": 0.16,
        "three_pa": 0.08,

        "ts_pct": 0.12,                    
        "efg_pct": 0.08,                   
        "ft_pct": 0.05,

        "three_par": 0.06,                 
        "ft_rate": 0.03,                   
        "corner3_pct": 0.04,
        "catch_shoot_3p_pct": 0.06,
        "pullup_3p_pct": 0.04,
    },
    invert=set(),
    tiebreakers=[
        "three_pt_pct",
        "three_pm",
        "catch_shoot_3p_pct",
        "pullup_3p_pct",
        "three_pa",
        "ts_pct",
        "efg_pct",
    ],
)

PLAYMAKING_CONFIG = ScoreConfig(
    weights={
        "ast_per_game": 0.26,
        "ast_pct": 0.14,
        "potential_ast": 0.10,

        "assist_points_created": 0.10,
        "secondary_ast": 0.06,             
        "passes_made": 0.05,
        "time_of_poss": 0.04,              
        "usage_pct": 0.03,                 

        # Ball security (efficiency)
        "tov_per_game": 0.08,              
        "tov_pct": 0.08,                   
        "ast_to_tov": 0.06,
    },
    invert={"tov_per_game", "tov_pct"},
    tiebreakers=[
        "ast_per_game",
        "ast_pct",
        "assist_points_created",
        "potential_ast",
        "ast_to_tov",
        "tov_pct",
        "tov_per_game",
    ],
)

SCORING_CONFIG = ScoreConfig(
    weights={
        # Output + efficiency
        "ppg": 0.26,
        "ts_pct": 0.22,
        "efg_pct": 0.08,

        # Volume / load
        "fga": 0.10,
        "usage_pct": 0.10,

        # How they get points (helps separate archetypes)
        "fta": 0.06,
        "ft_rate": 0.05,
        "three_pa": 0.04,
        "three_pm": 0.03,

        # Mistakes that reduce scoring value
        "tov_per_game": 0.04,              # lower better
        "tov_pct": 0.02,                   # lower better
    },
    invert={"tov_per_game", "tov_pct"},
    tiebreakers=["ppg", "ts_pct", "usage_pct", "fga", "ft_rate", "three_pa"],
)

REBOUNDING_CONFIG = ScoreConfig(
    weights={
        "trb_per_game": 0.20,
        "oreb_per_game": 0.10,
        "dreb_per_game": 0.10,

        "trb_pct": 0.20,
        "oreb_pct": 0.15,
        "dreb_pct": 0.15,

        "contested_reb": 0.10,
    },
    invert=set(),
    tiebreakers=["trb_pct", "trb_per_game", "dreb_pct", "oreb_pct", "contested_reb"],
)


DOMAIN_CONFIGS: Dict[str, ScoreConfig] = {
    "defense": DEFENSE_CONFIG,
    "shooting": SHOOTING_CONFIG,
    "playmaking": PLAYMAKING_CONFIG,
    "scoring": SCORING_CONFIG,
    "rebounding": REBOUNDING_CONFIG,
}

# Kept for possible future experimentation, but disabled for production chat output.
ENABLE_COMPOSITE_SCORING = False



def _minmax(series: pd.Series) -> np.ndarray:
    arr = series.astype(float).to_numpy(copy=False)
    if np.isnan(arr).all():
        return np.zeros_like(arr)
    m = np.nanmin(arr)
    M = np.nanmax(arr)
    if not np.isfinite(m) or not np.isfinite(M) or M - m == 0:
        return np.zeros_like(arr)
    return (arr - m) / (M - m)


def compute_scores(df: pd.DataFrame, cfg: ScoreConfig) -> pd.DataFrame:
    entity_col = "player_name"
    if "player_name" not in df.columns:
        if "TEAM_NAME" in df.columns:
            entity_col = "TEAM_NAME"
        elif "TeamName" in df.columns:
            entity_col = "TeamName"
        elif "team_abbreviation" in df.columns:
            entity_col = "team_abbreviation"
        else:
            raise ValueError("DataFrame must include 'player_name' or a valid team column.")

    present = [c for c in cfg.weights if c in df.columns]
    if not present:
        raise ValueError("No required metric columns found for this domain.")

    norm: Dict[str, np.ndarray] = {}
    for c in present:
        v = _minmax(df[c])
        if c in cfg.invert:
            v = 1.0 - v
        norm[c] = v

    score = np.zeros(len(df))
    for c in present:
        score += cfg.weights[c] * norm[c]

    def tb_key(i: int):
        keys: List[float] = []
        for c in cfg.tiebreakers:
            if c in present:
                keys.append(float(norm[c][i]))
        return tuple(keys)

    order = sorted(range(len(df)), key=lambda i: (score[i], *tb_key(i)), reverse=True)
    rows = []
    for i in order:
        contribs = []
        for c in present:
            contribs.append((c, float(cfg.weights[c] * norm[c][i])))
        contribs.sort(key=lambda x: x[1], reverse=True)
        rows.append(
            {
                entity_col: df.iloc[i][entity_col],
                "composite_score": float(score[i]),
                "top_contributors": contribs[:3],
            }
        )
    return pd.DataFrame(rows)


def infer_domain(user_q: str, cols: List[str]) -> str:
    uq = (user_q or "").lower()
    txt = " ".join(cols).lower()

    if any(k in uq for k in ["team", "franchise", "standings", "winrate"]) or any(
        k in txt for k in ["teamname", "team_name", "w_pct", "wins", "net_rating"]
    ):
        return "team_performance"

    if any(k in uq for k in ["rebound", "boards", "glass", "oreb", "dreb"]) or any(
        k in txt for k in ["trb_per_game", "trb_pct", "oreb_pct", "dreb_pct", "contested_reb"]
    ):
        return "rebounding"

    if any(k in uq for k in ["mvp", "best player", "top players", "overall", "impact", "most valuable"]) or any(
        k in txt for k in ["overall_impact", "on_off"]
    ):
        return "overall_impact"

    if any(k in uq for k in ["defense", "defensive", "defender", "rim", "steal", "block", "deflect", "contest"]) or any(
        k in txt for k in ["defensive_impact", "rim_fg_pct_allowed", "deflections_per_game", "def_rtg"]
    ):
        return "defense"

    if any(k in uq for k in ["shoot", "3pt", "three", "percentage", "catch-and-shoot", "pull-up"]) or any(
        k in txt for k in ["three_pt_pct", "three_pm", "three_pa", "catch_shoot_3p_pct", "pullup_3p_pct"]
    ):
        return "shooting"

    if any(k in uq for k in ["assist", "playmaker", "passing", "creator"]) or any(
        k in txt for k in ["ast_per_game", "ast_pct", "potential_ast", "secondary_ast", "assist_points_created"]
    ):
        return "playmaking"

    if any(k in uq for k in ["score", "scorer", "points", "bucket", "leading scorer"]) or any(
        k in txt for k in ["ppg", "ts_pct", "usage_pct", "fga", "fta", "ft_rate"]
    ):
        return "scoring"

    return "scoring"


def _is_simple_top_scorers_question(question: str, domain: str) -> bool:
    q = (question or "").lower()
    asks_for_top_list = any(k in q for k in ["top ", "best ", "leading ", "leaders", "leaderboard"])
    if not asks_for_top_list:
        return False

    # Avoid hijacking single-player follow-ups like "what is his best skill".
    if any(k in q for k in ["best skill", "his best", "her best", "their best", "break down", "profile"]):
        return False

    if domain == "overall_impact":
        return False

    leaderboard_entity_markers = [
        "scorer",
        "scoring",
        "points",
        "rebound",
        "boards",
        "assist",
        "steal",
        "block",
        "shoot",
        "3pt",
        "3-point",
        "leaders",
        "leaderboard",
    ]
    has_entity = any(k in q for k in leaderboard_entity_markers)
    has_top_number = re.search(r"\b(top|best|leading)\s+\d{1,2}\b", q) is not None
    if not (has_entity or has_top_number):
        return False

    complex_markers = [
        "compare",
        "versus",
        " vs ",
        "between",
        "why",
        "how",
        "predict",
        "projection",
        "trend",
        "streak",
        "split",
    ]
    return not any(marker in q for marker in complex_markers)


def _is_team_or_standings_dataframe(df: pd.DataFrame) -> bool:
    if df is None or df.empty:
        return False
    team_cols = {"TEAM_NAME", "TeamName", "TeamCity", "DiffPointsPG", "PointsPG", "OppPointsPG"}
    return any(col in df.columns for col in team_cols)


def _build_simple_top_scorers_prompts(question: str, rows_to_show: pd.DataFrame) -> tuple[str, str]:
    system_prompt = (
        "You are an NBA stats assistant. For simple leaderboard questions, keep the response short and plain.\n"
        "Output format must be exactly:\n"
        "1) One short lead sentence in plain English, like: "
        "\"[Player] led the league at [PPG] PPG on [FG%] FG.\"\n"
        "2) A compact list with exactly the number of rows requested by the user, each line in this exact style: "
        "Name, TEAM | X ppg | Y% fg | Z% 3p | N games\n"
        "3) One final follow-up line asking if the user wants deeper scoring breakdowns, such as: "
        "\"Want me to break down each player's scoring profile further?\"\n\n"
        "Rules:\n"
        "- Keep total response concise.\n"
        "- No long paragraphs, no extra sections, no generic commentary.\n"
        "- Use only numbers from the provided data.\n"
        "- The lead sentence must always include player name, PPG, and FG% (use N/A if FG% is missing).\n"
        "- If a field is unavailable, show N/A.\n"
        "- Do not output unlabeled number-only lines.\n"
        "- Use lowercase stat labels exactly: ppg, fg, 3p, games.\n"
        "- Preserve ranking order from highest to lowest scorer.\n"
    )
    user_prompt = (
        f"Question: {question}\n\n"
        "Use this data only:\n"
        f"{rows_to_show.head(10).to_string(index=False)}\n\n"
        "Return only the requested short format."
    )
    return system_prompt, user_prompt


def _extract_requested_top_n(question: str, default_n: int = 5, max_n: int = 50) -> int:
    q = (question or "").lower()
    match = re.search(r"\b(top|best|leading|worst|bottom|lowest)\s+(\d{1,3})\b", q)
    if not match:
        return default_n
    try:
        n = int(match.group(2))
    except Exception:
        return default_n
    if n < 1:
        return default_n
    return min(n, max_n)


def _extract_season_reference_text(question: str) -> Optional[str]:
    q = (question or "").lower()
    is_playoffs = "playoff" in q or "postseason" in q

    range_match = re.search(r"\b(19\d{2}|20\d{2})\s*[-/]\s*(\d{2,4})\b", q)
    if range_match:
        start = int(range_match.group(1))
        end_raw = range_match.group(2)
        if len(end_raw) == 2:
            end = int(f"{str(start)[:2]}{end_raw}")
        else:
            end = int(end_raw)
        if is_playoffs:
            return f"{start}-{str(end)[-2:]} playoffs"
        return f"{start}-{str(end)[-2:]} season"

    year_match = re.search(r"\b(19\d{2}|20\d{2})\b", q)
    if year_match:
        year = int(year_match.group(1))
        if is_playoffs:
            start = year - 1
            end = year
            return f"{start}-{str(end)[-2:]} playoffs"
        start = year
        end = year + 1
        return f"{start}-{str(end)[-2:]} season"

    if "this season" in q or "current season" in q:
        return "2024-25 season"
    if "this playoff" in q or "current playoff" in q:
        return "2024-25 playoffs"
    if "last season" in q:
        return "2024-25 season"
    if "last playoff" in q:
        return "2024-25 playoffs"

    return None


def _is_single_player_stats_question(question: str, df: pd.DataFrame) -> bool:
    q = (question or "").lower()
    if df is None or df.empty:
        return False
    if "player_name" not in df.columns:
        return False
    if len(df) != 1:
        return False
    if "game_date" in df.columns:
        return False
    if any(k in q for k in ["top ", "best ", "leading ", "leaderboard", "rank leaders", "compare", "versus", " vs ", "between"]):
        return False
    has_career_phrase = re.search(r"\bcaree+r\b", q) is not None
    has_decade_phrase = re.search(r"\b(19\d{2}|20\d{2})s\b", q) is not None or "decade" in q
    has_from_to_phrase = re.search(r"\bfrom\b.+\bto\b", q) is not None
    if any(
        k in q
        for k in [
            "by season",
            "per season",
            "each season",
            "season by season",
            "over the years",
            "through the years",
            "across seasons",
            "year by year",
            "over his career",
            "over her career",
            "over their career",
            "throughout his career",
            "throughout her career",
            "throughout their career",
            "trend",
            "over time",
        ]
    ) or has_career_phrase or has_decade_phrase or has_from_to_phrase or ("rookie year" in q):
        return False
    if not any(k in q for k in [" stats", "stat ", "stat?", "what were", "show", "profile"]):
        return False
    try:
        unique_players = df["player_name"].dropna().astype(str).str.strip().nunique()
    except Exception:
        unique_players = 0
    return unique_players == 1


def _is_single_player_season_trend_question(question: str, df: pd.DataFrame) -> bool:
    q = (question or "").lower()
    if df is None or df.empty or "player_name" not in df.columns:
        return False
    if len(df) < 1:
        return False
    has_career_phrase = re.search(r"\bcaree+r\b", q) is not None
    has_decade_phrase = re.search(r"\b(19\d{2}|20\d{2})s\b", q) is not None or "decade" in q
    has_from_to_phrase = re.search(r"\bfrom\b.+\bto\b", q) is not None
    has_over_time_phrase = any(
        k in q
        for k in [
            "by season",
            "per season",
            "each season",
            "season by season",
            "over the years",
            "through the years",
            "across seasons",
            "year by year",
            "over his career",
            "over her career",
            "over their career",
            "throughout his career",
            "throughout her career",
            "throughout their career",
            "trend",
            "over time",
        ]
    ) or has_career_phrase or has_decade_phrase or has_from_to_phrase or ("rookie year" in q)
    has_year_range_phrase = re.search(r"\b(19\d{2}|20\d{2})\s*(to|through|thru|-)\s*(19\d{2}|20\d{2})\b", q) is not None
    has_peak_season_phrase = re.search(r"\b(highest|best|most|peak)\b.+\bseason\b", q) is not None
    if not (has_over_time_phrase or has_year_range_phrase or has_peak_season_phrase):
        return False
    if not any(c in df.columns for c in ["season_start", "season_label", "season", "season_year", "season_id"]):
        return False
    try:
        unique_players = df["player_name"].dropna().astype(str).str.strip().nunique()
    except Exception:
        unique_players = 0
    return unique_players <= 2


def _format_single_player_season_trend_response(df: pd.DataFrame, question: str, client: Optional[Any]) -> str:
    q = (question or "").lower()
    working = df.copy()
    player_name = str(working.iloc[0].get("player_name", "N/A")) if not working.empty else "N/A"
    try:
        unique_players = working["player_name"].dropna().astype(str).str.strip().unique().tolist()
    except Exception:
        unique_players = [player_name] if player_name != "N/A" else []
    is_multi_player = len(unique_players) > 1

    season_col = None
    for cand in ["season_label", "season", "season_year", "season_start", "season_id"]:
        if cand in working.columns:
            season_col = cand
            break

    if season_col is None:
        return ""

    if "season_start" in working.columns:
        try:
            working = working.sort_values(by=["season_start"], ascending=[True], na_position="last")
        except Exception:
            pass
    elif season_col in working.columns:
        try:
            working = working.sort_values(by=[season_col], ascending=[True], na_position="last")
        except Exception:
            pass

    def _fmt_num(v: Any, digits: int = 1) -> str:
        try:
            if pd.isna(v):
                return "N/A"
            return f"{float(v):.{digits}f}"
        except Exception:
            return "N/A"

    def _fmt_pct(v: Any) -> str:
        try:
            if pd.isna(v):
                return "N/A"
            num = float(v)
            if 0 <= num <= 1:
                num *= 100.0
            return f"{num:.1f}%"
        except Exception:
            return "N/A"

    # Remove duplicate rows for the same season/player if source tables contain repeats.
    dedupe_keys = [k for k in ["player_name", "season_start", "season_label"] if k in working.columns]
    if dedupe_keys:
        working = working.drop_duplicates(subset=dedupe_keys, keep="first")

    is_shooting = any(k in q for k in ["shoot", "fg%", "3 point", "3p", "percentage", "three point", "free throw", "ft%"])
    is_rebounding = any(k in q for k in ["rebound", "boards", "glass", "oreb", "dreb"])
    is_defense = any(k in q for k in ["block", "blk", "steal", "stl", "defense", "defensive", "defend", "deflect", "contest"])

    if is_shooting:
        columns = [("FG%", "fg_pct"), ("3P%", "fg3_pct"), ("FT%", "ft_pct"), ("FGM", "fgm"), ("FGA", "fga"), ("3PM", "fg3m"), ("3PA", "fg3a")]
    elif is_rebounding:
        columns = [("REB", "reb"), ("OREB", "oreb"), ("DREB", "dreb"), ("REB Rank", "reb_rank"), ("OREB Rank", "oreb_rank"), ("DREB Rank", "dreb_rank")]
    elif is_defense:
        columns = [("STL", "stl"), ("BLK", "blk"), ("DREB", "dreb"), ("PF", "pf"), ("Games", "gp")]
    else:
        columns = [("PTS", "pts"), ("REB", "reb"), ("AST", "ast"), ("FG%", "fg_pct"), ("3P%", "fg3_pct")]

    available = [(label, col) for label, col in columns if col in working.columns]
    if not available:
        return ""

    if is_multi_player:
        header = "| Player | Season | " + " | ".join(label for label, _ in available) + " |"
        divider = "|---|---|" + "|".join(["---:" for _ in available]) + "|"
        lines = ["## **Season-by-Season Results**", "", header, divider]
    else:
        header = "| Season | " + " | ".join(label for label, _ in available) + " |"
        divider = "|---|" + "|".join(["---:" for _ in available]) + "|"
        lines = [f"## **{player_name}**", "", header, divider]

    for _, row in working.iterrows():
        season_text = str(row.get(season_col, "N/A"))
        vals = []
        for label, col in available:
            if "%" in label:
                vals.append(_fmt_pct(row.get(col)))
            else:
                vals.append(_fmt_num(row.get(col)))
        if is_multi_player:
            row_player = str(row.get("player_name", "N/A"))
            lines.append("| " + row_player + " | " + season_text + " | " + " | ".join(vals) + " |")
        else:
            lines.append("| " + season_text + " | " + " | ".join(vals) + " |")

    if client is not None:
        try:
            # Send ALL rows (not just 8) so the summary covers the full span.
            trend_data = working[[season_col] + [c for _, c in available]].to_dict(orient="records")
            num_seasons = len(trend_data)

            trend_system = (
                "You are an NBA analyst writing a trend summary below a stats table.\n"
                "The user can already SEE the table — do NOT repeat every number from it.\n"
                "Instead, tell the STORY the numbers reveal:\n"
                "- Identify the peak season(s) and the low point(s) with specific numbers.\n"
                "- Describe the overall trajectory (improving, declining, consistent, U-shaped, etc.).\n"
                "- If there's a notable jump or drop between consecutive seasons, call it out and "
                "speculate briefly on context (injury, team change, role shift) if obvious.\n"
                "- For multi-player data, compare their trajectories — who improved more, "
                "who was more consistent, who peaked higher.\n"
                "Write 3-5 sentences in a natural, engaging sports-analyst tone. "
                "Reference concrete numbers but don't just list them — weave them into insight."
            )
            trend_user = (
                f"Question: {question}\n"
                f"Player(s): {unique_players or [player_name]}\n"
                f"Seasons covered: {num_seasons}\n"
                f"Data: {trend_data}"
            )

            resp = client.chat.completions.create(
                model="gpt-5.4-mini",
                messages=[
                    {"role": "system", "content": trend_system},
                    {"role": "user", "content": trend_user},
                ],
                temperature=0.3,
                max_completion_tokens=300,
            )
            summary = (resp.choices[0].message.content or "").strip()
        except Exception:
            summary = ""
    else:
        summary = ""

    wants_average = any(k in q for k in ["average", "averages", "avg"])
    if wants_average:
        avg_metrics = []
        for label, col in available:
            if col == "gp" or "rank" in col:
                continue
            try:
                numeric = pd.to_numeric(working[col], errors="coerce")
                mean_val = float(numeric.mean())
                if pd.isna(mean_val):
                    continue
                if "%" in label:
                    avg_metrics.append((f"Avg {label}", f"{mean_val*100.0:.1f}%" if mean_val <= 1 else f"{mean_val:.1f}%"))
                else:
                    avg_metrics.append((f"Avg {label}", f"{mean_val:.2f}"))
            except Exception:
                continue
        if avg_metrics:
            lines.append("")
            lines.append("| Metric | Value |")
            lines.append("|---|---:|")
            for m, v in avg_metrics[:8]:
                lines.append(f"| {m} | {v} |")

    asks_peak = any(k in q for k in ["highest", "best", "most", "peak"])
    if asks_peak and not working.empty:
        metric_candidates = ["pts", "ast", "reb", "blk", "stl", "fg3m", "fg_pct", "fg3_pct", "ft_pct"]
        chosen = next((c for c in metric_candidates if c in working.columns), None)
        if chosen is not None:
            numeric = pd.to_numeric(working[chosen], errors="coerce")
            if numeric.notna().any():
                max_idx = int(numeric.idxmax())
                max_row = working.loc[max_idx]
                peak_season = str(max_row.get(season_col, "N/A"))
                peak_val = _fmt_pct(max_row.get(chosen)) if "pct" in chosen else _fmt_num(max_row.get(chosen))
                metric_label = chosen.upper().replace("_PCT", "%").replace("FG3M", "3PM")
                summary = f"His peak {metric_label} season in this span was {peak_season} at {peak_val}."

    if not summary:
        # Auto-generate a basic arc description from the data itself.
        if is_multi_player:
            summary = "The table above shows each player's season-by-season production across the requested span."
        else:
            # Try to auto-detect peak and trajectory from the primary stat column.
            primary_col = available[0][1] if available else None
            if primary_col and primary_col in working.columns:
                try:
                    numeric = pd.to_numeric(working[primary_col], errors="coerce")
                    if numeric.notna().any():
                        peak_idx = int(numeric.idxmax())
                        low_idx = int(numeric.idxmin())
                        peak_row = working.loc[peak_idx]
                        low_row = working.loc[low_idx]
                        peak_season = str(peak_row.get(season_col, "N/A"))
                        low_season = str(low_row.get(season_col, "N/A"))
                        peak_val = _fmt_pct(peak_row.get(primary_col)) if "pct" in primary_col else _fmt_num(peak_row.get(primary_col))
                        low_val = _fmt_pct(low_row.get(primary_col)) if "pct" in primary_col else _fmt_num(low_row.get(primary_col))
                        stat_label = available[0][0]
                        summary = (
                            f"Over this span, his {stat_label} peaked at {peak_val} in {peak_season} "
                            f"and hit a low of {low_val} in {low_season}."
                        )
                    else:
                        summary = "The table above shows the season-by-season trend across the requested span."
                except Exception:
                    summary = "The table above shows the season-by-season trend across the requested span."
            else:
                summary = "The table above shows the season-by-season trend across the requested span."

    lines.extend(["", summary, "", "Want me to break down any specific season further?"])
    return "\n" + "\n".join(lines)


def _generate_natural_player_season_summary(
    row: pd.Series, question: str, season_text: Optional[str], client: Optional[Any]
) -> Optional[str]:
    if client is None:
        return None

    def _safe(v: Any) -> str:
        try:
            if pd.isna(v):
                return "N/A"
        except Exception:
            if v is None:
                return "N/A"
        return str(v)

    payload = {
        "player_name": _safe(row.get("player_name")),
        "team_abbreviation": _safe(row.get("team_abbreviation")),
        "season_text": season_text or "requested season",
        "gp": _safe(row.get("gp")),
        "w_pct": _safe(row.get("w_pct")),
        "min": _safe(row.get("min")),
        "pts": _safe(row.get("pts")),
        "pts_rank": _safe(row.get("pts_rank")),
        "fg_pct": _safe(row.get("fg_pct")),
        "fg3_pct": _safe(row.get("fg3_pct")),
        "ft_pct": _safe(row.get("ft_pct")),
        "fga": _safe(row.get("fga")),
        "fga_rank": _safe(row.get("fga_rank")),
        "fg3m": _safe(row.get("fg3m")),
        "fg3a": _safe(row.get("fg3a")),
        "fg3a_rank": _safe(row.get("fg3a_rank")),
        "ftm": _safe(row.get("ftm")),
        "fta": _safe(row.get("fta")),
        "reb": _safe(row.get("reb")),
        "oreb": _safe(row.get("oreb")),
        "dreb": _safe(row.get("dreb")),
        "reb_rank": _safe(row.get("reb_rank")),
        "ast": _safe(row.get("ast")),
        "ast_rank": _safe(row.get("ast_rank")),
        "tov": _safe(row.get("tov")),
        "stl": _safe(row.get("stl")),
        "stl_rank": _safe(row.get("stl_rank")),
        "blk": _safe(row.get("blk")),
        "blk_rank": _safe(row.get("blk_rank")),
        "pf": _safe(row.get("pf")),
        "plus_minus": _safe(row.get("plus_minus")),
        "dd2": _safe(row.get("dd2")),
        "td3": _safe(row.get("td3")),
    }

    system_prompt = (
        "You are an NBA analyst writing an in-depth season review.\n\n"
        "Structure your response as a natural, flowing analysis — NOT a stat dump.\n"
        "Cover these angles in 4-6 sentences:\n"
        "1. SCORING & EFFICIENCY: points, shooting splits (FG%, 3P%, FT%), volume (FGA), "
        "and what they reveal about the player's role (primary scorer, secondary, etc.).\n"
        "2. PLAYMAKING & BALL SECURITY: assists, turnovers, assist-to-turnover feel.\n"
        "3. REBOUNDING & DEFENSE: rebounds (offensive vs defensive if available), "
        "steals, blocks — note if these are elite, average, or a weakness.\n"
        "4. IMPACT: plus-minus, win percentage, games played. Flag injury-shortened "
        "seasons (under ~60 games) or heavy workloads (over 36 min).\n\n"
        "Use the rank columns (pts_rank, reb_rank, etc.) to contextualize where the player "
        "stood league-wide — e.g. 'ranking 6th in scoring' is more meaningful than just '26.4 PPG'.\n"
        "Mention double-doubles (dd2) or triple-doubles (td3) if they're notable (5+).\n"
        "Tone: knowledgeable, neutral, direct. Not robotic, not overhyped.\n"
        "Return plain text only — no headers, no bullet points, no markdown."
    )
    user_prompt = (
        f"Question: {question}\n"
        f"Season profile data: {payload}\n"
        "Write the season analysis now."
    )

    try:
        resp = client.chat.completions.create(
            model="gpt-5.4-mini",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.3,
            max_completion_tokens=400,
        )
        txt = (resp.choices[0].message.content or "").strip()
        return txt if txt else None
    except Exception:
        return None


def _format_single_player_stats_profile(df: pd.DataFrame, question: str, client: Optional[Any]) -> str:
    def _is_missing(v: Any) -> bool:
        try:
            return pd.isna(v)
        except Exception:
            return v is None

    def _fmt_num(v: Any, digits: int = 1) -> str:
        if _is_missing(v):
            return "N/A"
        try:
            return f"{float(v):.{digits}f}"
        except Exception:
            return "N/A"

    def _fmt_int(v: Any) -> str:
        if _is_missing(v):
            return "N/A"
        try:
            return str(int(float(v)))
        except Exception:
            return "N/A"

    def _fmt_pct(v: Any, digits: int = 1) -> str:
        if _is_missing(v):
            return "N/A"
        try:
            num = float(v)
            if 0 <= num <= 1:
                num *= 100.0
            return f"{num:.{digits}f}%"
        except Exception:
            return "N/A"

    def _v(row: pd.Series, key: str, default: str = "N/A") -> Any:
        return row.get(key, default)

    working = df.copy()
    if "player_name" in working.columns:
        working = working.dropna(subset=["player_name"])
        if "gp" in working.columns:
            working = working.sort_values(by=["gp"], ascending=[False], na_position="last")
        working = working.drop_duplicates(subset=["player_name"], keep="first")
    if working.empty:
        return "\nNo player stats were available for this question."

    row = working.iloc[0]
    name = str(_v(row, "player_name")) if not _is_missing(_v(row, "player_name")) else "N/A"
    team = str(_v(row, "team_abbreviation")) if not _is_missing(_v(row, "team_abbreviation")) else "N/A"
    age = _fmt_num(_v(row, "age"), digits=1)
    gp = _fmt_int(_v(row, "gp"))
    w_pct = _fmt_pct(_v(row, "w_pct"), digits=1)
    mins = _fmt_num(_v(row, "min"), digits=1)
    season_text = None
    if "season_label" in working.columns and not _is_missing(_v(row, "season_label")):
        season_text = str(_v(row, "season_label")) + " season"
    elif "season_start" in working.columns and not _is_missing(_v(row, "season_start")):
        try:
            start = int(float(_v(row, "season_start")))
            season_text = f"{start}-{str(start + 1)[-2:]} season"
        except Exception:
            season_text = None
    if not season_text:
        season_text = _extract_season_reference_text(question)

    heading = f"## **{name}** ({team})"
    if season_text:
        heading = f"## **{name}** ({team}, {season_text})"

    ranked_stat_candidates = [
        ("fgm_rank", "fgm", "FGM"),
        ("fg3m_rank", "fg3m", "3PM"),
        ("fg3a_rank", "fg3a", "3PA"),
        ("fg_pct_rank", "fg_pct", "FG%"),
        ("fg3_pct_rank", "fg3_pct", "3P%"),
        ("ftm_rank", "ftm", "FTM"),
        ("fta_rank", "fta", "FTA"),
        ("ft_pct_rank", "ft_pct", "FT%"),
        ("stl_rank", "stl", "STL"),
        ("blk_rank", "blk", "BLK"),
        ("oreb_rank", "oreb", "OREB"),
        ("dreb_rank", "dreb", "DREB"),
        ("dd2_rank", "dd2", "DD2"),
        ("td3_rank", "td3", "TD3"),
        ("min_rank", "min", "MIN"),
    ]

    best_extra: Optional[tuple[str, str, str, int]] = None
    for rank_col, value_col, label in ranked_stat_candidates:
        rank_val_raw = _v(row, rank_col)
        if _is_missing(rank_val_raw):
            continue
        try:
            rank_val = int(float(rank_val_raw))
        except Exception:
            continue
        if rank_val <= 0:
            continue
        if best_extra is None or rank_val < best_extra[3]:
            best_extra = (label, value_col, rank_col, rank_val)

    extra_label = "Best Extra (Rank)"
    extra_value = "N/A"
    if best_extra is not None:
        label, value_col, rank_col, rank_val = best_extra
        raw_value = _v(row, value_col)
        if label.endswith("%"):
            value_text = _fmt_pct(raw_value)
        else:
            value_text = _fmt_num(raw_value)
        extra_label = f"{label} (Rank)"
        extra_value = f"{value_text} (#{rank_val})"

    pts_rank = _fmt_int(_v(row, "pts_rank"))
    fg_text = _fmt_pct(_v(row, "fg_pct"))
    fga_rank = _fmt_int(_v(row, "fga_rank"))
    fg3a_rank = _fmt_int(_v(row, "fg3a_rank"))
    fg3_text = _fmt_pct(_v(row, "fg3_pct"))
    reb_rank = _fmt_int(_v(row, "reb_rank"))
    ast_rank = _fmt_int(_v(row, "ast_rank"))
    stl_rank = _fmt_int(_v(row, "stl_rank"))
    blk_rank = _fmt_int(_v(row, "blk_rank"))
    plus_minus_text = _fmt_num(_v(row, "plus_minus"))

    def _rank_to_int(rank_text: str) -> Optional[int]:
        if rank_text == "N/A":
            return None
        try:
            return int(rank_text)
        except Exception:
            return None

    stl_rank_i = _rank_to_int(stl_rank)
    blk_rank_i = _rank_to_int(blk_rank)
    reb_rank_i = _rank_to_int(reb_rank)
    ast_rank_i = _rank_to_int(ast_rank)
    fga_rank_i = _rank_to_int(fga_rank)
    fg3a_rank_i = _rank_to_int(fg3a_rank)
    gp_i = _rank_to_int(gp)

    def _top_or_rank(rank_i: Optional[int]) -> str:
        if rank_i is None:
            return "among league scorers"
        if rank_i <= 10:
            return f"top {rank_i} in the league"
        return f"ranked {rank_i}th in the league"

    season_summary = _generate_natural_player_season_summary(row, question, season_text, client)
    if not season_summary:
        summary_sentences = []
        summary_sentences.append(
            f"{name} averaged {_fmt_num(_v(row, 'pts'))} points per game, {_top_or_rank(_rank_to_int(pts_rank))}, while shooting {fg_text} from the field."
        )
        if fga_rank_i is not None and fg3a_rank_i is not None:
            summary_sentences.append(
                f"He did that on {_fmt_num(_v(row, 'fga'))} field-goal attempts per game (#{fga_rank}) and {_fmt_num(_v(row, 'fg3a'))} three-point attempts per game (#{fg3a_rank}), which adds context to his {fg_text} FG and {fg3_text} 3P efficiency."
            )
        if gp_i is not None and gp_i < 50:
            summary_sentences.append(
                f"He played {gp} games, so this season line comes from a relatively small sample."
            )
        season_summary = " ".join(summary_sentences)

    lines = [
        heading,
        "",
        f"_Age: {age} | Minutes: {mins} | W%: {w_pct} | Games Played: {gp}_",
        "",
        f"| PPG | REB | AST | Plus/Minus | {extra_label} |",
        "|---:|---:|---:|---:|---:|",
        f"| {_fmt_num(_v(row, 'pts'))} | {_fmt_num(_v(row, 'reb'))} | {_fmt_num(_v(row, 'ast'))} | {_fmt_num(_v(row, 'plus_minus'))} | {extra_value} |",
        "",
        "### Scoring",
        "| PTS | FG% | 3P% | FT% | PTS Rank | FG% Rank | 3P% Rank | FT% Rank |",
        "|---:|---:|---:|---:|---:|---:|---:|---:|",
        f"| {_fmt_num(_v(row, 'pts'))} | {_fmt_pct(_v(row, 'fg_pct'))} | {_fmt_pct(_v(row, 'fg3_pct'))} | {_fmt_pct(_v(row, 'ft_pct'))} | {_fmt_int(_v(row, 'pts_rank'))} | {_fmt_int(_v(row, 'fg_pct_rank'))} | {_fmt_int(_v(row, 'fg3_pct_rank'))} | {_fmt_int(_v(row, 'ft_pct_rank'))} |",
        "",
        "### Rebounding",
        "| REB | DREB | OREB | REB Rank | DREB Rank | OREB Rank |",
        "|---:|---:|---:|---:|---:|---:|",
        f"| {_fmt_num(_v(row, 'reb'))} | {_fmt_num(_v(row, 'dreb'))} | {_fmt_num(_v(row, 'oreb'))} | {_fmt_int(_v(row, 'reb_rank'))} | {_fmt_int(_v(row, 'dreb_rank'))} | {_fmt_int(_v(row, 'oreb_rank'))} |",
        "",
        "### Assists & Ball Security",
        "| AST | TOV | AST Rank | TOV Rank |",
        "|---:|---:|---:|---:|",
        f"| {_fmt_num(_v(row, 'ast'))} | {_fmt_num(_v(row, 'tov'))} | {_fmt_int(_v(row, 'ast_rank'))} | {_fmt_int(_v(row, 'tov_rank'))} |",
        "",
        "### Defense",
        "| STL | BLK | PF | STL Rank | BLK Rank | PF Rank |",
        "|---:|---:|---:|---:|---:|---:|",
        f"| {_fmt_num(_v(row, 'stl'))} | {_fmt_num(_v(row, 'blk'))} | {_fmt_num(_v(row, 'pf'))} | {_fmt_int(_v(row, 'stl_rank'))} | {_fmt_int(_v(row, 'blk_rank'))} | {_fmt_int(_v(row, 'pf_rank'))} |",
        "",
        "### Double-Double / Triple-Double",
        "| DD2 | TD3 | DD2 Rank | TD3 Rank |",
        "|---:|---:|---:|---:|",
        f"| {_fmt_num(_v(row, 'dd2'))} | {_fmt_num(_v(row, 'td3'))} | {_fmt_int(_v(row, 'dd2_rank'))} | {_fmt_int(_v(row, 'td3_rank'))} |",
        "",
        season_summary,
        "",
        "Want me to break down his season profile further?",
    ]
    return "\n" + "\n".join(lines)


def _is_games_played_question(question: str, df: pd.DataFrame) -> bool:
    # Strip any injected follow-up context like "(continuing comparison with ...)"
    # so that phrases from prior assistant responses don't false-positive.
    raw_q = re.sub(r"\(continuing comparison with[^)]*\)", "", question or "")
    q = raw_q.lower().strip()
    if "gp" not in df.columns:
        return False
    # Never fire for multi-player data — it's a comparison, not a games-played lookup.
    if "player_name" in df.columns and df["player_name"].dropna().nunique() > 1:
        return False
    return any(
        phrase in q
        for phrase in [
            "how many games",
            "games did he play",
            "games did she play",
            "games played",
            "did he play that year",
            "did she play that year",
        ]
    )


def _is_concise_single_player_season_lookup_question(question: str, df: pd.DataFrame) -> bool:
    raw_q = re.sub(r"\(continuing comparison with[^)]*\)", "", question or "")
    q = raw_q.lower().strip()
    asks_year_or_season = any(k in q for k in ["what year", "which year", "what season", "which season"])
    asks_ordinal = re.search(
        r"\b(\d{1,2}(st|nd|rd|th)|first|second|third|fourth|fifth|sixth|seventh|eighth|ninth|tenth)\s+(season|year)\b",
        q,
    ) is not None
    is_comparison = any(k in q for k in ["compare", "versus", " vs ", "better than", "between"])
    unique_players = df["player_name"].dropna().nunique() if "player_name" in df.columns else 0
    return (asks_year_or_season or asks_ordinal) and not is_comparison and unique_players <= 1 and len(df) <= 3


def _format_games_played_response(df: pd.DataFrame, question: str) -> str:
    row = df.iloc[0]
    player = str(row.get("player_name", "He")) if "player_name" in df.columns else "He"
    gp_raw = row.get("gp", None)
    season_text = None
    if "season_label" in df.columns and not pd.isna(row.get("season_label")):
        season_text = f"{row.get('season_label')} season"
    elif "season_start" in df.columns and not pd.isna(row.get("season_start")):
        try:
            start = int(float(row.get("season_start")))
            season_text = f"{start}-{str(start + 1)[-2:]} season"
        except Exception:
            season_text = None
    if season_text is None:
        season_text = _extract_season_reference_text(question)

    try:
        gp_text = str(int(float(gp_raw))) if gp_raw is not None and not pd.isna(gp_raw) else "N/A"
    except Exception:
        gp_text = "N/A"

    if season_text:
        return f"\n{player} played **{gp_text}** games in the **{season_text}**."
    return f"\n{player} played **{gp_text}** games."


def _format_concise_single_player_season_lookup_response(df: pd.DataFrame, question: str) -> str:
    row = df.iloc[0]
    player = str(row.get("player_name", "Player")) if "player_name" in df.columns else "Player"
    season_text = None
    if "season_label" in df.columns and not pd.isna(row.get("season_label")):
        season_text = f"{row.get('season_label')} season"
    elif "season_start" in df.columns and not pd.isna(row.get("season_start")):
        try:
            start = int(float(row.get("season_start")))
            season_text = f"{start}-{str(start + 1)[-2:]} season"
        except Exception:
            season_text = None
    if season_text is None:
        season_text = _extract_season_reference_text(question) or "requested season"

    def _num(v: Any, digits: int = 1) -> str:
        if v is None or pd.isna(v):
            return "N/A"
        try:
            return f"{float(v):.{digits}f}"
        except Exception:
            return "N/A"

    def _pct(v: Any) -> str:
        if v is None or pd.isna(v):
            return "N/A"
        try:
            num = float(v)
            if 0 <= num <= 1:
                num *= 100
            return f"{num:.1f}%"
        except Exception:
            return "N/A"

    gp = _num(row.get("gp"), 0)
    pts = _num(row.get("pts"))
    reb = _num(row.get("reb"))
    ast = _num(row.get("ast"))
    fg = _pct(row.get("fg_pct"))

    def _rank_int(v: Any) -> Optional[int]:
        if v is None or pd.isna(v):
            return None
        try:
            r = int(float(v))
            return r if r > 0 else None
        except Exception:
            return None

    scorer_rank = _rank_int(row.get("pts_rank"))
    passer_rank = _rank_int(row.get("ast_rank"))
    rebound_rank = _rank_int(row.get("reb_rank"))

    strengths: List[str] = []
    if scorer_rank is not None and scorer_rank <= 15:
        strengths.append(f"a strong scorer (#{scorer_rank} in points)")
    if passer_rank is not None and passer_rank <= 20:
        strengths.append(f"a reliable playmaker (#{passer_rank} in assists)")
    if rebound_rank is not None and rebound_rank <= 20:
        strengths.append(f"an impactful rebounder (#{rebound_rank} in rebounds)")

    efficiency_line = ""
    fg_raw = row.get("fg_pct")
    try:
        fg_num = float(fg_raw) if fg_raw is not None and not pd.isna(fg_raw) else None
    except Exception:
        fg_num = None
    if fg_num is not None:
        if fg_num >= 0.50:
            efficiency_line = "He scored very efficiently for his volume."
        elif fg_num >= 0.47:
            efficiency_line = "He was an efficient scorer overall."

    if strengths:
        if len(strengths) == 1:
            profile_line = f"He was {strengths[0]} that season."
        else:
            profile_line = f"He was {', '.join(strengths[:-1])}, and {strengths[-1]} that season."
    else:
        profile_line = "He had a solid all-around season."

    detail_line = profile_line if not efficiency_line else f"{profile_line} {efficiency_line}"

    return (
        f"\n{player}'s requested season was **{season_text}**.\n"
        f"- Games played: **{gp}**\n"
        f"- Key stats: **{pts} PPG**, **{reb} REB**, **{ast} AST**, **{fg} FG**\n"
        f"{detail_line}\n\n"
        "Want me to break his season down further?"
    )


def _format_simple_top_scorers_response(df: pd.DataFrame, question: str) -> str:
    def _is_missing(v: Any) -> bool:
        try:
            return pd.isna(v)
        except Exception:
            return v is None

    def _fmt_num(v: Any, digits: int = 1) -> str:
        if _is_missing(v):
            return "N/A"
        try:
            return f"{float(v):.{digits}f}"
        except Exception:
            return "N/A"

    def _fmt_pct(v: Any) -> str:
        if _is_missing(v):
            return "N/A"
        try:
            num = float(v)
            if 0 <= num <= 1:
                num *= 100.0
            return f"{num:.1f}%"
        except Exception:
            return "N/A"

    def _fmt_games(v: Any) -> str:
        if _is_missing(v):
            return "N/A"
        try:
            return str(int(float(v)))
        except Exception:
            return "N/A"

    def _first_present(candidates: List[str], available_cols: set[str]) -> Optional[str]:
        for col in candidates:
            if col in available_cols:
                return col
        return None

    if df is None or df.empty:
        return "\nNo leaderboard data was available for this question."

    working = df.copy()
    cols = set(working.columns)
    q = (question or "").lower()
    requested_n = _extract_requested_top_n(question, default_n=5, max_n=50)

    metric_configs = [
        {
            "intent_terms": ["rebound", "boards", "glass", "rebounding"],
            "metric_candidates": ["reb", "trb_per_game", "trb_pct"],
            "rank_col": "reb_rank",
            "metric_label": "REB",
            "lead_phrase": "led the league on the glass",
            "table_header": "| # | Player | REB | OREB | DREB | Games |",
            "table_divider": "|---|---|---:|---:|---:|---:|",
            "row_builder": lambda idx, row, name, team: (
                f"| {idx} | **{name}** ({team}) | {_fmt_num(row.get('reb'))} reb | "
                f"{_fmt_num(row.get('oreb'))} oreb | {_fmt_num(row.get('dreb'))} dreb | {_fmt_games(row.get('gp'))} |"
            ),
        },
        {
            "intent_terms": ["assist", "playmaker", "passing"],
            "metric_candidates": ["ast", "ast_per_game", "ast_pct"],
            "rank_col": "ast_rank",
            "metric_label": "AST",
            "lead_phrase": "led the league in playmaking",
            "table_header": "| # | Player | AST | TOV | Games |",
            "table_divider": "|---|---|---:|---:|---:|",
            "row_builder": lambda idx, row, name, team: (
                f"| {idx} | **{name}** ({team}) | {_fmt_num(row.get('ast'))} ast | "
                f"{_fmt_num(row.get('tov'))} tov | {_fmt_games(row.get('gp'))} |"
            ),
        },
        {
            "intent_terms": ["steal", "steals"],
            "metric_candidates": ["stl", "stl_per_game"],
            "rank_col": "stl_rank",
            "metric_label": "STL",
            "lead_phrase": "led the league in steals",
            "table_header": "| # | Player | STL | BLK | Games |",
            "table_divider": "|---|---|---:|---:|---:|",
            "row_builder": lambda idx, row, name, team: (
                f"| {idx} | **{name}** ({team}) | {_fmt_num(row.get('stl'))} stl | "
                f"{_fmt_num(row.get('blk'))} blk | {_fmt_games(row.get('gp'))} |"
            ),
        },
        {
            "intent_terms": ["block", "blocks", "rim protection"],
            "metric_candidates": ["blk", "blk_per_game"],
            "rank_col": "blk_rank",
            "metric_label": "BLK",
            "lead_phrase": "led the league in blocks",
            "table_header": "| # | Player | BLK | STL | Games |",
            "table_divider": "|---|---|---:|---:|---:|",
            "row_builder": lambda idx, row, name, team: (
                f"| {idx} | **{name}** ({team}) | {_fmt_num(row.get('blk'))} blk | "
                f"{_fmt_num(row.get('stl'))} stl | {_fmt_games(row.get('gp'))} |"
            ),
        },
        {
            "intent_terms": ["three", "3pt", "3-point", "3pm", "threes"],
            "metric_candidates": ["fg3m", "three_pm", "fg3_pct"],
            "rank_col": "fg3m_rank",
            "metric_label": "3PM",
            "lead_phrase": "led the league from deep",
            "table_header": "| # | Player | 3PM | 3PA | 3P | Games |",
            "table_divider": "|---|---|---:|---:|---:|---:|",
            "row_builder": lambda idx, row, name, team: (
                f"| {idx} | **{name}** ({team}) | {_fmt_num(row.get('fg3m'))} 3pm | "
                f"{_fmt_num(row.get('fg3a'))} 3pa | {_fmt_pct(row.get('fg3_pct'))} 3p | {_fmt_games(row.get('gp'))} |"
            ),
        },
    ]

    selected = None
    for cfg in metric_configs:
        if any(term in q for term in cfg["intent_terms"]):
            selected = cfg
            break

    if selected is None:
        selected = {
            "metric_candidates": ["pts", "ppg", "ts_pct"],
            "rank_col": "pts_rank",
            "metric_label": "PPG",
            "lead_phrase": "led the league",
            "table_header": "| # | Player | PPG | FG | 3P | Games |",
            "table_divider": "|---|---|---:|---:|---:|---:|",
            "row_builder": lambda idx, row, name, team: (
                f"| {idx} | **{name}** ({team}) | {_fmt_num(row.get('pts'))} ppg | "
                f"{_fmt_pct(row.get('fg_pct'))} fg | {_fmt_pct(row.get('fg3_pct'))} 3p | {_fmt_games(row.get('gp'))} |"
            ),
        }

    rank_col = selected["rank_col"]
    metric_col = _first_present(selected["metric_candidates"], cols)
    if metric_col is None:
        metric_col = "pts" if "pts" in cols else ("reb" if "reb" in cols else "")

    if rank_col in cols and metric_col in cols:
        working = working.sort_values(by=[rank_col, metric_col], ascending=[True, False], na_position="last")
    elif rank_col in cols:
        working = working.sort_values(by=[rank_col], ascending=[True], na_position="last")
    elif metric_col in cols:
        working = working.sort_values(by=[metric_col], ascending=[False], na_position="last")

    before_dedup = working.copy()
    if "player_name" in cols:
        working = working.dropna(subset=["player_name"])
        working = working.drop_duplicates(subset=["player_name"], keep="first")
    else:
        working = working.drop_duplicates(keep="first")

    # Re-sort AFTER dedup to make sure the order is clean.
    if rank_col in cols and metric_col in cols:
        working = working.sort_values(by=[rank_col, metric_col], ascending=[True, False], na_position="last")
    elif rank_col in cols:
        working = working.sort_values(by=[rank_col], ascending=[True], na_position="last")
    elif metric_col in cols:
        working = working.sort_values(by=[metric_col], ascending=[False], na_position="last")

    # For "worst" questions, flip the sort so the worst performers come first.
    is_worst = any(k in q for k in ["worst", "bottom", "lowest"])
    if is_worst and metric_col in cols:
        working = working.sort_values(by=[metric_col], ascending=[True], na_position="last")

    # If the query already returned the requested number of rows but de-duping
    # collapsed it, do not silently show fewer rows. This can happen when a
    # season table contains multiple team rows for the same player.
    if len(working) < requested_n and len(before_dedup) >= requested_n:
        working = before_dedup

    top = working.head(requested_n)
    if top.empty:
        return "\nNo leaderboard data was available for this question."

    lead_row = top.iloc[0]

    def get_entity_name(row):
        for col in ["player_name", "TEAM_NAME", "TeamName", "team_abbreviation"]:
            if col in row and not pd.isna(row[col]):
                return str(row[col])
        return "N/A"

    lead_row = top.iloc[0]
    lead_name = get_entity_name(lead_row) # This now works perfectly
    
    lead_metric = _fmt_num(lead_row.get(metric_col))
    season_text = _extract_season_reference_text(question)
    if season_text:
        lead_line = (
            f"In {season_text}, {lead_name} {selected['lead_phrase']} at {lead_metric} {selected['metric_label']}."
        )
    else:
        lead_line = f"{lead_name} {selected['lead_phrase']} at {lead_metric} {selected['metric_label']}."

    lines = [lead_line, "", selected["table_header"], selected["table_divider"]]

    for idx, (_, row) in enumerate(top.iterrows(), start=1):
        # FIXED: Call your function here for every row
        name = get_entity_name(row) 
        team = str(row.get("team_abbreviation", "N/A")) if not _is_missing(row.get("team_abbreviation")) else "N/A"
        
        # Optional safeguard: prevents printing "GSW (GSW)" if the name is already the team abbreviation
        if name == team:
            team = "Team"
            
        lines.append(selected["row_builder"](idx, row, name, team))

    lines.append("")
    lines.append(f"Showing {len(top)} of {requested_n} requested rows.")
    lines.append("Want me to break down each player's profile further?")
    return "\n" + "\n".join(lines)


def _is_spatial_shot_dataframe(df: pd.DataFrame) -> bool:
    """court_shots-style rows with coordinates + make flag."""
    if df is None or df.empty:
        return False
    cols = {c.lower() for c in df.columns}
    return "loc_x" in cols and "loc_y" in cols and "shot_made_flag" in cols


def _build_spatial_shot_summary(df: pd.DataFrame) -> str:
    """Aggregate raw shot rows into zone / distance / side rates for the LLM."""
    parts: List[str] = []
    parts.append(
        "=== PRE-COMPUTED SUMMARY (use these rates; do NOT recite individual shot rows or sound like a table dump) ==="
    )
    d = df.copy()
    n = len(d)
    parts.append(f"Shots in this query sample: {n}")
    if n == 0:
        return "\n".join(parts)

    made_col = "shot_made_flag"
    if made_col not in d.columns:
        parts.append("(No shot_made_flag column.)")
        return "\n".join(parts)

    d["_mk"] = pd.to_numeric(d[made_col], errors="coerce").fillna(0).astype(int)
    makes = int(d["_mk"].sum())
    parts.append(f"Makes / attempts (sample): {makes} / {n}  ({(makes / max(n, 1)) * 100:.1f}% FG)")

    # By shot_type (2PT vs 3PT)
    st_col = next((c for c in d.columns if c.lower() == "shot_type"), None)
    if st_col:
        g = (
            d.groupby(st_col, dropna=False)["_mk"]
            .agg(["sum", "count"])
            .rename(columns={"sum": "makes", "count": "att"})
        )
        g["fg_pct"] = (g["makes"] / g["att"].replace(0, np.nan) * 100).round(1)
        parts.append("\nBy shot_type:")
        parts.append(g.to_string())

    # Distance buckets (feet)
    sd_col = next((c for c in d.columns if c.lower() == "shot_distance"), None)
    if sd_col:
        sd = pd.to_numeric(d[sd_col], errors="coerce")
        bins = [-0.1, 5, 16, 24, 500]
        labels = ["at_rim_to_5ft", "short_mid_5_to_16ft", "long_two_16_to_24ft", "three_24ft_plus"]
        bucket = pd.cut(sd, bins=bins, labels=labels)
        gg = d.assign(_bucket=bucket).groupby("_bucket", observed=False)["_mk"].agg(["sum", "count"])
        gg = gg.rename(columns={"sum": "makes", "count": "att"})
        gg["fg_pct"] = (gg["makes"] / gg["att"].replace(0, np.nan) * 100).round(1)
        parts.append("\nBy distance bucket (feet):")
        parts.append(gg.to_string())

    # Court side from loc_x (NBA stats.coordinate convention: negative/positive split)
    lx_col = next((c for c in d.columns if c.lower() == "loc_x"), None)
    if lx_col:
        lx = pd.to_numeric(d[lx_col], errors="coerce")
        side = np.where(lx < -40, "left_side", np.where(lx > 40, "right_side", "middle"))
        gg2 = d.assign(_side=side).groupby("_side", observed=False)["_mk"].agg(["sum", "count"])
        gg2 = gg2.rename(columns={"sum": "makes", "count": "att"})
        gg2["fg_pct"] = (gg2["makes"] / gg2["att"].replace(0, np.nan) * 100).round(1)
        parts.append("\nBy basket side (broad buckets from loc_x):")
        parts.append(gg2.to_string())

    zb = next((c for c in d.columns if c.lower() == "shot_zone_basic"), None)
    if zb:
        gz = d.groupby(zb, dropna=False)["_mk"].agg(["sum", "count"])
        gz = gz.rename(columns={"sum": "makes", "count": "att"})
        gz["fg_pct"] = (gz["makes"] / gz["att"].replace(0, np.nan) * 100).round(1)
        parts.append("\nBy NBA shot_zone_basic (if present):")
        parts.append(gz.to_string())

    parts.append(
        "\nNote: If the SQL query included a LIMIT, percentages reflect that sample only; otherwise treat as full query output."
    )
    return "\n".join(parts)


def _format_spatial_shots_narrative(df: pd.DataFrame, question: str, client: Any) -> str:
    """Free-form broadcast-style answer for shot-location questions — not rigid tables."""
    summary = _build_spatial_shot_summary(df)
    system_prompt = (
        "You are an NBA analyst on TV — conversational, sharp, and easy to listen to.\n"
        "The user asked about shooting spots / court locations / where shots come from.\n\n"
        "RULES:\n"
        "- Do NOT use stiff section headers like 'Executive Summary', 'Analysis', 'Conclusion', 'Key findings'.\n"
        "- Do NOT narrate the dataset row-by-row or paste coordinate pairs.\n"
        "- Use ONLY the PRE-COMPUTED SUMMARY for percentages — interpret what it means on the floor "
        "(paint vs jumper vs three, left vs right if relevant).\n"
        "- Write 2–4 short paragraphs OR a tight mix of prose + a few bullets — whichever fits naturally.\n"
        "- Sound human: one hook line, then texture. Light use of **bold** on a few numbers max.\n"
        "- If the sample is small or limited, say so casually in one clause — no alarmist disclaimers.\n"
        "- No bullet wall of every stat; prioritize the story of 'best spots' and 'weaker spots'.\n"
        "Return Markdown suitable for chat (no code blocks)."
    )
    user_prompt = f"Question:\n{question}\n\n{summary}\n\nAnswer in a relaxed, broadcast tone."
    try:
        resp = client.chat.completions.create(
            model="gpt-5.4-mini",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.75,
            max_completion_tokens=900,
        )
        raw = (resp.choices[0].message.content or "").strip()
        formatted = raw.replace("###", "\n\n###").replace("####", "\n\n####")
        return "\n" + formatted.strip()
    except Exception as e:
        return f"\nError generating shot-location narrative: {e}"


def analyze_question(question: str) -> str:
    """Run a real-time question from the CLI and analyze the resulting DataFrame."""
    try:
        df = run_query(question)
    except Exception as exc:
        return f"Error running query: {exc}"
        
    if df is None or getattr(df, 'empty', False):
        return "Error: The query returned an empty result set."
        
    return analyze_question_with_data(question, df)


def analyze_question_with_data(question: str, df: pd.DataFrame) -> str:
    """
    Analyze a pre-fetched DataFrame directly without re-running any query.
    This is called from main.py after run_query() has already succeeded,
    so we never run the query twice or trigger a false empty-result error.
    """


    if df is None or df.empty:
        return (
            "No data was found for this query. The player may not have participated "
            "in the requested season or playoffs, or the name was not recognized."
        )

    # Shot-chart / court coordinate data: narrative style, pre-aggregated — avoid rigid table dumps.
    if _is_spatial_shot_dataframe(df):
        return _format_spatial_shots_narrative(df, question, client)

    domain = infer_domain(question, df.columns.tolist())

    # Detect if data came from game logs (has game_date column)
    is_game_log = "game_date" in df.columns

    # Only run composite scoring for season summary data, not game logs
    score_table: Optional[pd.DataFrame] = None
    if ENABLE_COMPOSITE_SCORING and not is_game_log:
        try:
            cfg = DOMAIN_CONFIGS[domain]
            score_table = compute_scores(df, cfg)
        except Exception:
            score_table = None

    # Detect single-player drill-down tag set by main.py. When present, scope
    # the dataframe to ONLY that player and treat as a profile, not a comparison.
    drilldown_match = re.search(r"\(single-player drill-down:\s*([^)]+)\)", question or "")
    if drilldown_match and "player_name" in df.columns:
        target_player = drilldown_match.group(1).strip()
        target_lower = target_player.lower()
        # Match by substring (handles 'LeBron' matching 'LeBron James').
        mask = df["player_name"].dropna().astype(str).str.lower().str.contains(target_lower, regex=False)
        filtered = df[mask.reindex(df.index, fill_value=False)]
        if not filtered.empty:
            df = filtered
            print(f"[Analyzer] Drill-down tag found: scoped df to '{target_player}' "
                  f"({len(df)} rows from original {unique_player_count if 'unique_player_count' in dir() else '?'} players)")

    # Detect comparisons: explicit keywords OR multiple unique players in a small result set.
    comparison_keywords = any(
        k in question.lower() for k in ["compare", "better", "versus", "vs", "between", "who"]
    )
    try:
        unique_player_count = df["player_name"].dropna().astype(str).str.strip().nunique() if "player_name" in df.columns else 0
    except Exception:
        unique_player_count = 0

    team_entity_col = next(
        (c for c in ["TEAM_NAME", "TeamName", "team_name", "team_abbreviation"] if c in df.columns),
        None,
    )
    try:
        unique_team_count = df[team_entity_col].dropna().astype(str).str.strip().nunique() if team_entity_col else 0
    except Exception:
        unique_team_count = 0
    unique_entity_count = max(unique_player_count, unique_team_count)
    # Drill-down explicitly disables comparison mode regardless of df shape.
    is_drilldown = drilldown_match is not None
    is_comparison = (
        not is_drilldown
        and len(df) <= 10
        and (comparison_keywords or unique_entity_count >= 2)
        and unique_entity_count >= 2
    )
    comparison_entity_label = "TEAM" if unique_team_count >= 2 and unique_player_count == 0 else "PLAYER"
    comparison_entity_noun = "teams" if comparison_entity_label == "TEAM" else "players"
    comparison_entity_singular = "team" if comparison_entity_label == "TEAM" else "player"
    rows_to_show = df if is_comparison else df.head(20)

    # Apply user-friendly column renames before showing data to GPT or the user.
    display_df = _rename_columns_for_display(df)
    display_rows_to_show = display_df if is_comparison else display_df.head(20)

    df_summary = (
        f"DataFrame shape: {display_df.shape[0]} rows, {display_df.shape[1]} columns\n"
        f"Columns: {', '.join(display_df.columns.tolist())}\n\n"
        f"Data:\n{display_rows_to_show.to_string(index=False)}\n"
    )
    if len(display_df) > 20 and not is_comparison:
        df_summary += f"\nSummary statistics:\n{display_df.describe().to_string()}\n"

    # If the user asked for "PER" specifically, prepend a note for GPT so it
    # opens with a one-line acknowledgment that classic PER isn't stored.
    per_note = ""
    if _user_asked_for_per(question) and any(c in df.columns for c in ("PIE", "pie")):
        per_note = (
            "\n\nIMPORTANT: This database does not store classic PER (Hollinger). "
            "The 'PIE (PER equivalent)' column shown is the NBA's official equivalent. "
            "Open your answer with one short sentence acknowledging this so the user "
            "knows what they're seeing."
        )

    rubric_by_domain = {
        "defense": "Prioritize defensive_impact; rim protection (rim_fg_pct_allowed lower is better; rim_shots_contested higher is better); on-ball impact (opp_fg_pct_as_primary_defender lower is better); versatility; disruptions (deflections, loose balls).",
        "shooting": "Prioritize accuracy (three_pt_pct), then volume (three_pm, three_pa). Include role, shot quality, and sustainability commentary.",
        "playmaking": "Prioritize ast_per_game, ast_pct, potential_ast, assist_points_created; penalize turnovers (tov_per_game lower is better); reward efficiency (ast_to_tov). Consider on-ball workload.",
        "scoring": "Prioritize ppg and efficiency (ts_pct), then usage and volume (fga). Discuss shot mix and scalability.",
        "rebounding": "Prioritize trb_pct and trb_per_game, then oreb_pct and dreb_pct. Discuss contested rebounds and positioning.",
    }

    is_team_or_standings = _is_team_or_standings_dataframe(df)
    is_simple_top_scorers = (
        _is_simple_top_scorers_question(question, domain)
        and not is_team_or_standings
    )
    is_single_player_trend = _is_single_player_season_trend_question(question, df)
    is_single_player_stats = _is_single_player_stats_question(question, df)
    is_games_played_q = _is_games_played_question(question, df)
    is_concise_season_lookup = _is_concise_single_player_season_lookup_question(question, df)

    if is_games_played_q:
        return _format_games_played_response(df, question)
    if is_concise_season_lookup:
        return _format_concise_single_player_season_lookup_response(df, question)

    if is_single_player_trend:
        trend_text = _format_single_player_season_trend_response(df, question, client)
        if trend_text:
            return trend_text
    if is_single_player_stats:
        return _format_single_player_stats_profile(df, question, client)
    elif is_simple_top_scorers:
        return _format_simple_top_scorers_response(df, question)
    elif ENABLE_COMPOSITE_SCORING and score_table is not None and not score_table.empty:
        score_text = score_table.to_string(index=True)
        system_prompt = (
            "You are an expert NBA analyst providing insightful, narrative-driven analysis.\n\n"
            "Adapt your formatting to best answer the specific question asked. Do NOT use standard rigid headers.\n"
            "Instead, write in a fluid, engaging sports article style:\n"
            "- Start with a strong hook or direct answer.\n"
            "- Use natural paragraphs, bold text for emphasis, and bullet points only when helpful.\n"
            "- Incorporate context, player roles, and data limitations organically.\n"
            "Use the provided ranking. Be specific and reference actual numbers from the data."
        )
        if is_comparison:
            system_prompt += (
                f"\n\nThis is a {comparison_entity_label} COMPARISON. Structure your response as:\n"
                f"1. A paragraph on each {comparison_entity_singular}'s performance, referencing specific stats.\n"
                "2. Where they each have an edge (scoring, efficiency, playmaking, defense, etc.).\n"
                "3. End with a clear 1-2 sentence VERDICT: who had the better season overall and why. "
                "Be decisive — don't hedge with 'both were great'. Pick a winner and justify it."
        )
        if is_game_log:
            system_prompt += "\n\nNote: Data comes from game-by-game logs. Focus on trends, streaks, consistency, or individual game performances."
            
        user_prompt = (
            f"Question: {question}\n\n"
            f"Domain: {domain}\n"
            f"Guidance: {rubric_by_domain.get(domain, '')}\n\n"
            f"RANKED (DO NOT REORDER):\n{score_text}\n\n"
            f"ORIGINAL DATA (first rows):\n{display_rows_to_show.to_string(index=False)}\n\n"
            f"Analyze the top result and compare to the next strongest contenders in a natural, engaging format.{per_note}"
        )
    else:
        system_prompt = (
            "You are an expert NBA analyst providing insightful, narrative-driven analysis.\n\n"
            f"Domain: {domain}\n"
            f"Guidance: {rubric_by_domain.get(domain, '')}\n\n"
            "Adapt your formatting to best answer the specific question asked.\n"
            "- For a simple stat check, provide a concise, direct answer.\n"
            "- For complex questions, use engaging paragraphs and bold text for emphasis.\n"
            "- Only use bullet points if listing out specific game logs or multiple stats.\n"
            "Be specific and reference actual numbers from the data."
        )
        if is_comparison:
            system_prompt += (
                f"\n\nThis is a {comparison_entity_label} COMPARISON. Structure your response as:\n"
                f"1. A paragraph on each {comparison_entity_singular}'s performance, referencing specific stats.\n"
                "2. Where they each have an edge (scoring, efficiency, playmaking, defense, etc.).\n"
                "3. End with a clear 1-2 sentence VERDICT: who had the better season overall and why. "
                "Be decisive — don't hedge with 'both were great'. Pick a winner and justify it."
        )
        if is_game_log:
            system_prompt += "\n\nNote: Data comes from game-by-game logs. Focus your narrative on recent form, splits, streaks, or single-game anomalies."

        user_prompt = (
            f"User's question: {question}\n\n"
            f"Data:\n{df_summary}{per_note}\n"
            "Analyze the data and answer the question in a fluid, engaging sports-analyst style."
        )

    # For comparisons, reinforce the directive in the user prompt too so
    # the model can't ignore the system-level instruction.
    if is_comparison:
        try:
            if "player_name" in df.columns:
                unique_players = df["player_name"].dropna().astype(str).str.strip().unique().tolist()
            elif team_entity_col:
                unique_players = df[team_entity_col].dropna().astype(str).str.strip().unique().tolist()
            else:
                unique_players = []
        except Exception:
            unique_players = []
        if len(unique_players) >= 2:
            names_str = " and ".join(unique_players[:4])
            user_prompt += (
                f"\n\nCRITICAL: This is a HEAD-TO-HEAD comparison between {names_str}. "
                f"You MUST discuss BOTH {comparison_entity_noun} in detail — do NOT only mention one. "
                f"Cover each {comparison_entity_singular}'s stats, where each has an edge, and end with a verdict."
            )

    try:
        response = client.chat.completions.create(
            model="gpt-5.4-mini",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0,
            max_completion_tokens=1600,
        )
        raw_response = response.choices[0].message.content.strip()
        
        # RETRY: if the response is suspiciously short for a comparison
        # (under 150 chars when we have 2+ players), the model likely
        # ignored the comparison directive. Retry with higher temperature
        # and an even more forceful prompt.
        if is_comparison and len(raw_response) < 150:
            retry_prompt = (
                f"Your previous response was too short and only mentioned one {comparison_entity_singular}. "
                f"The user asked to COMPARE multiple {comparison_entity_noun}. Here is the data again:\n\n"
                f"{df_summary}\n\n"
                f"Write a FULL comparison covering EACH {comparison_entity_singular}'s stats, their relative "
                f"strengths, and a clear verdict on who performed better. "
                f"Minimum 4 sentences."
            )
            retry_response = client.chat.completions.create(
                model="gpt-5.4-mini",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                    {"role": "assistant", "content": raw_response},
                    {"role": "user", "content": retry_prompt},
                ],
                temperature=0.3,
                max_completion_tokens=1600,
            )
            retry_text = retry_response.choices[0].message.content.strip()
            if len(retry_text) > len(raw_response):
                raw_response = retry_text

        formatted_response = raw_response.replace("###", "\n\n###").replace("####", "\n\n####")
        return "\n" + formatted_response.strip()
    except Exception as e:
        return f"Error during AI analysis: {str(e)}"

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Query+Analyze chatbot for NBA data")
    parser.add_argument("-q", "--question", help="Ask a one-shot question to query and analyze")
    args = parser.parse_args()

    if args.question:
        answer = analyze_question(args.question)
        print(answer)
    else:
        while True:
            try:
                q = input("\nask> ").strip()
            except (KeyboardInterrupt, EOFError):
                break
            if not q:
                continue
            if q.lower() in {"exit", "quit"}:
                break
            answer = analyze_question(q)
            print(answer)
