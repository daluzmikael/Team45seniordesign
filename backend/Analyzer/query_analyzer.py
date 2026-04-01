import os
import sys
import re
from dataclasses import dataclass
from typing import Optional, Any, List, Set, Dict
from pathlib import Path
import importlib.util

import numpy as np
import pandas as pd

# Dynamically find and import query_bot
query_bot_module = None

def find_query_bot():
    """Search for query_bot.py starting from current file and going up the directory tree"""
    current_file = Path(__file__).resolve()
    current_dir = current_file.parent
    
    # Search upward through parent directories
    search_dir = current_dir
    for _ in range(5):  # Search up to 5 levels up
        # Check common locations relative to current search directory
        candidates = [
            search_dir / "query_bot.py",
            search_dir / "Executer" / "query_bot.py",
            search_dir / ".." / "Executer" / "query_bot.py",  # Analyzer/../Executer
            search_dir / "backend" / "Executer" / "query_bot.py",
            search_dir / "backend" / "query_bot.py",
        ]
        
        for candidate in candidates:
            resolved = candidate.resolve()
            if resolved.exists():
                return str(resolved)
        
        # Move up one directory
        search_dir = search_dir.parent
    
    return None

# Try to import query_bot
try:
    # First try direct import (if already in path)
    import query_bot as query_bot_module
except ImportError:
    # Find the file dynamically
    query_bot_path = find_query_bot()
    
    if query_bot_path:
        # Load it manually using importlib
        try:
            spec = importlib.util.spec_from_file_location("query_bot", query_bot_path)
            if spec and spec.loader:
                query_bot_module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(query_bot_module)
        except Exception as e:
            print(f"Warning: Failed to load query_bot from {query_bot_path}: {e}")

# Optional stub client if query_bot doesn't export one
try:
    from openai import OpenAI
except Exception:
    OpenAI = None


def _resolve_client(module: Optional[Any]):
    if module is not None and hasattr(module, "client"):
        return getattr(module, "client")
    if OpenAI is not None:
        api_key = os.getenv("OPENAI_API_KEY")
        if api_key:
            try:
                return OpenAI(api_key=api_key)
            except Exception:
                return None
    return None


def _resolve_user_input(module: Optional[Any]) -> Optional[str]:
    if module is not None and hasattr(module, "user_input"):
        return getattr(module, "user_input")
    return None


def _resolve_df_output(module: Optional[Any]) -> Optional[pd.DataFrame]:
    if module is not None and hasattr(module, "df_output"):
        value = getattr(module, "df_output")
        try:
            if isinstance(value, pd.DataFrame):
                return value
        except Exception:
            pass
    try:
        import temp_df
        if hasattr(temp_df, "df_output") and isinstance(temp_df.df_output, pd.DataFrame):
            return temp_df.df_output
    except Exception:
        pass
    return None


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
    if "player_name" not in df.columns:
        raise ValueError("DataFrame must include 'player_name'.")
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
                "player_name": df.iloc[i]["player_name"],
                "composite_score": float(score[i]),
                "top_contributors": contribs[:3],
            }
        )
    return pd.DataFrame(rows)


def infer_domain(user_q: str, cols: List[str]) -> str:
    uq = (user_q or "").lower()
    txt = " ".join(cols).lower()

    if any(k in uq for k in ["rebound", "boards", "glass", "oreb", "dreb"]) or any(
        k in txt for k in ["trb_per_game", "trb_pct", "oreb_pct", "dreb_pct", "contested_reb"]
    ):
        return "rebounding"

    if any(k in uq for k in ["mvp", "best player", "top players", "overall", "impact", "most valuable"]) or any(
        k in txt for k in ["overall_impact", "on_off"]
    ):
        return "overall_impact"

    if any(k in uq for k in ["defense", "defender", "rim", "steal", "block"]) or any(
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


def _build_simple_top_scorers_prompts(question: str, rows_to_show: pd.DataFrame) -> tuple[str, str]:
    system_prompt = (
        "You are an NBA stats assistant. For simple top-scorer questions, keep the response short and plain.\n"
        "Output format must be exactly:\n"
        "1) One short lead sentence in plain English, like: "
        "\"[Player] led the league at [PPG] PPG on [FG%] FG.\"\n"
        "2) A compact list of exactly the top 5 scorers from the provided data, each line in this exact style: "
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
    match = re.search(r"\b(top|best|leading)\s+(\d{1,3})\b", q)
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
    if not (has_over_time_phrase or has_year_range_phrase):
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

    is_shooting = any(k in q for k in ["shoot", "fg%", "3 point", "3p", "percentage"])
    is_rebounding = any(k in q for k in ["rebound", "boards", "glass"])
    is_defense = any(k in q for k in ["block", "blk", "steal", "stl", "defense"])

    if is_shooting:
        columns = [("FG%", "fg_pct"), ("3P%", "fg3_pct"), ("FT%", "ft_pct"), ("FGM", "fgm"), ("FGA", "fga"), ("3PM", "fg3m"), ("3PA", "fg3a")]
    elif is_rebounding:
        columns = [("REB", "reb"), ("OREB", "oreb"), ("DREB", "dreb"), ("REB Rank", "reb_rank"), ("OREB Rank", "oreb_rank"), ("DREB Rank", "dreb_rank")]
    elif is_defense:
        columns = [("BLK", "blk"), ("STL", "stl"), ("BLK Rank", "blk_rank"), ("STL Rank", "stl_rank"), ("Games", "gp")]
    else:
        columns = [("PTS", "pts"), ("REB", "reb"), ("AST", "ast"), ("FG%", "fg_pct"), ("3P%", "fg3_pct")]

    available = [(label, col) for label, col in columns if col in working.columns]
    if not available:
        return ""

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
        lines.append("| " + season_text + " | " + " | ".join(vals) + " |")

    if client is not None:
        try:
            sample = working[[season_col] + [c for _, c in available]].head(8).to_dict(orient="records")
            resp = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "Write 2 concise sentences summarizing the trend in these season-by-season stats. Use plain language and include at least one concrete stat reference."},
                    {"role": "user", "content": f"Question: {question}\nPlayer: {player_name}\nSeason rows: {sample}"},
                ],
                temperature=0.2,
                max_tokens=140,
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

    if not summary:
        summary = "This table shows the season-by-season trend so you can see how his production and efficiency changed over time."

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
        "pts": _safe(row.get("pts")),
        "pts_rank": _safe(row.get("pts_rank")),
        "fg_pct": _safe(row.get("fg_pct")),
        "fg3_pct": _safe(row.get("fg3_pct")),
        "fga": _safe(row.get("fga")),
        "fga_rank": _safe(row.get("fga_rank")),
        "fg3a": _safe(row.get("fg3a")),
        "fg3a_rank": _safe(row.get("fg3a_rank")),
        "reb": _safe(row.get("reb")),
        "reb_rank": _safe(row.get("reb_rank")),
        "ast": _safe(row.get("ast")),
        "ast_rank": _safe(row.get("ast_rank")),
        "stl": _safe(row.get("stl")),
        "stl_rank": _safe(row.get("stl_rank")),
        "blk": _safe(row.get("blk")),
        "blk_rank": _safe(row.get("blk_rank")),
        "plus_minus": _safe(row.get("plus_minus")),
    }

    system_prompt = (
        "You are an NBA analyst. Write a short, natural season review in 2-3 sentences.\n"
        "Tone must be neutral and direct, not robotic and not overhyped.\n"
        "Do not use a rigid template like 'X averaged ... top Y ...'.\n"
        "Use the provided stats together to explain scoring + efficiency + volume context.\n"
        "Always mention games played and note sample-size caution if games are low (around <50).\n"
        "Return plain text only."
    )
    user_prompt = (
        f"Question: {question}\n"
        f"Season profile data: {payload}\n"
        "Write the 2-3 sentence review now."
    )

    try:
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.3,
            max_tokens=180,
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

    if "player_name" in cols:
        working = working.dropna(subset=["player_name"])
        working = working.drop_duplicates(subset=["player_name"], keep="first")
    else:
        working = working.drop_duplicates(keep="first")

    requested_n = _extract_requested_top_n(question, default_n=5, max_n=50)
    top = working.head(requested_n)
    if top.empty:
        return "\nNo leaderboard data was available for this question."

    lead_row = top.iloc[0]
    lead_name = str(lead_row.get("player_name", "N/A")) if not _is_missing(lead_row.get("player_name")) else "N/A"
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
        name = str(row.get("player_name", "N/A")) if not _is_missing(row.get("player_name")) else "N/A"
        team = str(row.get("team_abbreviation", "N/A")) if not _is_missing(row.get("team_abbreviation")) else "N/A"
        lines.append(selected["row_builder"](idx, row, name, team))

    lines.append("")
    lines.append("Want me to break down each player's profile further?")
    return "\n" + "\n".join(lines)


def analyze_dataframe() -> str:
    if query_bot_module is None:
        return (
            "Error: Could not import query_bot. Make sure query_bot.py exists in backend/Executer/ directory."
        )

    user_input = _resolve_user_input(query_bot_module)
    df_output = _resolve_df_output(query_bot_module)
    client = _resolve_client(query_bot_module)

    if not user_input:
        return "Error: No user input available from query_bot."
    if df_output is None:
        return "Error: No data available to analyze (df_output missing)."
    if df_output.empty:
        return "Error: The query returned an empty result set."
    if client is None:
        return "Error: OpenAI client not available. Set OPENAI_API_KEY or expose 'client' in query_bot."

    domain = infer_domain(user_input, df_output.columns.tolist())

    # Check if we are using the game logs table by looking for game_id
    is_game_log = "game_id" in df_output.columns

    # Skip the scoring/ranking engine entirely if it's just a player's game logs
    score_table: Optional[pd.DataFrame] = None
    if not is_game_log:
        try:
            cfg = DOMAIN_CONFIGS[domain]
            score_table = compute_scores(df_output, cfg)
        except Exception:
            score_table = None

    is_comparison = len(df_output) <= 5 and any(
        k in user_input.lower() for k in ["compare", "better", "versus", "vs", "between", "who"]
    )
    rows_to_show = df_output if is_comparison else df_output.head(20)

    df_summary = (
        f"DataFrame shape: {df_output.shape[0]} rows, {df_output.shape[1]} columns\n"
        f"Columns: {', '.join(df_output.columns.tolist())}\n\n"
        f"Data:\n{rows_to_show.to_string(index=False)}\n"
    )
    if len(df_output) > 20 and not is_comparison:
        df_summary += f"\nSummary statistics:\n{df_output.describe().to_string()}\n"

    rubric_by_domain = {
        "defense": "Prioritize defensive_impact; rim protection (rim_fg_pct_allowed lower is better; rim_shots_contested higher is better); on-ball impact (opp_fg_pct_as_primary_defender lower is better); versatility; disruptions (deflections, loose balls).",
        "shooting": "Prioritize accuracy (three_pt_pct), then volume (three_pm, three_pa). Include role, shot quality, and sustainability commentary.",
        "playmaking": "Prioritize ast_per_game, ast_pct, potential_ast, assist_points_created; penalize turnovers (tov_per_game lower is better); reward efficiency (ast_to_tov). Consider on-ball workload.",
        "scoring": "Prioritize ppg and efficiency (ts_pct), then usage and volume (fga). Discuss shot mix and scalability.",
    }

    # Check if we are using the game logs table by looking for game_id
    is_game_log = "game_id" in df_output.columns

    is_simple_top_scorers = _is_simple_top_scorers_question(user_input, domain)
    is_single_player_trend = _is_single_player_season_trend_question(user_input, df_output)
    is_single_player_stats = _is_single_player_stats_question(user_input, df_output)

    if is_single_player_trend:
        trend_text = _format_single_player_season_trend_response(df_output, user_input, client)
        if trend_text:
            return trend_text
    if is_single_player_stats:
        return _format_single_player_stats_profile(df_output, user_input, client)
    elif is_simple_top_scorers:
        return _format_simple_top_scorers_response(df_output, user_input)
    elif score_table is not None and not score_table.empty:
        score_text = score_table.to_string(index=True)
        system_prompt = (
            "You are an expert NBA analyst providing insightful, narrative-driven analysis.\n\n"
            "Adapt your formatting to best answer the specific question asked. Do NOT use standard rigid headers like 'Executive Summary' or 'Detailed Analysis' every time.\n"
            "Instead, write in a fluid, engaging sports article style:\n"
            "- Start with a strong hook or direct answer.\n"
            "- Use natural paragraphs, bold text for emphasis, and bullet points only when helpful (like listing specific player rankings).\n"
            "- Incorporate context, player roles, and data limitations organically into your sentences.\n"
            "Use the provided ranking. Be specific and reference actual numbers from the data."
        )
        if is_game_log:
            system_prompt += "\n\nNote: The data provided comes from game-by-game logs. Focus on trends, streaks, consistency, or individual game performances rather than just overall averages."
            
        user_prompt = (
            f"Question: {user_input}\n\n"
            f"Domain: {domain}\n"
            f"Guidance: {rubric_by_domain[domain]}\n\n"
            f"RANKED (DO NOT REORDER):\n{score_text}\n\n"
            f"ORIGINAL DATA (first rows):\n{rows_to_show.to_string(index=False)}\n\n"
            f"Analyze the top result and compare to the next strongest contenders in a natural, engaging format."
        )
    else:
        system_prompt = (
            "You are an expert NBA analyst providing insightful, narrative-driven analysis.\n\n"
            f"Domain: {domain}\n"
            f"Guidance: {rubric_by_domain[domain]}\n\n"
            "Adapt your formatting to best answer the specific question asked. Do NOT use standard rigid headers like 'Executive Summary' or 'Detailed Analysis' every time.\n"
            "Instead, structure your response organically:\n"
            "- For a simple stat check, provide a concise, direct answer.\n"
            "- For complex questions, use engaging paragraphs and bold text for emphasis.\n"
            "- Only use bullet points if listing out specific game logs or multiple stats.\n"
            "Be specific and reference actual numbers from the data."
        )
        if is_game_log:
            system_prompt += "\n\nNote: The data provided comes from game-by-game logs. Focus your narrative on recent form, splits, streaks, or single-game anomalies."

        user_prompt = (
            f"User's question: {user_input}\n\n"
            f"Data:\n{df_summary}\n"
            "Analyze the data and answer the question in a fluid, engaging sports-analyst style."
        )

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0,
            max_tokens=1600,
        )
        raw_response = response.choices[0].message.content.strip()
        
        # Format the response for better readability
        formatted_response = raw_response.replace("###", "\n\n###").replace("####", "\n\n####")
        formatted_response = "\n" + formatted_response.strip()
        
        return formatted_response
    except Exception as e:
        return f"Error during AI analysis: {str(e)}"


def analyze() -> str:
    return analyze_dataframe()


def analyze_question(question: str) -> str:
    """Run a real-time question through query_bot, then analyze the resulting DataFrame."""
    if query_bot_module is None:
        return (
            "Error: Could not import query_bot. Make sure query_bot.py exists in backend/Executer/ directory."
        )

    df = None
    if hasattr(query_bot_module, 'run_query'):
        try:
            df = query_bot_module.run_query(question)
        except Exception as exc:
            return f"Error running query_bot.run_query: {exc}"
    elif hasattr(query_bot_module, 'natural_language_to_sql'):
        try:
            df = query_bot_module.natural_language_to_sql(question)
        except Exception as exc:
            return f"Error running query_bot.natural_language_to_sql: {exc}"

    try:
        setattr(query_bot_module, 'user_input', question)
    except Exception:
        pass

    if df is None:
        df = _resolve_df_output(query_bot_module)

    client = _resolve_client(query_bot_module)
    if df is None:
        return "Error: No data returned from query_bot."
    if df is not None and getattr(df, 'empty', False):
        return "Error: The query returned an empty result set."
    if client is None:
        return "Error: OpenAI client not available. Set OPENAI_API_KEY or expose 'client' in query_bot."

    domain = infer_domain(question, df.columns.tolist())

    score_table: Optional[pd.DataFrame] = None
    try:
        cfg = DOMAIN_CONFIGS[domain]
        score_table = compute_scores(df, cfg)
    except Exception:
        score_table = None

    is_comparison = len(df) <= 5 and any(
        k in question.lower() for k in ["compare", "better", "versus", "vs", "between", "who"]
    )
    rows_to_show = df if is_comparison else df.head(20)

    df_summary = (
        f"DataFrame shape: {df.shape[0]} rows, {df.shape[1]} columns\n"
        f"Columns: {', '.join(df.columns.tolist())}\n\n"
        f"Data:\n{rows_to_show.to_string(index=False)}\n"
    )
    if len(df) > 20 and not is_comparison:
        df_summary += f"\nSummary statistics:\n{df.describe().to_string()}\n"

    rubric_by_domain = {
        "defense": "Prioritize defensive_impact; rim protection (rim_fg_pct_allowed lower is better; rim_shots_contested higher is better); on-ball impact (opp_fg_pct_as_primary_defender lower is better); versatility; disruptions (deflections, loose balls).",
        "shooting": "Prioritize accuracy (three_pt_pct), then volume (three_pm, three_pa). Include role, shot quality, and sustainability commentary.",
        "playmaking": "Prioritize ast_per_game, ast_pct, potential_ast, assist_points_created; penalize turnovers (tov_per_game lower is better); reward efficiency (ast_to_tov). Consider on-ball workload.",
        "scoring": "Prioritize ppg and efficiency (ts_pct), then usage and volume (fga). Discuss shot mix and scalability.",
    }

    # Check if we are using the game logs table by looking for game_id
    is_game_log = "game_id" in df.columns 

    is_simple_top_scorers = _is_simple_top_scorers_question(question, domain)
    is_single_player_trend = _is_single_player_season_trend_question(question, df)
    is_single_player_stats = _is_single_player_stats_question(question, df)

    if is_single_player_trend:
        trend_text = _format_single_player_season_trend_response(df, question, client)
        if trend_text:
            return trend_text
    if is_single_player_stats:
        return _format_single_player_stats_profile(df, question, client)
    elif is_simple_top_scorers:
        return _format_simple_top_scorers_response(df, question)
    elif score_table is not None and not score_table.empty:
        score_text = score_table.to_string(index=True)
        system_prompt = (
            "You are an expert NBA analyst providing insightful, narrative-driven analysis.\n\n"
            "Adapt your formatting to best answer the specific question asked. Do NOT use standard rigid headers like 'Executive Summary' or 'Detailed Analysis' every time.\n"
            "Instead, write in a fluid, engaging sports article style:\n"
            "- Start with a strong hook or direct answer.\n"
            "- Use natural paragraphs, bold text for emphasis, and bullet points only when helpful (like listing specific player rankings).\n"
            "- Incorporate context, player roles, and data limitations organically into your sentences.\n"
            "Use the provided ranking. Be specific and reference actual numbers from the data."
        )
        if is_game_log:
            system_prompt += "\n\nNote: The data provided comes from game-by-game logs. Focus on trends, streaks, consistency, or individual game performances rather than just overall averages."
            
        user_prompt = (
            f"Question: {question}\n\n"
            f"Domain: {domain}\n"
            f"Guidance: {rubric_by_domain[domain]}\n\n"
            f"RANKED (DO NOT REORDER):\n{score_text}\n\n"
            f"ORIGINAL DATA (first rows):\n{rows_to_show.to_string(index=False)}\n\n"
            f"Analyze the top result and compare to the next strongest contenders in a natural, engaging format."
        )
    else:
        system_prompt = (
            "You are an expert NBA analyst providing insightful, narrative-driven analysis.\n\n"
            f"Domain: {domain}\n"
            f"Guidance: {rubric_by_domain[domain]}\n\n"
            "Adapt your formatting to best answer the specific question asked. Do NOT use standard rigid headers like 'Executive Summary' or 'Detailed Analysis' every time.\n"
            "Instead, structure your response organically:\n"
            "- For a simple stat check, provide a concise, direct answer.\n"
            "- For complex questions, use engaging paragraphs and bold text for emphasis.\n"
            "- Only use bullet points if listing out specific game logs or multiple stats.\n"
            "Be specific and reference actual numbers from the data."
        )
        if is_game_log:
            system_prompt += "\n\nNote: The data provided comes from game-by-game logs. Focus your narrative on recent form, splits, streaks, or single-game anomalies."

        user_prompt = (
            f"User's question: {question}\n\n"
            f"Data:\n{df_summary}\n"
            "Analyze the data and answer the question in a fluid, engaging sports-analyst style."
        )

    try:
        response = _resolve_client(query_bot_module).chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0,
            max_tokens=1600,
        )
        raw_response = response.choices[0].message.content.strip()
        
        # Format the response for better readability
        formatted_response = raw_response.replace("###", "\n\n###").replace("####", "\n\n####")
        formatted_response = "\n" + formatted_response.strip()
        
        return formatted_response
    except Exception as e:
        return f"Error during AI analysis: {str(e)}"


def analyze_question_with_data(question: str, df: pd.DataFrame) -> str:
    """
    Analyze a pre-fetched DataFrame directly without re-running any query.
    This is called from main.py after run_query() has already succeeded,
    so we never run the query twice or trigger a false empty-result error.
    """
    client = _resolve_client(query_bot_module)
    if client is None:
        # Fall back to building a client directly from env
        try:
            from openai import OpenAI
            client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        except Exception as e:
            return f"Error: OpenAI client not available: {e}"

    if df is None or df.empty:
        return (
            "No data was found for this query. The player may not have participated "
            "in the requested season or playoffs, or the name was not recognized."
        )

    domain = infer_domain(question, df.columns.tolist())

    # Detect if data came from game logs (has game_date column)
    is_game_log = "game_date" in df.columns

    # Only run composite scoring for season summary data, not game logs
    score_table: Optional[pd.DataFrame] = None
    if not is_game_log:
        try:
            cfg = DOMAIN_CONFIGS[domain]
            score_table = compute_scores(df, cfg)
        except Exception:
            score_table = None

    is_comparison = len(df) <= 5 and any(
        k in question.lower() for k in ["compare", "better", "versus", "vs", "between", "who"]
    )
    rows_to_show = df if is_comparison else df.head(20)

    df_summary = (
        f"DataFrame shape: {df.shape[0]} rows, {df.shape[1]} columns\n"
        f"Columns: {', '.join(df.columns.tolist())}\n\n"
        f"Data:\n{rows_to_show.to_string(index=False)}\n"
    )
    if len(df) > 20 and not is_comparison:
        df_summary += f"\nSummary statistics:\n{df.describe().to_string()}\n"

    rubric_by_domain = {
        "defense": "Prioritize defensive_impact; rim protection (rim_fg_pct_allowed lower is better; rim_shots_contested higher is better); on-ball impact (opp_fg_pct_as_primary_defender lower is better); versatility; disruptions (deflections, loose balls).",
        "shooting": "Prioritize accuracy (three_pt_pct), then volume (three_pm, three_pa). Include role, shot quality, and sustainability commentary.",
        "playmaking": "Prioritize ast_per_game, ast_pct, potential_ast, assist_points_created; penalize turnovers (tov_per_game lower is better); reward efficiency (ast_to_tov). Consider on-ball workload.",
        "scoring": "Prioritize ppg and efficiency (ts_pct), then usage and volume (fga). Discuss shot mix and scalability.",
        "rebounding": "Prioritize trb_pct and trb_per_game, then oreb_pct and dreb_pct. Discuss contested rebounds and positioning.",
    }

    is_simple_top_scorers = _is_simple_top_scorers_question(question, domain)
    is_single_player_trend = _is_single_player_season_trend_question(question, df)
    is_single_player_stats = _is_single_player_stats_question(question, df)

    if is_single_player_trend:
        trend_text = _format_single_player_season_trend_response(df, question, client)
        if trend_text:
            return trend_text
    if is_single_player_stats:
        return _format_single_player_stats_profile(df, question, client)
    elif is_simple_top_scorers:
        return _format_simple_top_scorers_response(df, question)
    elif score_table is not None and not score_table.empty:
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
        if is_game_log:
            system_prompt += "\n\nNote: Data comes from game-by-game logs. Focus on trends, streaks, consistency, or individual game performances."

        user_prompt = (
            f"Question: {question}\n\n"
            f"Domain: {domain}\n"
            f"Guidance: {rubric_by_domain.get(domain, '')}\n\n"
            f"RANKED (DO NOT REORDER):\n{score_text}\n\n"
            f"ORIGINAL DATA (first rows):\n{rows_to_show.to_string(index=False)}\n\n"
            f"Analyze the top result and compare to the next strongest contenders in a natural, engaging format."
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
        if is_game_log:
            system_prompt += "\n\nNote: Data comes from game-by-game logs. Focus your narrative on recent form, splits, streaks, or single-game anomalies."

        user_prompt = (
            f"User's question: {question}\n\n"
            f"Data:\n{df_summary}\n"
            "Analyze the data and answer the question in a fluid, engaging sports-analyst style."
        )

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0,
            max_tokens=1600,
        )
        raw_response = response.choices[0].message.content.strip()
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