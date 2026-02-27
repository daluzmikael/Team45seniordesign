import os
import sys
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

    score_table: Optional[pd.DataFrame] = None
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

    if score_table is not None and not score_table.empty:
        score_text = score_table.to_string(index=True)
        system_prompt = (
            "You are an expert NBA analyst. Provide a clear, well-structured analysis.\n\n"
            "Format your response with these sections (use ### for headers):\n"
            "### Executive Summary\n"
            "One concise paragraph highlighting the key finding.\n\n"
            "### Key Metrics\n"
            "Bullet points of the most important stats (use - for bullets).\n\n"
            "### Detailed Analysis\n"
            "2-3 paragraphs with specific evidence from the data.\n\n"
            "### Context & Considerations\n"
            "Brief paragraph about role, usage, and limitations.\n\n"
            "Use the provided ranking. Be specific and reference actual numbers from the data."
        )
        user_prompt = (
            f"Question: {user_input}\n\n"
            f"Domain: {domain}\n"
            f"Guidance: {rubric_by_domain[domain]}\n\n"
            f"RANKED (DO NOT REORDER):\n{score_text}\n\n"
            f"ORIGINAL DATA (first rows):\n{rows_to_show.to_string(index=False)}\n\n"
            f"Analyze the top result and compare to the next strongest contenders."
        )
    else:
        system_prompt = (
            "You are an expert NBA analyst. Provide a clear, well-structured analysis.\n\n"
            f"Domain: {domain}\n"
            f"Guidance: {rubric_by_domain[domain]}\n\n"
            "Format your response with these sections (use ### for headers):\n"
            "### Executive Summary\n"
            "One concise paragraph highlighting the key finding.\n\n"
            "### Key Metrics\n"
            "Bullet points of the most important stats (use - for bullets).\n\n"
            "### Detailed Analysis\n"
            "2-3 paragraphs with specific evidence from the data.\n\n"
            "### Context & Considerations\n"
            "Brief paragraph about role, usage, and any limitations.\n\n"
            "Be specific and reference actual numbers from the data."
        )
        user_prompt = (
            f"User's question: {user_input}\n\n"
            f"Data:\n{df_summary}\n"
            "Analyze the data and answer the question clearly."
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

    if score_table is not None and not score_table.empty:
        score_text = score_table.to_string(index=True)
        system_prompt = (
            "You are an expert NBA analyst. Provide a clear, well-structured analysis.\n\n"
            "Format your response with these sections (use ### for headers):\n"
            "### Executive Summary\n"
            "One concise paragraph highlighting the key finding.\n\n"
            "### Key Metrics\n"
            "Bullet points of the most important stats (use - for bullets).\n\n"
            "### Detailed Analysis\n"
            "2-3 paragraphs with specific evidence from the data.\n\n"
            "### Context & Considerations\n"
            "Brief paragraph about role, usage, and limitations.\n\n"
            "Use the provided ranking. Be specific and reference actual numbers from the data."
        )
        user_prompt = (
            f"Question: {question}\n\n"
            f"Domain: {domain}\n"
            f"Guidance: {rubric_by_domain[domain]}\n\n"
            f"RANKED (DO NOT REORDER):\n{score_text}\n\n"
            f"ORIGINAL DATA (first rows):\n{rows_to_show.to_string(index=False)}\n\n"
            f"Analyze the top result and compare to the next strongest contenders."
        )
    else:
        system_prompt = (
            "You are an expert NBA analyst. Provide a clear, well-structured analysis.\n\n"
            f"Domain: {domain}\n"
            f"Guidance: {rubric_by_domain[domain]}\n\n"
            "Format your response with these sections (use ### for headers):\n"
            "### Executive Summary\n"
            "One concise paragraph highlighting the key finding.\n\n"
            "### Key Metrics\n"
            "Bullet points of the most important stats (use - for bullets).\n\n"
            "### Detailed Analysis\n"
            "2-3 paragraphs with specific evidence from the data.\n\n"
            "### Context & Considerations\n"
            "Brief paragraph about role, usage, and any limitations.\n\n"
            "Be specific and reference actual numbers from the data."
        )
        user_prompt = (
            f"User's question: {question}\n\n"
            f"Data:\n{df_summary}\n"
            "Analyze the data and answer the question clearly."
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