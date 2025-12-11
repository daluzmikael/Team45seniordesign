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


DEFENSE_CONFIG = ScoreConfig(
    weights={
        "defensive_impact": 0.35,
        "rim_fg_pct_allowed": 0.18,
        "rim_shots_contested": 0.12,
        "opp_fg_pct_as_primary_defender": 0.10,
        "versatility_index": 0.10,
        "deflections_per_game": 0.08,
        "loose_balls_recovered": 0.04,
        "stl_per_game": 0.02,
        "blk_per_game": 0.01,
    },
    invert={"rim_fg_pct_allowed", "opp_fg_pct_as_primary_defender"},
    tiebreakers=[
        "defensive_impact",
        "rim_fg_pct_allowed",
        "rim_shots_contested",
        "versatility_index",
        "opp_fg_pct_as_primary_defender",
        "deflections_per_game",
    ],
)

SHOOTING_CONFIG = ScoreConfig(
    weights={"three_pt_pct": 0.55, "three_pm": 0.30, "three_pa": 0.15},
    invert=set(),
    tiebreakers=["three_pt_pct", "three_pm", "three_pa"],
)

PLAYMAKING_CONFIG = ScoreConfig(
    weights={
        "ast_per_game": 0.45,
        "ast_pct": 0.20,
        "potential_ast": 0.15,
        "assist_points_created": 0.10,
        "tov_per_game": 0.05,
        "ast_to_tov": 0.05,
    },
    invert={"tov_per_game"},
    tiebreakers=["ast_per_game", "ast_pct", "ast_to_tov", "potential_ast"],
)

SCORING_CONFIG = ScoreConfig(
    weights={"ppg": 0.45, "ts_pct": 0.30, "fga": 0.15, "usage_pct": 0.10},
    invert=set(),
    tiebreakers=["ppg", "ts_pct", "usage_pct"],
)

DOMAIN_CONFIGS: Dict[str, ScoreConfig] = {
    "defense": DEFENSE_CONFIG,
    "shooting": SHOOTING_CONFIG,
    "playmaking": PLAYMAKING_CONFIG,
    "scoring": SCORING_CONFIG,
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
    if any(k in uq for k in ["defense", "defender", "rim", "steal", "block"]) or any(
        k in txt for k in ["defensive_impact", "rim_fg_pct_allowed", "deflections_per_game"]
    ):
        return "defense"
    if any(k in uq for k in ["shoot", "3pt", "three", "percentage", "catch-and-shoot"]) or any(
        k in txt for k in ["three_pt_pct", "three_pm", "three_pa"]
    ):
        return "shooting"
    if any(k in uq for k in ["assist", "playmaker", "passing"]) or any(
        k in txt for k in ["ast_per_game", "ast_pct", "potential_ast"]
    ):
        return "playmaking"
    if any(k in uq for k in ["score", "scorer", "points"]) or any(
        k in txt for k in ["ppg", "ts_pct", "usage_pct"]
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
            "You are an expert NBA analyst. Use ONLY the provided data and ranking. "
            "DO NOT change the ranking order. Write an in-depth analysis with clear sections: "
            "1) Executive summary, 2) Ranking rationale, 3) Top contenders breakdown with metric-by-metric evidence "
            "(include strengths, weaknesses, and role fit), 4) Context (usage, role, sample caveats), "
            "5) Sensitivity and limitations, 6) Actionable insights. Be specific and role-aware."
        )
        user_prompt = (
            f"Question: {user_input}\n\n"
            f"Domain: {domain}\n"
            f"Guidance: {rubric_by_domain[domain]}\n\n"
            f"RANKED (DO NOT REORDER):\n{score_text}\n\n"
            f"ORIGINAL DATA (first rows):\n{rows_to_show.to_string(index=False)}\n\n"
            f"Explain the result by identifying #1 and comparing them to the next strongest contenders."
        )
    else:
        system_prompt = (
            "You are an expert NBA data analyst. Produce an in-depth analysis based ONLY on the provided DataFrame.\n\n"
            f"DECISION RUBRIC (domain={domain}): {rubric_by_domain[domain]}\n"
            "If players span positions, explain role differences but still pick ONE overall best unless asked by role.\n"
            "Structure your response with: Executive summary; Detailed findings (metric-by-metric); "
            "Comparative evaluation; Context (role, usage, sample size); Limitations; Actionable insights. "
            "Be specific, avoid generic statements, and tie every claim to the data shown."
        )
        user_prompt = (
            f"User's question: {user_input}\n\n"
            f"Data:\n{df_summary}\n"
            "Please analyze the data and answer the question clearly."
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
        return response.choices[0].message.content.strip()
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
            "You are an expert NBA analyst. Use ONLY the provided data and ranking. "
            "DO NOT change the ranking order. Write an in-depth analysis with clear sections: "
            "1) Executive summary, 2) Ranking rationale, 3) Top contenders breakdown with metric-by-metric evidence "
            "(include strengths, weaknesses, and role fit), 4) Context (usage, role, sample caveats), "
            "5) Sensitivity and limitations, 6) Actionable insights. Be specific and role-aware."
        )
        user_prompt = (
            f"Question: {question}\n\n"
            f"Domain: {domain}\n"
            f"Guidance: {rubric_by_domain[domain]}\n\n"
            f"RANKED (DO NOT REORDER):\n{score_text}\n\n"
            f"ORIGINAL DATA (first rows):\n{rows_to_show.to_string(index=False)}\n\n"
            f"Explain the result by identifying #1 and comparing them to the next strongest contenders."
        )
    else:
        system_prompt = (
            "You are an expert NBA data analyst. Produce an in-depth analysis based ONLY on the provided DataFrame.\n\n"
            f"DECISION RUBRIC (domain={domain}): {rubric_by_domain[domain]}\n"
            "If players span positions, explain role differences but still pick ONE overall best unless asked by role.\n"
            "Structure your response with: Executive summary; Detailed findings (metric-by-metric); "
            "Comparative evaluation; Context (role, usage, sample size); Limitations; Actionable insights. "
            "Be specific, avoid generic statements, and tie every claim to the data shown."
        )
        user_prompt = (
            f"User's question: {question}\n\n"
            f"Data:\n{df_summary}\n"
            "Please analyze the data and answer the question clearly."
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
        return response.choices[0].message.content.strip()
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