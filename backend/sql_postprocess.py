"""Shared SQL string fixes before execution (hallucinated column names, etc.)."""

from __future__ import annotations

import re


def normalize_game_log_wl_column(sql_query: str) -> str:
    """
    player_game_logs uses `wl` ('W'/'L'); models sometimes emit nonexistent `result`.
    Only applies when the query references player_game_logs.
    """
    if not sql_query or "player_game_logs" not in sql_query.lower():
        return sql_query
    out = sql_query
    out = re.sub(r"(?i)CASE\s+WHEN\s+result\s*=", "CASE WHEN wl =", out)
    out = re.sub(r"(?i)\bAND\s+result\s*=", "AND wl =", out)
    out = re.sub(r"(?i)\bOR\s+result\s*=", "OR wl =", out)
    out = re.sub(r"(?i)\bWHERE\s+result\s*=", "WHERE wl =", out)
    return out
