"""
Post-query data normalization for analyzers and API JSON (no LLM prompt changes).

Converts DB/driver types (Decimal, datetime) into JSON-safe values, trims strings,
and optionally adds a `stat_value` alias when a single obvious metric column exists.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any, List, Optional

import numpy as np
import pandas as pd

__all__ = [
    "prepare_dataframe_for_downstream",
    "prepare_raw_records",
    "dataframe_to_json_records",
]


def _json_safe_scalar(value: Any) -> Any:
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return None
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return float(value)
    if isinstance(value, np.bool_):
        return bool(value)
    return value


def prepare_dataframe_for_downstream(df: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
    """
    Clean a query result DataFrame for pandas analytics and JSON serialization.
    Safe to call multiple times.
    """
    if df is None or df.empty:
        return df

    out = df.copy()
    out = out.apply(lambda col: col.map(_json_safe_scalar))
    out = _maybe_add_stat_value(out)
    return out


def _maybe_add_stat_value(df: pd.DataFrame) -> pd.DataFrame:
    """If `stat_value` is missing, copy the first matching metric column when unambiguous."""
    lower = {str(c).lower(): c for c in df.columns}
    if "stat_value" in lower:
        return df

    for candidate in (
        "total_pts",
        "pts",
        "points",
        "total_points",
        "ppg",
        "value",
    ):
        if candidate in lower:
            col = lower[candidate]
            out = df.copy()
            out["stat_value"] = out[col]
            return out
    return df


def prepare_raw_records(records: List[Any]) -> List[dict]:
    """
    Normalize rows from psycopg2 RealDictCursor (or plain dicts) for charts / JSON.
    """
    if not records:
        return []

    out: List[dict] = []
    for row in records:
        if row is None:
            continue
        d = dict(row) if not isinstance(row, dict) else row
        cleaned = {str(k): _json_safe_scalar(v) for k, v in d.items()}
        cleaned = _maybe_add_stat_value_to_record(cleaned)
        out.append(cleaned)
    return out


def _maybe_add_stat_value_to_record(rec: dict) -> dict:
    if "stat_value" in {k.lower() for k in rec}:
        return rec
    lower_map = {str(k).lower(): k for k in rec}
    for candidate in ("total_pts", "pts", "points", "total_points", "ppg", "value"):
        if candidate in lower_map:
            key = lower_map[candidate]
            rec = {**rec, "stat_value": rec[key]}
            return rec
    return rec


def dataframe_to_json_records(df: pd.DataFrame) -> List[dict]:
    """Replace NaN with None and return records suitable for FastAPI JSON."""
    if df is None or df.empty:
        return []
    prepared = prepare_dataframe_for_downstream(df)
    if prepared is None:
        return []
    return prepared.replace({np.nan: None}).to_dict(orient="records")
