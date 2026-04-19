"""
Run against live Postgres (same .env as executor). Exercises execute_query normalizers.
Usage: from backend directory:  python Executer/edge_case_checks.py
   or: cd Executer && python edge_case_checks.py
Optional:  python Executer/edge_case_checks.py --nl   (needs OPENAI_API_KEY in backend/.env)
"""
from __future__ import annotations

import argparse
import logging
import os
import sys

# Allow `python edge_case_checks.py` from this folder or `python Executer/edge_case_checks.py` from backend.
_HERE = os.path.dirname(os.path.abspath(__file__))
_BACKEND = os.path.normpath(os.path.join(_HERE, ".."))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

from dotenv import load_dotenv

load_dotenv(os.path.join(_BACKEND, ".env"))  # noqa: E402
load_dotenv()  # cwd fallback

from executor import execute_query, get_connection  # noqa: E402


def _pick_table(conn, pattern: str, exclude_substrings: tuple[str, ...] = ()) -> str | None:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name LIKE %s
        ORDER BY table_name DESC
        """,
        (pattern,),
    )
    rows = [r[0] for r in cur.fetchall()]
    cur.close()
    for name in rows:
        if any(x in name for x in exclude_substrings):
            continue
        return name
    return rows[0] if rows else None


def _pick_team_advanced_table(conn) -> str | None:
    """Prefer season/per-game team_advanced tables over staging when both exist."""
    return _pick_table(conn, "%team_advanced%", exclude_substrings=("staging",))


def run_nl_checks() -> int:
    """End-to-end: LLM → SQL → execute_query (normalizers apply)."""
    key = (os.getenv("OPENAI_API_KEY") or "").strip()
    if not key:
        print("NL checks skipped (no OPENAI_API_KEY)")
        return 0

    if _BACKEND not in sys.path:
        sys.path.insert(0, _BACKEND)
    logging.getLogger().setLevel(logging.WARNING)
    from Interpreter.interpreter import natural_language_to_sql  # noqa: E402

    questions = [
        "Golden State Warriors team advanced stats 2024-25 regular season — net rating and pace.",
        "Oklahoma City Thunder team advanced stats 2024-25 regular season — net rating.",
        # fg3a in this schema is ~per-game attempts (max ~11), not season totals — avoid 50+ thresholds.
        "Top 5 players by three point percentage in 2024-25 regular season among players with at least 5 three point attempts per game.",
    ]
    failed = 0
    for q in questions:
        df, sql = natural_language_to_sql(q)
        n = 0 if df is None else len(df)
        ok = df is not None and not df.empty
        print(f"[{'OK' if ok else 'FAIL'}] NL: {q[:72]}...")
        print(f"       rows={n}")
        if sql:
            print(f"       sql: {sql[:200].replace(chr(10), ' ')}...")
        if not ok:
            failed += 1
        print()
    print("--- summary ---")
    print(f"NL cases: {len(questions)}  failed_or_empty: {failed}")
    return failed


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--nl",
        action="store_true",
        help="Also run natural_language_to_sql smoke tests (requires OPENAI_API_KEY)",
    )
    args = parser.parse_args()

    sql_ok = False
    conn = get_connection()
    try:
        ta = _pick_team_advanced_table(conn)
        ap = _pick_table(conn, "%all_players%")
        print("team_advanced table:", ta)
        print("all_players table:", ap)
        if not ta:
            print("FAIL: no team_advanced table found")
            return 1

        cases: list[tuple[str, str]] = [
            (
                "OKC abbrev in TEAM_NAME ILIKE (quoted)",
                f'SELECT "TEAM_ID", "TEAM_NAME" FROM "{ta}" '
                f'WHERE "TEAM_NAME" ILIKE \'%OKC%\' LIMIT 5;',
            ),
            (
                "GSW abbrev in TEAM_NAME ILIKE",
                f'SELECT "TEAM_ID", "TEAM_NAME" FROM "{ta}" '
                f'WHERE "TEAM_NAME" ILIKE \'%GSW%\' LIMIT 5;',
            ),
            (
                "unquoted TEAM_NAME ILIKE NYK",
                f'SELECT "TEAM_ID", "TEAM_NAME" FROM "{ta}" '
                f'WHERE TEAM_NAME ILIKE \'%NYK%\' LIMIT 5;',
            ),
            (
                "TEAM_ID = DEN abbrev",
                f'SELECT "TEAM_ID", "TEAM_NAME" FROM "{ta}" '
                f'WHERE "TEAM_ID" = \'DEN\' LIMIT 5;',
            ),
            (
                "TEAM_ID IN (LAL, BOS)",
                f'SELECT "TEAM_ID", "TEAM_NAME" FROM "{ta}" '
                f'WHERE "TEAM_ID" IN (\'LAL\', \'BOS\') LIMIT 10;',
            ),
        ]

        if ap:
            cases.append(
                (
                    "all_players ORDER BY fg3_pct (text sort fix)",
                    f'SELECT player_name, fg3_pct FROM "{ap}" '
                    f'WHERE fg3_pct IS NOT NULL AND fg3_pct::text <> \'\' '
                    f'ORDER BY fg3_pct DESC NULLS LAST LIMIT 5;',
                )
            )

        failed = 0
        for label, sql in cases:
            try:
                df = execute_query(conn, sql)
                n = len(df)
                ok = n > 0
                status = "OK" if ok else "EMPTY"
                print(f"[{status}] {label}  rows={n}")
                if not ok:
                    failed += 1
                elif n <= 5:
                    print(df.to_string(index=False))
                print()
            except Exception as e:
                failed += 1
                print(f"[ERR] {label}\n  {e}\n")

        print("--- summary ---")
        print(f"SQL cases: {len(cases)}  failed_or_empty: {failed}")
        sql_ok = failed == 0

    finally:
        conn.close()

    if args.nl:
        nl_fail = run_nl_checks()
        if not sql_ok:
            return 2
        return 2 if nl_fail > 0 else 0
    return 2 if not sql_ok else 0


if __name__ == "__main__":
    raise SystemExit(main())
