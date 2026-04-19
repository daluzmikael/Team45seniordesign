"""Smoke test /api/dashboards and /api/analysis. Run: python scripts/smoke_test_api.py"""
from __future__ import annotations

import json
import sys

if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
import time
import urllib.error
import urllib.request

BASE = "http://127.0.0.1:8000"
TIMEOUT = 180


def post(path: str, body: dict) -> tuple[int, dict]:
    data = json.dumps(body).encode()
    req = urllib.request.Request(
        BASE + path,
        data=data,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT) as r:
            return r.status, json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        raw = e.read().decode() or "{}"
        try:
            return e.code, json.loads(raw)
        except json.JSONDecodeError:
            return e.code, {"detail": raw[:500]}
    except Exception as ex:
        return -1, {"error": str(ex)}


def get_root() -> tuple[int, dict]:
    req = urllib.request.Request(BASE + "/")
    with urllib.request.urlopen(req, timeout=10) as r:
        return r.status, json.loads(r.read().decode())


def main() -> int:
    dashboards = [
        ("README minutes", "Who are the top 5 players with the most total minutes played in the 2024 season?"),
        ("README trend", "Show me the trend for Shai Gilgeous-Alexander's attempted free throws per year in his career"),
        ("README compare", "Show me a comparison of LeBron James and Kevin Durant points per game in the 2024 season"),
        ("README skill", "Show me the skill profile of Anthony Edwards and Shai Gilgeous-Alexander in 2024."),
        ("Leaderboard pts", "Top 10 scorers in the 2023-24 regular season"),
        ("Short", "Lakers wins 2024"),
    ]

    analysis = [
        ("README scorers", "top 5 scorers 2023"),
        ("README defenders", "top 5 defenders 2023"),
        ("README Jaylen", "what are jaylen brown's stats"),
        ("Rebounders", "who led the league in rebounds per game in 2023-24"),
        ("Playoffs", "Jayson Tatum playoff stats 2024"),
        ("One player season", "Giannis 2024 season"),
        ("Compare bigs", "Compare efficiency between Nikola Jokic and Joel Embiid in 2023-24"),
    ]

    fails = 0

    print("=== GET / ===")
    try:
        code, resp = get_root()
        print(f"  [{'OK' if code == 200 else 'FAIL'}] HTTP {code}  {resp}")
        if code != 200:
            fails += 1
    except Exception as e:
        print(f"  [FAIL] {e}")
        print("  Is the backend running?  uvicorn main:app --host 127.0.0.1 --port 8000")
        return 1

    print("\n=== POST /api/dashboards ===")
    for label, q in dashboards:
        t0 = time.time()
        code, resp = post("/api/dashboards", {"question": q})
        dt = time.time() - t0
        ok = code == 200 and resp.get("success") is True
        detail = ""
        if isinstance(resp, dict):
            detail = (resp.get("detail") or resp.get("error") or "")[:100]
        status = "OK" if ok else "FAIL"
        if not ok:
            fails += 1
        print(f"  [{status}] {label}  HTTP {code}  {dt:.1f}s  {detail}")

    print("\n=== POST /api/analysis ===")
    for label, q in analysis:
        t0 = time.time()
        code, resp = post("/api/analysis", {"question": q})
        dt = time.time() - t0
        ok = code == 200 and resp.get("success") is True
        if ok and isinstance(resp.get("analysis"), str) and len(resp["analysis"].strip()) < 15:
            ok = False
        detail = ""
        if isinstance(resp, dict):
            detail = (resp.get("detail") or "")[:90]
        status = "OK" if ok else "FAIL"
        if not ok:
            fails += 1
        preview = ""
        if isinstance(resp, dict) and resp.get("analysis"):
            preview = str(resp["analysis"])[:70].replace("\n", " ")
            preview = preview.encode("ascii", errors="replace").decode("ascii")
        print(f"  [{status}] {label}  HTTP {code}  {dt:.1f}s  {detail}")
        if preview:
            print(f"         -> {preview}...")

    print(f"\nDone. Failures: {fails}")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
