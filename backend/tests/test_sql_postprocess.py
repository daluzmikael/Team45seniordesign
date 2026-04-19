"""Tests for sql_postprocess.normalize_game_log_wl_column."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

# Allow `python -m unittest` from repo root or from backend/
_backend = Path(__file__).resolve().parent.parent
if str(_backend) not in sys.path:
    sys.path.insert(0, str(_backend))

from sql_postprocess import normalize_game_log_wl_column


class TestNormalizeGameLogWlColumn(unittest.TestCase):
    def test_no_player_game_logs_unchanged(self) -> None:
        sql = "SELECT * FROM all_players_regular_2023_2024 WHERE result = 'W'"
        self.assertEqual(normalize_game_log_wl_column(sql), sql)

    def test_case_when_result_becomes_wl(self) -> None:
        sql = (
            "SELECT CASE WHEN result = 'W' THEN 1 ELSE 0 END FROM player_game_logs LIMIT 5"
        )
        out = normalize_game_log_wl_column(sql)
        self.assertIn("CASE WHEN wl =", out)
        self.assertNotRegex(out, r"(?i)\bresult\s*=")

    def test_where_and_or_result(self) -> None:
        base = "SELECT 1 FROM player_game_logs WHERE "
        self.assertIn(
            "WHERE wl =",
            normalize_game_log_wl_column(base + "result = 'W'"),
        )
        self.assertIn(
            "AND wl =",
            normalize_game_log_wl_column(
                "SELECT 1 FROM player_game_logs WHERE x = 1 AND result = 'L'"
            ),
        )
        self.assertIn(
            "OR wl =",
            normalize_game_log_wl_column(
                "SELECT 1 FROM player_game_logs WHERE x = 1 OR result = 'W'"
            ),
        )


if __name__ == "__main__":
    unittest.main()
