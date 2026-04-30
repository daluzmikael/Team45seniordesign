"""
Edge-case tests for cross-season compare routing, SQL validation, and history heuristics.

Run from backend directory:
    python -m unittest discover -s tests -p "test_*.py" -v

Optional DB checks (skipped unless RUN_DB_EDGE_TESTS=1):
    set RUN_DB_EDGE_TESTS=1 && python -m unittest discover -s tests -p "test_*.py" -v

Unset RUN_DB_EDGE_TESTS when you want faster runs without hitting RDS.
"""

from __future__ import annotations

import os
import sys
import unittest

# Ensure backend package root is on path when tests are run from repo root.
_BACKEND_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _BACKEND_ROOT not in sys.path:
    sys.path.insert(0, _BACKEND_ROOT)

import pandas as pd

from Analyzer import query_analyzer as qa  # noqa: E402
from Executer.executor import validate_and_normalize_sql  # noqa: E402
from Interpreter import interpreter as intr  # noqa: E402


SCHEMA_MIN = """
player_pergame_regularseason_2012_2013(player, team, gp, pts, reb, ast, fg_pct, x, y, z)
player_pergame_regularseason_2015_2016(player, team, gp, pts, reb, ast, fg_pct, x, y, z)
advance_totals_regularseason_2012_2013(player, team, ts, usg, offrtg, defrtg, netrtg)
advance_totals_regularseason_2015_2016(player, team, ts, usg, offrtg, defrtg, netrtg)
defense_totals_regularseason_2012_2013(player, team, dreb, stl, blk, def_rtg, opp_pts)
defense_totals_regularseason_2015_2016(player, team, dreb, stl, blk, def_rtg, opp_pts)
"""


class TestParseCompareSide(unittest.TestCase):
    def test_two_part_names_with_year(self):
        n, y = intr._parse_compare_side_for_name_and_year("Lebron James 2012")
        self.assertEqual(y, 2012)
        self.assertIn("lebron", n.lower())
        self.assertIn("james", n.lower())

    def test_trailing_offensively(self):
        n, y = intr._parse_compare_side_for_name_and_year("Kevin durant 2015 offensively")
        self.assertEqual(y, 2015)
        self.assertIn("durant", n.lower())

    def test_last_year_wins(self):
        n, y = intr._parse_compare_side_for_name_and_year("Something 1999 extra 2015")
        self.assertEqual(y, 2015)
        self.assertIn("something", n.lower())

    def test_no_year(self):
        n, y = intr._parse_compare_side_for_name_and_year("Stephen Curry")
        self.assertIsNone(y)
        self.assertTrue(n)


class TestNormalizePlayerCandidate(unittest.TestCase):
    def test_strips_trailing_calendar_year(self):
        out = intr._normalize_player_candidate("LeBron James 2012")
        self.assertNotIn("2012", out)
        self.assertIn("James", out)

    def test_offensively_not_chopped(self):
        out = intr._normalize_player_candidate("durant offensively")
        self.assertNotIn("offensivel", out.lower())


class TestValidateUnion(unittest.TestCase):
    def test_union_all_accepted(self):
        sql = (
            "SELECT 1 AS a UNION ALL SELECT 2 AS a "
            'FROM public."player_pergame_regularseason_2012_2013" LIMIT 5'
        )
        # Note: intentionally odd shape still parses as UNION root in sqlglot.
        out = validate_and_normalize_sql(sql)
        self.assertIn("UNION", out.upper())

    def test_plain_select_still_ok(self):
        out = validate_and_normalize_sql('SELECT 1 AS x FROM public."player_pergame_regularseason_2012_2013" LIMIT 1')
        self.assertIn("SELECT", out.upper())


class TestBuildDynamicMultitable(unittest.TestCase):
    def _fake_join_sql(self) -> str:
        # No LIMIT on stub — LIMIT from stub is reused as UNION-wide cap in multitable builder.
        return """
        SELECT pg.player, adv.ts
        FROM player_pergame_regularseason_2025_2026 pg
        LEFT JOIN advance_totals_regularseason_2025_2026 adv
          ON pg.player = adv.player AND pg.team = adv.team
        """

    def test_cross_season_offensive_compare_union(self):
        q = "Compare Lebron James 2012 and Kevin durant 2015 offensively"
        sql = intr._build_dynamic_multitable_sql(self._fake_join_sql(), q, SCHEMA_MIN)
        low = (sql or "").lower()
        self.assertIn("union all", low)
        self.assertIn("2012_2013", sql)
        self.assertIn("2015_2016", sql)
        self.assertIn("season_start", low)

    def test_same_season_no_union_two_players(self):
        q = "Compare Lebron James 2012 and Kevin durant 2012 offensively"
        sql = intr._build_dynamic_multitable_sql(self._fake_join_sql(), q, SCHEMA_MIN)
        low = (sql or "").lower()
        self.assertNotIn("union all", low)
        self.assertIn("2012_2013", sql)

    def test_compare_regex_with_wrapped_question(self):
        wrapped = (
            "Use the recent conversation context below only to resolve references.\n\n"
            "Conversation context:\nUser: hi\n\n"
            "Current question: Compare Lebron James 2012 and Kevin durant 2015 offensively"
        )
        sql = intr._build_dynamic_multitable_sql(self._fake_join_sql(), wrapped, SCHEMA_MIN)
        self.assertIn("UNION ALL", sql.upper())

    def test_compare_versus_phrasing(self):
        q = "Compare Lebron James 2012 versus Kevin durant 2015 offensively"
        sql = intr._build_dynamic_multitable_sql(self._fake_join_sql(), q, SCHEMA_MIN)
        self.assertIn("UNION ALL", sql.upper())

    def test_compare_vs_phrasing(self):
        q = "Compare Lebron James 2012 vs Kevin durant 2015"
        sql = intr._build_dynamic_multitable_sql(self._fake_join_sql(), q, SCHEMA_MIN)
        self.assertIn("UNION ALL", sql.upper())

    def test_playoff_compare_skips_multitable_union(self):
        q = "Compare Lebron James 2012 and Kevin durant 2015 in the playoffs"
        raw = "SELECT 1 FROM player_pergame_regularseason_2025_2026 LIMIT 1"
        sql = intr._build_dynamic_multitable_sql(raw, q, SCHEMA_MIN)
        self.assertEqual(sql, raw)

    def test_playoff_compare_for_the_playoffs_skips_union(self):
        q = "Compare Lebron James 2012 and Kevin durant 2015 for the playoffs"
        raw = "SELECT 1 FROM player_pergame_regularseason_2025_2026"
        sql = intr._build_dynamic_multitable_sql(raw, q, SCHEMA_MIN)
        self.assertEqual(sql, raw)

    def test_who_was_better_or_phrasing_builds_union(self):
        q = "Who was better Lebron James 2012 or Kevin durant 2015 offensively"
        sql = intr._build_dynamic_multitable_sql(self._fake_join_sql(), q, SCHEMA_MIN)
        self.assertIn("UNION ALL", sql.upper())

    def test_which_one_is_better_or_phrasing_builds_union(self):
        q = "Which one is better Lebron James 2012 or Kevin durant 2015 offensively"
        sql = intr._build_dynamic_multitable_sql(self._fake_join_sql(), q, SCHEMA_MIN)
        self.assertIn("UNION ALL", sql.upper())

    def test_which_had_better_season_or_phrasing_builds_union(self):
        q = "Which had the better season: Lebron James 2012 or Kevin durant 2015?"
        sql = intr._build_dynamic_multitable_sql(self._fake_join_sql(), q, SCHEMA_MIN)
        self.assertIn("UNION ALL", sql.upper())

    def test_between_and_phrasing_builds_union(self):
        q = "Between Lebron James 2012 and Kevin durant 2015 offensively"
        sql = intr._build_dynamic_multitable_sql(self._fake_join_sql(), q, SCHEMA_MIN)
        self.assertIn("UNION ALL", sql.upper())


class TestCareerRewriterPreservesCrossSeasonCompare(unittest.TestCase):
    def test_preserves_union_when_two_distinct_years(self):
        q = "Compare Lebron James 2012 and Kevin durant 2015 offensively"
        union_sql = intr._build_dynamic_multitable_sql(
            """
            SELECT pg.player FROM player_pergame_regularseason_2025_2026 pg
            LEFT JOIN advance_totals_regularseason_2025_2026 adv
            ON pg.player = adv.player AND pg.team = adv.team
            """,
            q,
            SCHEMA_MIN,
        )
        self.assertIn("UNION ALL", union_sql.upper())
        out = intr._rewrite_career_aggregate_to_by_season(union_sql, q, conn=None)
        self.assertEqual(out.strip(), union_sql.strip())

    def test_preserves_union_who_was_better_or_two_years(self):
        q = "Who was better Lebron James 2012 or Kevin durant 2015 offensively"
        union_sql = intr._build_dynamic_multitable_sql(
            """
            SELECT pg.player FROM player_pergame_regularseason_2025_2026 pg
            LEFT JOIN advance_totals_regularseason_2025_2026 adv
            ON pg.player = adv.player AND pg.team = adv.team
            """,
            q,
            SCHEMA_MIN,
        )
        self.assertIn("UNION ALL", union_sql.upper())
        out = intr._rewrite_career_aggregate_to_by_season(union_sql, q, conn=None)
        self.assertEqual(out.strip(), union_sql.strip())


class TestBreakdownPullsDefenseJoin(unittest.TestCase):
    def test_breakdown_compare_includes_defense_totals_in_union(self):
        q = "Compare Lebron James 2012 and Kevin durant 2015 offensively — breakdown"
        sql = intr._build_dynamic_multitable_sql(
            """
            SELECT pg.player FROM player_pergame_regularseason_2025_2026 pg
            LEFT JOIN advance_totals_regularseason_2025_2026 adv
            ON pg.player = adv.player AND pg.team = adv.team
            """,
            q,
            SCHEMA_MIN,
        )
        self.assertIn("defn.", sql.lower())
        self.assertIn("defense_totals_regularseason_2012_2013".lower(), sql.lower())
        self.assertIn("defense_totals_regularseason_2015_2016".lower(), sql.lower())


class TestAnalyzerCompareBreakdown(unittest.TestCase):
    def test_should_format_breakdown_compare(self):
        df = pd.DataFrame(
            [
                {"player_name": "A", "season_start": 2012, "pts": 20.0, "opp_pts": 101.0},
                {"player_name": "B", "season_start": 2015, "pts": 25.0, "opp_pts": 99.0},
            ]
        )
        self.assertTrue(
            qa._should_format_compare_season_breakdown("Compare A vs B with a full breakdown", df)
        )

    def test_should_format_between_with_defense_columns(self):
        df = pd.DataFrame(
            [
                {"player_name": "A", "season_start": 2012, "pts": 20.0, "opp_pts": 101.0},
                {"player_name": "B", "season_start": 2015, "pts": 25.0, "opp_pts": 99.0},
            ]
        )
        self.assertTrue(
            qa._should_format_compare_season_breakdown("Between A and B in their peak seasons", df)
        )

    def test_should_format_rejects_three_player_rows(self):
        df = pd.DataFrame(
            [
                {"player_name": "A", "season_start": 2012, "pts": 20.0, "opp_pts": 101.0},
                {"player_name": "B", "season_start": 2015, "pts": 25.0, "opp_pts": 99.0},
                {"player_name": "C", "season_start": 2016, "pts": 22.0, "opp_pts": 100.0},
            ]
        )
        self.assertFalse(
            qa._should_format_compare_season_breakdown("Compare A vs B vs C — full breakdown", df)
        )

    def test_infer_domain_defense_when_opp_pts_compare(self):
        d = qa.infer_domain("Compare them in depth", ["player", "pts", "opp_pts", "def_rtg"])
        self.assertEqual(d, "defense")

    def test_infer_domain_who_was_better_with_opp_pts(self):
        d = qa.infer_domain("Who was better when both were at their peak", ["player", "opp_pts", "def_rtg"])
        self.assertEqual(d, "defense")

    def test_infer_domain_between_with_opp_pts(self):
        d = qa.infer_domain("Between these two seasons who held up better defensively", ["player", "opp_pts"])
        self.assertEqual(d, "defense")

    def test_infer_domain_who_was_better_or_with_opp_pts(self):
        d = qa.infer_domain("Who was better LeBron 2012 or Durant 2015", ["player", "opp_pts", "def_rtg"])
        self.assertEqual(d, "defense")

    def test_infer_domain_which_one_is_better_or_with_opp_pts(self):
        d = qa.infer_domain(
            "Which one is better LeBron 2012 or Durant 2015 defensively",
            ["player", "opp_pts", "def_rtg"],
        )
        self.assertEqual(d, "defense")

    def test_infer_domain_which_had_better_season_or_with_opp_pts(self):
        d = qa.infer_domain(
            "Which had the better season: LeBron 2012 or Durant 2015?",
            ["player", "opp_pts", "def_rtg"],
        )
        self.assertEqual(d, "defense")

    def test_format_compare_breakdown_renders_sections(self):
        df = pd.DataFrame(
            [
                {
                    "player_name": "LeBron James",
                    "team": "MIA",
                    "season_start": 2012,
                    "season_label": "2012-13",
                    "gp": 76,
                    "pts": 26.8,
                    "reb": 8.0,
                    "ast": 7.3,
                    "fg_pct": 0.565,
                    "ts": 64.0,
                    "def_rtg": 102.0,
                    "opp_pts": 99.5,
                },
                {
                    "player_name": "Kevin Durant",
                    "team": "OKC",
                    "season_start": 2015,
                    "season_label": "2015-16",
                    "gp": 72,
                    "pts": 28.2,
                    "reb": 8.2,
                    "ast": 5.0,
                    "fg_pct": 0.505,
                    "ts": 63.4,
                    "def_rtg": 103.0,
                    "opp_pts": 98.0,
                },
            ]
        )
        out = qa._format_compare_season_breakdown(df, "Compare them — full breakdown")
        self.assertIn("## Season comparison", out)
        self.assertIn("### LeBron James", out)
        self.assertIn("Defense totals profile", out)
        self.assertIn("At a glance", out)


class TestHistoryHeuristic(unittest.TestCase):
    def test_compare_and_players_does_not_trigger_history(self):
        import main as app_main

        q = "Compare Lebron James 2012 and Kevin durant 2015 offensively"
        self.assertFalse(app_main._should_apply_history_context(q))

    def test_who_is_better_or_does_not_trigger_history(self):
        import main as app_main

        q = "Who is better Lebron James 2012 or Kevin durant 2015"
        self.assertFalse(app_main._should_apply_history_context(q))

    def test_who_was_better_or_wrapped_no_history(self):
        import main as app_main

        wrapped = (
            "Conversation context:\nUser: random\n\n"
            "Current question: Who was better Lebron James 2012 or Kevin durant 2015?"
        )
        self.assertFalse(app_main._should_apply_history_context(wrapped))

    def test_which_one_is_better_or_does_not_trigger_history(self):
        import main as app_main

        q = "Which one is better Lebron James 2012 or Kevin durant 2015?"
        self.assertFalse(app_main._should_apply_history_context(q))

    def test_which_had_better_season_or_does_not_trigger_history(self):
        import main as app_main

        q = "Which had the better season: Lebron James 2012 or Kevin durant 2015?"
        self.assertFalse(app_main._should_apply_history_context(q))

    def test_who_is_bare_followup_still_uses_history(self):
        import main as app_main

        self.assertTrue(app_main._should_apply_history_context("who is better?"))

    def test_and_alone_in_compare_not_marker(self):
        import main as app_main

        # Bare "and " removed from markers — compare uses grammatical "and".
        self.assertFalse(app_main._should_apply_history_context("Compare A and B in 2020"))

    def test_and_also_still_followup(self):
        import main as app_main

        self.assertTrue(app_main._should_apply_history_context("and also show rebounds"))


@unittest.skipUnless(os.getenv("RUN_DB_EDGE_TESTS") == "1", "Set RUN_DB_EDGE_TESTS=1 to run DB smoke tests")
class TestDbSmokeCompare(unittest.TestCase):
    def test_end_to_end_union_executes_two_rows(self):
        from Executer.executor import execute_query, get_connection, get_db_schema

        conn = get_connection()
        try:
            schema = get_db_schema(conn)
            q = "Compare Lebron James 2012 and Kevin durant 2015 offensively"
            # Do not put LIMIT 1 on the stub — it becomes the UNION-wide LIMIT and drops a leg.
            fake = """
            SELECT pg.player, adv.ts FROM player_pergame_regularseason_2025_2026 pg
            LEFT JOIN advance_totals_regularseason_2025_2026 adv
            ON pg.player = adv.player AND pg.team = adv.team
            """
            built = intr._build_dynamic_multitable_sql(fake, q, schema)
            norm = validate_and_normalize_sql(built.rstrip().rstrip(";"))
            df = execute_query(conn, norm)
            self.assertGreaterEqual(len(df), 2, msg="Expected LeBron 2012 row + Durant 2015 row")
        finally:
            conn.close()


if __name__ == "__main__":
    unittest.main()
