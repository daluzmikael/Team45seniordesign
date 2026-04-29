import os
import logging
import re
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()
# Configure logging before importing executor: executor previously called basicConfig(INFO)
# first, which blocked this module's DEBUG config and hid logger.debug(...) lines.
_level_name = (os.getenv("LOG_LEVEL") or "INFO").upper()
_level = getattr(logging, _level_name, logging.INFO)
_fmt = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
if not logging.getLogger().handlers:
    logging.basicConfig(level=_level, format=_fmt)
else:
    # Uvicorn (or another host) configured the root logger; only adjust severity.
    logging.getLogger().setLevel(_level)

from Executer.executor import (
    get_connection,
    get_db_schema,
    #is_safe_sql,
    limit_rows,
    execute_query,
    validate_and_normalize_sql
)

logger = logging.getLogger(__name__)
api_key = os.getenv("OPENAI_API_KEY")
if not api_key:
    logger.warning("OpenAI API key missing from environment")
client = OpenAI(api_key=api_key)
_LAST_TABLES_USED: list[str] = []


def _extract_current_question_text(user_input: str) -> str:
    raw = (user_input or "").strip()
    if not raw:
        return ""
    # If the backend wrapped chat history, use only the explicit current-question segment
    # for intent/routing safeguards.
    match = re.search(r"(?is)\bcurrent question\s*:\s*(.+)$", raw)
    if match:
        return match.group(1).strip()
    return raw


def _is_single_player_profile_request(user_input: str) -> bool:
    q = _extract_current_question_text(user_input).lower()
    has_profile_intent = any(
        k in q for k in ["what were", " stats", "stat ", "show", "profile", "season stats"]
    )
    has_exclusions = any(
        k in q
        for k in [
            "top ",
            "best ",
            "highest",
            "most ",
            "leading ",
            "compare",
            "versus",
            " vs ",
            "between",
            "leaderboard",
            "by season",
            "per season",
            "trend",
            "over time",
            "over the years",
            "through the years",
            "decade",
            "rookie year",
            "from ",
            " to ",
            "career",
        ]
    )
    return has_profile_intent and not has_exclusions


def _extract_bare_year_request(user_input: str):
    q = _extract_current_question_text(user_input).lower()
    # If explicit season range is provided (e.g., 2024-25), do not override.
    if re.search(r"\b(19\d{2}|20\d{2})\s*[-/_]\s*(\d{2}|19\d{2}|20\d{2})\b", q):
        return None

    year_match = re.search(r"\b(19\d{2}|20\d{2})\b", q)
    if not year_match:
        return None

    year = int(year_match.group(1))
    is_playoffs = ("playoff" in q) or ("postseason" in q)
    return year, is_playoffs


_ORDINAL_WORDS = {
    "first": 1,
    "second": 2,
    "third": 3,
    "fourth": 4,
    "fifth": 5,
    "sixth": 6,
    "seventh": 7,
    "eighth": 8,
    "ninth": 9,
    "tenth": 10,
    "eleventh": 11,
    "twelfth": 12,
    "thirteenth": 13,
    "fourteenth": 14,
    "fifteenth": 15,
}


def _extract_requested_nth_season(user_input: str):
    q = _extract_current_question_text(user_input).lower()
    digit_match = re.search(r"\b(\d{1,2})(st|nd|rd|th)\s+(season|year)\b", q)
    if digit_match:
        try:
            n = int(digit_match.group(1))
            if n > 0:
                return n
        except Exception:
            pass

    for word, n in _ORDINAL_WORDS.items():
        if re.search(rf"\b{word}\s+(season|year)\b", q):
            return n
    return None


def _extract_nth_comparison_player_names(user_input: str):
    q = _extract_current_question_text(user_input)
    if not q:
        return []
    names = []

    def _normalize_candidate(raw: str):
        candidate = (raw or "").strip()
        candidate = re.sub(r"(?i)^(than|and|vs|versus|between|to|was|is|are|were|whether|who)\s+", "", candidate).strip()
        candidate = candidate.strip(" ,.;:!?")
        parts = candidate.split()
        if parts:
            last = parts[-1]
            if last.lower().endswith("s") and last.lower() not in {"james"} and len(last) > 3:
                parts[-1] = last[:-1]
            candidate = " ".join(parts)
        return candidate.strip()

    ordinal_words = "|".join(_ORDINAL_WORDS.keys())
    possessive_pattern = re.compile(
        rf"(?i)\b([A-Za-z][A-Za-z\.\-]*(?:\s+[A-Za-z][A-Za-z\.\-]*)?)(?:['’]s|s)?\s+((\d{{1,2}}(st|nd|rd|th))|({ordinal_words}))\s+(season|year)\b"
    )
    for m in possessive_pattern.finditer(q):
        candidate = _normalize_candidate(m.group(1) or "")
        if candidate:
            names.append(candidate)

    if len(names) < 2:
        than_pattern = re.search(
            r"(?i)\b([A-Za-z][A-Za-z\.\-]*(?:\s+[A-Za-z][A-Za-z\.\-]*)?)\b.*?\bthan\b\s+([A-Za-z][A-Za-z\.\-]*(?:\s+[A-Za-z][A-Za-z\.\-]*)?)\b",
            q,
        )
        if than_pattern:
            names.extend([
                _normalize_candidate(than_pattern.group(1)),
                _normalize_candidate(than_pattern.group(2)),
            ])

    # Deduplicate while preserving order.
    deduped = []
    seen = set()
    for name in names:
        key = name.lower()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(name)
    return deduped


def _physical_regular_summary_table(start: int, end: int) -> str:
    return f"player_pergame_regularseason_{start}_{end}"


def _physical_playoff_summary_table(start: int, end: int) -> str:
    return f"player_totals_playoffseason_{start}_{end}"


def _strip_rank_like_columns_from_select(fragment: str) -> str:
    """Remove legacy *_rank columns from SELECT lists (not present on NBA-STATS summary tables)."""
    m = re.search(r"(?is)\bselect\b(?P<sel>.*?)\bfrom\b", fragment)
    if not m:
        return fragment
    sel = m.group("sel")
    sel2 = re.sub(r",?\s*\b[a-z0-9_]*_rank\b", "", sel, flags=re.IGNORECASE)
    if sel2 == sel:
        return fragment
    return fragment[: m.start("sel")] + sel2 + fragment[m.end("sel") :]


def _apply_pergame_column_aliases(fragment: str) -> str:
    out = fragment
    pairs = [
        (r"\bplayer_name\b", "player"),
        (r"\bteam_abbreviation\b", "team"),
        (r"\bplus_minus\b", '"+/-"'),
        (r"\bdd2\b", "double_double"),
        (r"\btd3\b", "triple_double"),
        (r"\bfg3_pct\b", '"3p%"'),
        (r"\bfg3a\b", '"3pa"'),
        (r"\bfg3m\b", '"3pm"'),
        (r"\bfg_pct\b", '"fg%"'),
        (r"\bft_pct\b", '"ft%"'),
    ]
    for pat, repl in pairs:
        out = re.sub(pat, repl, out, flags=re.IGNORECASE)
    return _strip_rank_like_columns_from_select(out)


def _apply_totals_column_aliases(fragment: str) -> str:
    out = fragment
    pairs = [
        (r"\bplayer_name\b", "player"),
        (r"\bteam_abbreviation\b", "team"),
        (r"\bfg3_pct\b", "c_3p"),
        (r"\bfg3a\b", "c_3pa"),
        (r"\bfg3m\b", "c_3pm"),
        (r"\bfg_pct\b", "fg"),
        (r"\bft_pct\b", "ft"),
    ]
    for pat, repl in pairs:
        out = re.sub(pat, repl, out, flags=re.IGNORECASE)
    return _strip_rank_like_columns_from_select(out)


def _map_sql_fragment_to_physical_columns(fragment: str) -> str:
    low = (fragment or "").lower()
    if "advance_totals_" in low:
        out = re.sub(r"\bplayer_name\b", "player", fragment, flags=re.IGNORECASE)
        out = re.sub(r"\bteam_abbreviation\b", "team", out, flags=re.IGNORECASE)
        out = re.sub(r"\bts_pct\b", "ts", out, flags=re.IGNORECASE)
        return out
    if "player_totals_playoffseason_" in low or "player_totals_regularseason_" in low:
        return _apply_totals_column_aliases(fragment)
    if "player_pergame_regularseason_" in low:
        return _apply_pergame_column_aliases(fragment)
    return fragment


def _canonicalize_nba_stats_sql(sql_query: str) -> str:
    """
    Map legacy prompt/table names to physical NBA-STATS tables and column identifiers.
    Safe to run multiple times (idempotent for already-canonical SQL).
    """
    if not sql_query:
        return sql_query
    q = sql_query
    q = re.sub(
        r"(?i)\ball_players_regular_(\d{4})_(\d{4})\b",
        r"player_pergame_regularseason_\1_\2",
        q,
    )
    q = re.sub(
        r"(?i)\ball_players_playoffs_(\d{4})_(\d{4})\b",
        r"player_totals_playoffseason_\1_\2",
        q,
    )
    # Canonicalize legacy clutch aliases to a family marker; actual season table is
    # resolved later against live schema.
    q = re.sub(r"(?i)\bnba__clutch__\d{4}_\d{4}\b", "clutch_totals_regularseason_", q)
    q = re.sub(r"(?i)\bnba_clutch_stats\b", "clutch_totals_regularseason_", q)
    q = re.sub(r"(?i)\bclutch_stats\b", "clutch_totals_regularseason_", q)
    parts = re.split(r"(?i)\bunion\s+all\b", q)
    mapped = [_map_sql_fragment_to_physical_columns(p) for p in parts]
    return " UNION ALL ".join(mapped)


def _normalize_table_family_name(table_name: str) -> str:
    """Collapse year-suffixed physical tables into a stable family key."""
    return re.sub(r"_(\d{4})_(\d{4})$", "", (table_name or "").strip())


def _build_schema_routing_guide(schema_description: str) -> str:
    """
    Build a compact routing guide from live information_schema output so the
    model can map associated words to real columns and table families.
    """
    if not schema_description:
        return ""

    family_to_columns: dict[str, set[str]] = {}
    for raw_line in schema_description.splitlines():
        line = (raw_line or "").strip()
        if not line or "(" not in line or ")" not in line:
            continue
        m = re.match(r"^([a-zA-Z0-9_]+)\((.*)\)$", line)
        if not m:
            continue
        table_name = m.group(1).strip()
        cols_blob = m.group(2).strip()
        columns = [c.strip() for c in cols_blob.split(",") if c.strip()]
        if not columns:
            continue
        family = _normalize_table_family_name(table_name)
        family_to_columns.setdefault(family, set()).update(columns)

    if not family_to_columns:
        return ""

    preferred_families = [
        "player_pergame_regularseason",
        "player_totals_regularseason",
        "player_totals_playoffseason",
        "advance_totals_regularseason",
        "advance_totals_playoffseason",
        "defense_totals_regularseason",
        "court_shots",
    ]

    keyword_targets = {
        "points/scoring": ["pts"],
        "rebounds/boards": ["reb", "oreb", "dreb"],
        "assists/playmaking": ["ast", "ast_to", "ast_ratio"],
        "steals/defense": ["stl", "blk", "defrtg", "netrtg"],
        "turnovers/ball security": ["tov", "to_ratio"],
        "shooting efficiency": ["fg%", "3p%", "ft%", "fg", "c_3p", "ft", "efg", "ts"],
        "volume shooting": ["fga", "3pa", "fta", "fg3a"],
        "usage/workload": ["usg", "min", "gp", "pace", "poss"],
        "lineup impact": ["netrtg", "offrtg", "defrtg", "poss"],
        "shot location": ["shot_zone_basic", "shot_zone_area", "loc_x", "loc_y", "shot_made_flag"],
        "schedule/standings context": ["game_date", "matchup", "wl", "season_type", "season_id"],
    }

    def _families_for_column(col: str) -> list[str]:
        matches = [f for f, cols in family_to_columns.items() if col in cols]
        return sorted(matches)

    lines: list[str] = []
    lines.append("LIVE SCHEMA ROUTING GUIDE (AUTO-GENERATED FROM DATABASE SCHEMA):")
    lines.append("- Use this mapping before guessing columns. If a requested stat term appears here, route to these families first.")
    lines.append("- Always verify final table+column names against DATABASE SCHEMA below when there is any ambiguity.")
    lines.append("")
    lines.append("Keyword → candidate columns → table families:")

    for label, cols in keyword_targets.items():
        family_hits: list[str] = []
        for col in cols:
            family_hits.extend(_families_for_column(col))
        deduped_hits = sorted(set(family_hits))
        if not deduped_hits:
            continue
        show_cols = ", ".join(cols)
        show_fams = ", ".join(deduped_hits[:8])
        suffix = " ..." if len(deduped_hits) > 8 else ""
        lines.append(f"- {label}: [{show_cols}] -> {show_fams}{suffix}")

    nba_families = sorted(f for f in family_to_columns if f.startswith("nba__"))
    if nba_families:
        lines.append("")
        lines.append("NBA__ families discovered in schema:")
        lines.append("- " + ", ".join(nba_families))

    lines.append("")
    lines.append("Family column snapshots (real columns discovered):")
    shown = set()
    for fam in preferred_families + nba_families[:5]:
        if fam not in family_to_columns or fam in shown:
            continue
        shown.add(fam)
        cols = sorted(family_to_columns[fam])
        lines.append(f"- {fam}: " + ", ".join(cols[:24]))

    return "\n".join(lines)


def _extract_tables_from_sql(sql_query: str) -> list[str]:
    """
    Extract table names referenced by FROM/JOIN so we can print routing visibility
    in the backend terminal for each user question.
    """
    if not sql_query:
        return []
    q = re.sub(r"\s+", " ", sql_query)
    matches = re.findall(
        r"(?i)\b(?:from|join)\s+(?:public\.)?(?:\"([^\"]+)\"|([a-zA-Z_][a-zA-Z0-9_]*))",
        q,
    )
    tables: list[str] = []
    seen = set()
    for quoted_name, bare_name in matches:
        name = (quoted_name or bare_name or "").strip()
        if not name:
            continue
        key = name.lower()
        if key in seen:
            continue
        seen.add(key)
        tables.append(name)
    return tables


def get_last_tables_used() -> list[str]:
    return list(_LAST_TABLES_USED)


def _available_season_starts(conn, table_type: str):
    try:
        cursor = conn.cursor()
        if table_type == "regular":
            like_patterns = ("player_pergame_regularseason_%", "all_players_regular_%")
        else:
            like_patterns = ("player_totals_playoffseason_%", "all_players_playoffs_%")
        starts = []
        for like_pat in like_patterns:
            cursor.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                  AND table_name LIKE %s
                ORDER BY table_name ASC;
                """,
                (like_pat,),
            )
            for (table_name,) in cursor.fetchall():
                tn = table_name or ""
                if table_type == "regular":
                    m = re.match(
                        r"(?:player_pergame_regularseason|all_players_regular)_(\d{4})_(\d{4})$",
                        tn,
                    )
                else:
                    m = re.match(
                        r"(?:player_totals_playoffseason|all_players_playoffs)_(\d{4})_(\d{4})$",
                        tn,
                    )
                if m:
                    starts.append(int(m.group(1)))
        return sorted(set(starts))
    except Exception:
        return []


def _first_season_start_for_where(conn, table_type: str, where_clause: str, starts: list[int]):
    if not where_clause or not starts:
        return None
    cleaned_where = where_clause.strip().rstrip(";")
    cleaned_where = re.sub(r"(?i)\bplayer_name\b", "player", cleaned_where)
    for start in starts:
        end = start + 1
        if table_type == "playoffs":
            table_name = _physical_playoff_summary_table(start, end)
        else:
            table_name = _physical_regular_summary_table(start, end)
        sql = f'SELECT 1 FROM public."{table_name}" WHERE {cleaned_where} LIMIT 1'
        try:
            cursor = conn.cursor()
            cursor.execute(sql)
            if cursor.fetchone() is not None:
                return start
        except Exception:
            continue
    return None


def _latest_season_start_for_where(conn, table_type: str, where_clause: str, starts: list[int]):
    if not where_clause or not starts:
        return None
    cleaned_where = where_clause.strip().rstrip(";")
    cleaned_where = re.sub(r"(?i)\bplayer_name\b", "player", cleaned_where)
    for start in sorted(starts, reverse=True):
        end = start + 1
        if table_type == "playoffs":
            table_name = _physical_playoff_summary_table(start, end)
        else:
            table_name = _physical_regular_summary_table(start, end)
        sql = f'SELECT 1 FROM public."{table_name}" WHERE {cleaned_where} LIMIT 1'
        try:
            cursor = conn.cursor()
            cursor.execute(sql)
            if cursor.fetchone() is not None:
                return start
        except Exception:
            continue
    return None


def _has_explicit_year_or_season_ref(user_input: str) -> bool:
    q = _extract_current_question_text(user_input).lower()
    if re.search(r"\b(19\d{2}|20\d{2})\b", q):
        return True
    if re.search(r"\b(19\d{2}|20\d{2})\s*[-/_]\s*(\d{2}|19\d{2}|20\d{2})\b", q):
        return True
    if any(k in q for k in ["last season", "this season", "current season", "last playoff", "this playoff", "current playoff"]):
        return True
    return False


def _enforce_no_year_playoff_player_fallback(sql_query: str, user_input: str, conn) -> str:
    """
    For no-year playoff player questions, use the latest playoff season where
    the requested player actually appears.
    """
    if not sql_query or conn is None:
        return sql_query
    q = _extract_current_question_text(user_input).lower()
    if "playoff" not in q and "postseason" not in q:
        return sql_query
    if _has_explicit_year_or_season_ref(user_input):
        return sql_query

    named_players = _extract_named_players(user_input, sql_query)
    if not named_players:
        return sql_query
    where_clause = _player_where_clause_for_names(named_players)
    if not where_clause:
        return sql_query

    playoff_starts = _available_season_starts(conn, "playoffs")
    if not playoff_starts:
        return sql_query
    latest_start = _latest_season_start_for_where(conn, "playoffs", where_clause, playoff_starts)
    if latest_start is None:
        return sql_query
    target_table = _physical_playoff_summary_table(latest_start, latest_start + 1)
    return re.sub(
        r"(?i)(?:player_totals_playoffseason|all_players_playoffs)_\d{4}_\d{4}",
        target_table,
        sql_query,
    )


def _is_recency_request(user_input: str) -> bool:
    q = _extract_current_question_text(user_input).lower()
    return any(
        k in q
        for k in ["lately", "recent", "last ", "game log", "game by game", "hot streak", "cold streak", "how has", "been playing"]
    )


def _build_regular_summary_fallback_sql(user_input: str, conn) -> str | None:
    names = _extract_named_players(user_input, "")
    where_clause = _player_where_clause_for_names(names) if names else ""
    starts = _available_season_starts(conn, "regular")
    if not starts:
        return None
    start = max(starts)
    end = start + 1
    table = _physical_regular_summary_table(start, end)
    where_sql = f"WHERE {where_clause}" if where_clause else ""
    return (
        'SELECT DISTINCT player, team, gp, pts, reb, ast, fgm, fga, "fg%", "3pm", "3pa", "3p%", ftm, fta, "ft%" '
        f'FROM public."{table}" {where_sql} LIMIT 50'
    )


def _build_team_record_fallback_sql(user_input: str, conn) -> str | None:
    if not _is_team_record_request(user_input):
        return None
    q = _extract_current_question_text(user_input).lower()
    is_playoffs = "playoff" in q or "postseason" in q
    prefix = "teams_pergame_playoffseason_" if is_playoffs else "teams_pergame_regularseason_"
    team_candidates: list[str] = []
    for key, aliases in _TEAM_ALIASES.items():
        if key in q:
            team_candidates.extend(aliases)
    if not team_candidates:
        return None
    where_clause = " OR ".join([f"\"Team\" ILIKE '%{a}%'" for a in dict.fromkeys(team_candidates)])
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema='public' AND table_name LIKE %s
            ORDER BY table_name DESC
            """,
            (f"{prefix}%",),
        )
        tables = [r[0] for r in cur.fetchall()]
        for t in tables:
            cur.execute(
                f'SELECT 1 FROM public."{t}" WHERE ({where_clause}) AND "GP" IS NOT NULL LIMIT 1'
            )
            if cur.fetchone():
                return (
                    f'SELECT "Team", "GP", "W", "L", "WIN%", "PTS", "FG%", "3P%", "FT%", "+/-" '
                    f'FROM public."{t}" WHERE ({where_clause}) ORDER BY "GP" DESC NULLS LAST LIMIT 1'
                )
    except Exception:
        return None
    return None


def _enforce_nth_season_table_mapping(sql_query: str, user_input: str, conn) -> str:
    if not sql_query:
        return sql_query
    nth = _extract_requested_nth_season(user_input)
    if not nth:
        return sql_query

    q = sql_query
    regular_starts = _available_season_starts(conn, "regular")
    playoffs_starts = _available_season_starts(conn, "playoffs")

    leg_pattern = re.compile(
        r"(?is)(select\s+)(?P<select_part>.*?)(\s+from\s+)"
        r"(?P<table_name>(?:player_pergame_regularseason|player_totals_playoffseason|player_totals_regularseason|all_players_(?:regular|playoffs))_(?P<start>\d{4})_(?P<end>\d{4}))"
        r"(?P<rest>\s+where\s+.*?)(?=(\bunion\s+all\b|$))"
    )

    def _rewrite_leg(match: re.Match):
        select_part = match.group("select_part")
        table_name_full = (match.group("table_name") or "").lower()
        if "playoffseason" in table_name_full or (
            "all_players_" in table_name_full and "playoffs" in table_name_full
        ):
            table_type = "playoffs"
        else:
            table_type = "regular"
        rest = match.group("rest") or ""

        where_match = re.search(
            r"(?is)\bwhere\b\s*(?P<where>.*?)(\bgroup\s+by\b|\border\s+by\b|\blimit\b|$)",
            rest,
        )
        if not where_match:
            return match.group(0)

        where_clause = (where_match.group("where") or "").strip()
        if not where_clause:
            return match.group(0)

        starts = regular_starts if table_type == "regular" else playoffs_starts
        first_start = _first_season_start_for_where(conn, table_type, where_clause, starts)
        if first_start is None:
            return match.group(0)

        target_start = first_start + (nth - 1)
        if target_start not in starts:
            return match.group(0)
        target_end = target_start + 1
        if table_type == "playoffs":
            target_table = _physical_playoff_summary_table(target_start, target_end)
        else:
            target_table = _physical_regular_summary_table(target_start, target_end)
        season_label = f"{target_start}-{str(target_end)[-2:]}"

        updated_select = select_part
        if not re.search(r"(?i)\bseason_label\b", updated_select):
            updated_select = updated_select.rstrip() + f", '{season_label}' AS season_label"
        if not re.search(r"(?i)\bseason_start\b", updated_select):
            updated_select = updated_select.rstrip() + f", {target_start} AS season_start"

        return (
            f"{match.group(1)}{updated_select}{match.group(3)}{target_table}{rest}"
        )

    q = re.sub(leg_pattern, _rewrite_leg, q)
    return q


def _rewrite_nth_season_comparison_sql(sql_query: str, user_input: str, conn) -> str:
    if re.search(r"(?i)\bjoin\b", sql_query or ""):
        return sql_query
    nth = _extract_requested_nth_season(user_input)
    if not nth:
        return sql_query

    q_input = _extract_current_question_text(user_input).lower()
    is_comparison = any(k in q_input for k in ["compare", "better than", "versus", " vs ", "between", " than "])
    if not is_comparison:
        return sql_query

    player_names = _extract_nth_comparison_player_names(user_input)
    if len(player_names) < 2:
        return sql_query

    regular_starts = _available_season_starts(conn, "regular")
    if not regular_starts:
        return sql_query

    legs = []
    for player_name in player_names[:2]:
        safe_name = player_name.replace("'", "''")
        where_clause = f"player ILIKE '%{safe_name}%'"
        first_start = _first_season_start_for_where(conn, "regular", where_clause, regular_starts)
        if first_start is None:
            continue
        target_start = first_start + (nth - 1)
        if target_start not in regular_starts:
            continue
        target_end = target_start + 1
        season_label = f"{target_start}-{str(target_end)[-2:]}"
        table_name = _physical_regular_summary_table(target_start, target_end)
        legs.append(
            "SELECT DISTINCT "
            f"{target_start} AS season_start, '{season_label}' AS season_label, "
            'player, pts, reb, ast, "fg%", "3p%", "ft%", gp '
            f'FROM public."{table_name}" WHERE {where_clause}'
        )

    if len(legs) < 2:
        return sql_query
    return (
        'SELECT season_start, season_label, player, pts, reb, ast, "fg%", "3p%", "ft%", gp '
        f"FROM ({' UNION ALL '.join(legs)}) AS comparison LIMIT 50;"
    )


def _enforce_start_year_table_mapping(sql_query: str, user_input: str) -> str:
    if not sql_query:
        return sql_query
    if " union " in sql_query.lower():
        return sql_query

    req = _extract_bare_year_request(user_input)
    if req is None:
        return sql_query
    year, is_playoffs = req

    if is_playoffs:
        start = year - 1
        end = year
        target = _physical_playoff_summary_table(start, end)
    else:
        # Bare regular-season year maps to season START year.
        # Example: "2020 season" -> 2020_2021.
        start = year
        end = year + 1
        target = _physical_regular_summary_table(start, end)

    summary_pat = (
        r"(?i)(?:all_players_(?:regular|playoffs)_\d{4}_\d{4}|"
        r"player_pergame_regularseason_\d{4}_\d{4}|"
        r"player_totals_playoffseason_\d{4}_\d{4}|"
        r"player_totals_regularseason_\d{4}_\d{4})"
    )
    return re.sub(summary_pat, target, sql_query)


def _enforce_current_regular_season_default(sql_query: str, user_input: str) -> str:
    """
    If a regular-season intent has no explicit year/season reference, force current
    season summary tables to 2025_2026.
    """
    if not sql_query:
        return sql_query
    q = _extract_current_question_text(user_input).lower()
    has_regular_intent = any(
        k in q
        for k in [
            "regular season",
            "season performance",
            "season stats",
            "season averages",
            "this season",
            "current season",
        ]
    )
    has_explicit_time = (
        re.search(r"\b(19\d{2}|20\d{2})\b", q) is not None
        or re.search(r"\b(19\d{2}|20\d{2})\s*[-/_]\s*(\d{2}|19\d{2}|20\d{2})\b", q) is not None
        or "last season" in q
        or "last year" in q
        or "playoff" in q
        or "postseason" in q
    )
    if not has_regular_intent or has_explicit_time:
        return sql_query

    out = sql_query
    out = re.sub(
        r"(?i)player_pergame_regularseason_\d{4}_\d{4}",
        "player_pergame_regularseason_2025_2026",
        out,
    )
    out = re.sub(
        r"(?i)player_totals_regularseason_\d{4}_\d{4}",
        "player_totals_regularseason_2025_2026",
        out,
    )
    out = re.sub(
        r"(?i)all_players_regular_\d{4}_\d{4}",
        "player_pergame_regularseason_2025_2026",
        out,
    )
    out = re.sub(
        r"(?i)advance_totals_regularseason_\d{4}_\d{4}",
        "advance_totals_regularseason_2025_2026",
        out,
    )
    out = re.sub(
        r"(?i)defense_totals_regularseason_\d{4}_\d{4}",
        "defense_totals_regularseason_2025_2026",
        out,
    )
    return out


def _is_advanced_metrics_request(user_input: str) -> bool:
    q = _extract_current_question_text(user_input).lower()
    advanced_terms = [
        "true shooting",
        "ts%",
        "ts pct",
        "ts_pct",
        "efg",
        "efg%",
        "usage rate",
        "usg",
        "off rating",
        "def rating",
        "net rating",
        "pie",
        "advanced stat",
        "advanced metric",
        # Common phrasing/typo variants from user prompts.
        "advanced stats",
        "advanced metrics",
        "advance stats",
        "advance metric",
        "advance metrics",
        "advances stats",
        "advances metric",
        "advances metrics",
    ]
    return any(term in q for term in advanced_terms)


def _extract_requested_season_window(user_input: str):
    q = _extract_current_question_text(user_input).lower()
    is_playoffs = ("playoff" in q) or ("postseason" in q)

    range_match = re.search(r"\b(19\d{2}|20\d{2})\s*[-/_]\s*(\d{2}|19\d{2}|20\d{2})\b", q)
    if range_match:
        start = int(range_match.group(1))
        end_raw = range_match.group(2)
        if len(end_raw) == 2:
            end = int(f"{str(start)[:2]}{end_raw}")
            if end < start:
                end += 100
        else:
            end = int(end_raw)
        return start, end, is_playoffs

    bare = _extract_bare_year_request(user_input)
    if bare is not None:
        year, playoffs_flag = bare
        if playoffs_flag:
            return year - 1, year, True
        # Bare regular-season year maps to season START year.
        # Example: "2020 season" -> 2020_2021.
        return year, year + 1, False

    if "this season" in q or "current season" in q:
        return 2025, 2026, False
    if "last season" in q:
        return 2024, 2025, False
    if "this playoff" in q or "current playoff" in q:
        # Playoffs are discrete and may not exist yet for the in-progress regular season.
        # Route default playoff asks to the latest completed playoffs table.
        return 2024, 2025, True
    if "last playoff" in q:
        return 2024, 2025, True

    return 2025, 2026, is_playoffs


def _player_pergame_regularseason_columns() -> list[str]:
    """Column identifiers on player_pergame_regularseason_* (NBA-STATS)."""
    return [
        "player",
        "team",
        "age",
        "gp",
        "w",
        "l",
        "min",
        "pts",
        "fgm",
        "fga",
        '"fg%"',
        '"3pm"',
        '"3pa"',
        '"3p%"',
        "ftm",
        "fta",
        '"ft%"',
        "oreb",
        "dreb",
        "reb",
        "ast",
        "tov",
        "stl",
        "blk",
        "pf",
        "fp",
        "double_double",
        "triple_double",
        '"+/-"',
    ]


def _player_totals_season_columns() -> list[str]:
    """Column identifiers on player_totals_*season_* (NBA-STATS)."""
    return [
        "player",
        "team",
        "age",
        "gp",
        "w",
        "l",
        "min",
        "pts",
        "fgm",
        "fga",
        "fg",
        "c_3pm",
        "c_3pa",
        "c_3p",
        "ftm",
        "fta",
        "ft",
        "oreb",
        "dreb",
        "reb",
        "ast",
        "tov",
        "stl",
        "blk",
        "pf",
        "fp",
        "dd2",
        "td3",
    ]


def _all_players_full_row_columns() -> list[str]:
    """Backward-compatible name: full row slice uses per-game regular-season layout."""
    return _player_pergame_regularseason_columns()


_PLAYER_ALIAS_MAP = {
    "lebron": ["LeBron James"],
    "lebron james": ["LeBron James"],
    "kobe": ["Kobe Bryant"],
    "kobe bryant": ["Kobe Bryant"],
    "embiid": ["Joel Embiid"],
    "joel embiid": ["Joel Embiid"],
    "steph": ["Stephen Curry", "Steph Curry"],
    "steph curry": ["Stephen Curry", "Steph Curry"],
    "jokic": ["Nikola Jokic", "Jokic", "Jokić"],
    "nikola jokic": ["Nikola Jokic", "Jokic", "Jokić"],
    "giannis": ["Giannis Antetokounmpo", "Antetokounmpo", "Giannis"],
    "giannis antetokounmpo": ["Giannis Antetokounmpo", "Antetokounmpo", "Giannis"],
    "bron": ["LeBron James"],
    "king james": ["LeBron James"],
    "greek freak": ["Giannis Antetokounmpo", "Giannis"],
    "kd": ["Kevin Durant"],
    "ad": ["Anthony Davis"],
    "kawhi": ["Kawhi Leonard"],
    "cp3": ["Chris Paul"],
    "dame": ["Damian Lillard"],
    "russ": ["Russell Westbrook"],
    "pg": ["Paul George"],
    "pg13": ["Paul George"],
    "luka": ["Luka Doncic", "Luka Dončić", "Doncic", "Dončić"],
    "doncic": ["Luka Doncic", "Luka Dončić", "Doncic", "Dončić"],
    "luka doncic": ["Luka Doncic", "Luka Dončić", "Doncic", "Dončić"],
    "luka dončić": ["Luka Doncic", "Luka Dončić", "Doncic", "Dončić"],
}


def _normalize_player_candidate(raw: str) -> str:
    candidate = (raw or "").strip()
    candidate = re.sub(
        r"(?i)^(than|and|vs|versus|between|to|from|was|is|are|were|whether|who|what|show|me|did|does|do)\s+",
        "",
        candidate,
    ).strip()
    candidate = re.sub(
        r"(?i)\b(in|during|for|from|through|thru|to|over|by|per|the|playoffs?|postseason|"
        r"this|current|last|season|seasons|using|with|show|stats?|points?|rebounds?|assists?|"
        r"steals?|blocks?|ts%|true|shooting)\b.*$",
        "",
        candidate,
    ).strip()
    candidate = candidate.strip(" ,.;:!?")
    if not candidate:
        return ""
    parts = [p for p in candidate.split() if p]
    if parts:
        last = parts[-1]
        if last.lower().endswith("s") and last.lower() not in {"james"} and len(last) > 3:
            parts[-1] = last[:-1]
    return " ".join(parts).strip()


def _extract_player_names_from_question(question_text: str) -> list[str]:
    q = (question_text or "").strip()
    if not q:
        return []

    pair_patterns = [
        re.compile(r"(?i)\bcompare\s+(.+?)\s+(?:and|vs|versus)\s+(.+?)(?:\bfrom\b|\bin\b|\bduring\b|$)"),
        re.compile(r"(?i)\bbetween\s+(.+?)\s+and\s+(.+?)(?:\bfrom\b|\bin\b|\bduring\b|$)"),
        re.compile(r"(?i)\b(.+?)\s+\bthan\b\s+(.+?)(?:\bfrom\b|\bin\b|\bduring\b|$)"),
    ]
    pair_names: list[str] = []
    for pattern in pair_patterns:
        match = pattern.search(q)
        if not match:
            continue
        for idx in (1, 2):
            candidate = _normalize_player_candidate(match.group(idx))
            if candidate:
                pair_names.append(candidate)
        break
    if len(pair_names) >= 2:
        dedup_pair: list[str] = []
        seen_pair = set()
        for name in pair_names:
            key = name.lower()
            if key in seen_pair:
                continue
            seen_pair.add(key)
            dedup_pair.append(name)
        if len(dedup_pair) >= 2:
            return dedup_pair

    names: list[str] = []
    lower_q = q.lower()
    for alias, expanded in _PLAYER_ALIAS_MAP.items():
        if re.search(rf"(?i)\b{re.escape(alias)}\b", q):
            if expanded:
                names.append(expanded[0])

    possessive_pattern = re.compile(
        r"(?i)\b([A-Za-z][A-Za-z\.\-]*(?:\s+[A-Za-z][A-Za-z\.\-]*){0,2})(?:['’]s)\b"
    )
    for match in possessive_pattern.finditer(q):
        candidate = _normalize_player_candidate(match.group(1))
        if candidate:
            names.append(candidate)

    capitalized_pattern = re.compile(r"\b([A-Z][a-zA-Z\.\-]+(?:\s+[A-Z][a-zA-Z\.\-]+){1,2})\b")
    for match in capitalized_pattern.finditer(q):
        candidate = _normalize_player_candidate(match.group(1))
        if candidate:
            names.append(candidate)

    if not names and re.search(r"(?i)\b(player|players)\b", lower_q):
        return []

    deduped: list[str] = []
    seen = set()
    for name in names:
        key = name.lower()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(name)
    return deduped


def _extract_player_names_from_sql(sql_query: str) -> list[str]:
    q = sql_query or ""
    matches = [
        m.strip()
        for m in re.findall(r"(?i)(?:player|player_name)\s+ILIKE\s+'%([^%']+)%'", q)
        if m and m.strip()
    ]
    if not matches:
        return []

    multi_word = [m for m in matches if " " in m.strip()]
    preferred = multi_word if multi_word else matches

    deduped: list[str] = []
    seen = set()
    for name in preferred:
        key = name.lower()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(name)
    return deduped


def _extract_named_players(user_input: str, sql_query: str) -> list[str]:
    question_text = _extract_current_question_text(user_input)
    names = _extract_player_names_from_question(question_text)
    if not names:
        names = _extract_player_names_from_sql(sql_query)
    return names


def _player_where_clause_for_names(player_names: list[str]) -> str:
    clauses: list[str] = []
    for raw_name in player_names:
        variants = _PLAYER_ALIAS_MAP.get(raw_name.lower(), [raw_name])
        expanded_variants = []
        for variant in variants:
            if variant not in expanded_variants:
                expanded_variants.append(variant)
        subclauses = []
        for variant in expanded_variants:
            safe_name = variant.replace("'", "''")
            subclauses.append(f"player ILIKE '%{safe_name}%'")
        if subclauses:
            clauses.append("(" + " OR ".join(subclauses) + ")")
    return " OR ".join(clauses)


def _extract_requested_season_start_span(user_input: str):
    q = _extract_current_question_text(user_input).lower()
    is_playoffs = ("playoff" in q) or ("postseason" in q)

    explicit_range = re.search(
        r"\b(19\d{2}|20\d{2})(?:\s+season)?\s*(?:to|through|thru|-)\s*(19\d{2}|20\d{2})(?:\s+season)?\b",
        q,
    )
    if explicit_range:
        start = int(explicit_range.group(1))
        end = int(explicit_range.group(2))
        if start > end:
            start, end = end, start
        return start, end, is_playoffs

    prefixed_range = re.search(r"\bfrom\b\s*(19\d{2}|20\d{2})\b.+?\bto\b\s*(19\d{2}|20\d{2})\b", q)
    if prefixed_range:
        start = int(prefixed_range.group(1))
        end = int(prefixed_range.group(2))
        if start > end:
            start, end = end, start
        return start, end, is_playoffs

    decade_match = re.search(r"\b(19\d{2}|20\d{2})s\b", q)
    if decade_match:
        start = int(decade_match.group(1))
        return start, start + 9, is_playoffs

    season_label_match = re.search(r"\b(19\d{2}|20\d{2})\s*[-/_]\s*(\d{2})\b", q)
    if season_label_match:
        start = int(season_label_match.group(1))
        return start, start, is_playoffs

    bare = _extract_bare_year_request(user_input)
    if bare is not None:
        year, playoffs_flag = bare
        if playoffs_flag:
            return year - 1, year - 1, True
        return year, year, False

    if any(k in q for k in ["this season", "current season", "last season", "this playoff", "current playoff", "last playoff"]):
        start, end, playoffs_flag = _extract_requested_season_window(user_input)
        mapped_start = start if not playoffs_flag else start
        return mapped_start, mapped_start, playoffs_flag

    return None


def _advanced_table_name_for_window(start: int, end: int, is_playoffs: bool) -> str:
    if is_playoffs:
        return f"advance_totals_playoffseason_{start}_{end}"
    return f"advance_totals_regularseason_{start}_{end}"


def _qualified_public_table_ref(table_name: str) -> str:
    # Quote the identifier and qualify schema to avoid search_path mismatches.
    safe = (table_name or "").replace('"', '""')
    return f'public."{safe}"'


def _sql_select_mentions_column(select_part: str, col: str) -> bool:
    """Match quoted stat columns (e.g. \"fg%\") or plain identifiers in a SELECT list."""
    if not col:
        return True
    sp = select_part or ""
    if col.startswith('"'):
        return col.lower().replace(" ", "") in sp.lower().replace(" ", "")
    return re.search(rf"(?i)\b{re.escape(col)}\b", sp) is not None


def _pick_available_advanced_table(
    schema_description: str, start: int, end: int, is_playoffs: bool
) -> str:
    if not schema_description:
        return _advanced_table_name_for_window(start, end, is_playoffs)

    pattern = re.compile(
        r"advance_totals_(regularseason|playoffseason)_(\d{4})_(\d{4})",
        re.IGNORECASE,
    )
    matches = pattern.findall(schema_description)
    if not matches:
        legacy = re.compile(
            r"nba_advanced_season_(\d{4})_(\d{2})_season_type_(regular_season|playoffs)_[a-z0-9_]+",
            re.IGNORECASE,
        )
        leg = legacy.findall(schema_description)
        if not leg:
            return _advanced_table_name_for_window(start, end, is_playoffs)
        wanted = "playoffs" if is_playoffs else "regular_season"
        candidates = []
        for start_s, end_yy_s, season_type in leg:
            if season_type.lower() != wanted:
                continue
            try:
                st = int(start_s)
                ed = int(f"{str(st)[:2]}{end_yy_s}")
            except Exception:
                continue
            candidates.append((st, ed))
        if not candidates:
            return _advanced_table_name_for_window(start, end, is_playoffs)
        for st, ed in candidates:
            if st == start and ed == end:
                return _advanced_table_name_for_window(st, ed, is_playoffs)
        older_or_equal = [c for c in candidates if c[0] <= start]
        best = max(older_or_equal, key=lambda c: c[0]) if older_or_equal else max(candidates, key=lambda c: c[0])
        return _advanced_table_name_for_window(best[0], best[1], is_playoffs)

    wanted_kind = "playoffseason" if is_playoffs else "regularseason"
    candidates = []
    for kind, st_s, ed_s in matches:
        if kind.lower() != wanted_kind:
            continue
        try:
            candidates.append((int(st_s), int(ed_s)))
        except Exception:
            continue

    if not candidates:
        return _advanced_table_name_for_window(start, end, is_playoffs)

    for st, ed in candidates:
        if st == start and ed == end:
            return _advanced_table_name_for_window(st, ed, is_playoffs)

    older_or_equal = [c for c in candidates if c[0] <= start]
    if older_or_equal:
        best = max(older_or_equal, key=lambda c: c[0])
    else:
        best = max(candidates, key=lambda c: c[0])
    return _advanced_table_name_for_window(best[0], best[1], is_playoffs)


def _pick_latest_table_for_prefix(schema_description: str, prefix: str) -> str | None:
    if not schema_description or not prefix:
        return None
    table_names = []
    for raw_line in schema_description.splitlines():
        line = (raw_line or "").strip()
        m = re.match(r"^([a-zA-Z0-9_]+)\(", line)
        if not m:
            continue
        table_name = m.group(1)
        if table_name.startswith(prefix):
            table_names.append(table_name)
    if not table_names:
        return None

    def _sort_key(name: str):
        year_match = re.search(r"_(\d{4})_(\d{4})$", name)
        if year_match:
            return (int(year_match.group(1)), int(year_match.group(2)), name)
        return (-1, -1, name)

    return max(table_names, key=_sort_key)


def _replace_first_from_table(sql_query: str, table_name: str) -> str:
    if not sql_query or not table_name:
        return sql_query
    target_ref = _qualified_public_table_ref(table_name)
    from_pat = re.compile(
        r"(?is)\bfrom\s+(?:public\.)?(?:\"[^\"]+\"|[a-zA-Z_][a-zA-Z0-9_]*)"
    )
    return from_pat.sub(f"FROM {target_ref}", sql_query, count=1)


def _extract_schema_table_names(schema_description: str) -> set[str]:
    out: set[str] = set()
    if not schema_description:
        return out
    # 1) Exact table(col, ...) lines
    for raw_line in schema_description.splitlines():
        line = (raw_line or "").strip()
        m = re.match(r"^([a-zA-Z0-9_]+)\(", line)
        if m:
            out.add(m.group(1))
    # 2) Any season-table tokens embedded in compact section text
    token_pat = (
        r"\b(?:player_pergame_regularseason|player_totals_regularseason|player_totals_playoffseason|"
        r"advance_totals_regularseason|advance_totals_playoffseason|defense_totals_regularseason|"
        r"teams_pergame_regularseason|teams_pergame_playoffseason|players_clutch_regularseason|"
        r"players_clutch_playoffseason|clutch_totals_regularseason|clutch_totals_playoffseason|"
        r"violations_totals_regularseason|violations_totals_playoffseason|"
        r"all_players_regular|all_players_playoffs)_(\d{4})_(\d{4})\b"
    )
    for m in re.finditer(token_pat, schema_description, flags=re.IGNORECASE):
        out.add(f"{m.group(0)}")
    for static_name in ["player_game_logs", "court_shots"]:
        if re.search(rf"(?i)\b{re.escape(static_name)}\b", schema_description):
            out.add(static_name)
    return out


def _normalize_identifier_for_match(name: str) -> str:
    return re.sub(r"[^a-z0-9]", "", (name or "").lower())


def _best_existing_table_match(ref: str, existing_tables: set[str]) -> str | None:
    if not ref or not existing_tables:
        return None

    ref_clean = (ref or "").strip('"').strip().lower()
    if ref_clean in {t.lower() for t in existing_tables}:
        for t in existing_tables:
            if t.lower() == ref_clean:
                return t

    ref_norm = _normalize_identifier_for_match(ref_clean)
    if not ref_norm:
        return None

    # First pass: exact normalized match
    exact = [t for t in existing_tables if _normalize_identifier_for_match(t) == ref_norm]
    if exact:
        return sorted(exact)[-1]

    # Second pass: tolerate underscore loss/truncation by prefix overlap.
    close = [
        t
        for t in existing_tables
        if _normalize_identifier_for_match(t).startswith(ref_norm)
        or ref_norm.startswith(_normalize_identifier_for_match(t))
    ]
    if close:
        return max(close, key=len)
    return None


def _remap_unknown_tables_to_existing(sql_query: str, schema_description: str = "") -> str:
    """
    Repair malformed/nonexistent table identifiers in FROM/JOIN clauses by mapping
    them to the closest live table name from schema_description.
    """
    if not sql_query:
        return sql_query
    existing_tables = _extract_schema_table_names(schema_description)
    if not existing_tables:
        return sql_query

    q = sql_query
    refs = set(
        re.findall(
            r"(?i)\b(?:from|join)\s+(?:public\.)?\"?([a-zA-Z_][a-zA-Z0-9_]*)\"?",
            q,
        )
    )
    for ref in refs:
        if ref in existing_tables:
            continue
        replacement = _best_existing_table_match(ref, existing_tables)
        if not replacement or replacement == ref:
            continue

        # Replace only table refs in FROM/JOIN positions, preserving public. prefix.
        q = re.sub(
            rf'(?i)(\b(?:from|join)\s+(?:public\.)?)"{re.escape(ref)}"',
            rf'\1"{replacement}"',
            q,
        )
        q = re.sub(
            rf"(?i)(\b(?:from|join)\s+(?:public\.)?){re.escape(ref)}\b",
            rf'\1"{replacement}"',
            q,
        )
    return q


def _resolve_table_for_season(prefix: str, start: int, end: int, schema_description: str) -> str | None:
    preferred = f"{prefix}{start}_{end}"
    existing = _extract_schema_table_names(schema_description)
    if preferred in existing:
        return preferred
    return _pick_latest_table_for_prefix(schema_description, prefix)


def _sql_joins_player_pergame_and_advance_totals(sql_query: str) -> bool:
    """
    Detect model-built JOINs between per-game and advance totals. Those queries often
    mismatch seasons or reference per-game columns on `adv` (e.g. adv.fgm); we rebuild
    them with the canonical multi-family template when this pattern appears.
    """
    if not sql_query or not re.search(r"(?i)\bjoin\b", sql_query):
        return False
    low = sql_query.lower()
    if "player_pergame_regularseason_" not in low:
        return False
    return "advance_totals_regularseason_" in low or "advance_totals_playoffseason_" in low


def _build_dynamic_multitable_sql(sql_query: str, user_input: str, schema_description: str = "") -> str:
    q = _extract_current_question_text(user_input).lower()
    is_playoffs = "playoff" in q or "postseason" in q
    if is_playoffs:
        return sql_query

    is_compare_prompt = any(k in q for k in ["compare", " vs ", "versus", "between"])
    wants_advanced = any(
        k in q
        for k in [
            "advanced",
            "advance",
            "advances",
            "ts%",
            "true shooting",
            "usage",
            "usg",
            "offrtg",
            "defrtg",
            "netrtg",
        ]
    )
    wants_defense = any(k in q for k in ["defense", "defensive", "blocks", "steals", "def rating", "def_rtg", "opp_pts"])
    wants_offense = any(k in q for k in ["offense", "offensive", "points", "scoring", "assists", "rebounds", "fg%", "3p%", "ft%"])
    # For plain compare prompts, automatically include commonly useful families.
    if is_compare_prompt and not (wants_offense or wants_advanced or wants_defense):
        wants_offense = True
        wants_advanced = True
        wants_defense = True
    # Model often JOINs pergame + advance without keywords that set both offense and
    # advanced flags, leaving broken SQL (mismatched seasons, adv.fgm / adv.tov, bare age).
    if _sql_joins_player_pergame_and_advance_totals(sql_query or ""):
        wants_offense = True
        wants_advanced = True
    needed = int(wants_advanced) + int(wants_defense) + int(wants_offense)
    if needed < 2:
        return sql_query

    start, end, _ = _extract_requested_season_window(user_input)
    base_table = _resolve_table_for_season("player_pergame_regularseason_", start, end, schema_description)
    adv_table = _resolve_table_for_season("advance_totals_regularseason_", start, end, schema_description) if wants_advanced else None
    def_table = _resolve_table_for_season("defense_totals_regularseason_", start, end, schema_description) if wants_defense else None

    if not base_table:
        return sql_query
    if wants_advanced and not adv_table:
        return sql_query
    if wants_defense and not def_table:
        return sql_query

    names = _extract_named_players(user_input, sql_query)
    compare_match = re.search(r"(?i)\bcompare\s+(.+?)\s+(?:and|vs|versus)\s+(.+?)(?:\bfrom\b|\bin\b|\bduring\b|$)", _extract_current_question_text(user_input))
    explicit_names: list[str] = []
    if compare_match:
        explicit_left = _normalize_player_candidate(compare_match.group(1))
        explicit_right = _normalize_player_candidate(compare_match.group(2))
        explicit_names = [n for n in [explicit_left, explicit_right] if n]
        if len(explicit_names) >= 2:
            names = explicit_names
    if any(k in q for k in ["compare", "vs", "versus", "between"]):
        sql_names = _extract_player_names_from_sql(sql_query)
        if len(explicit_names) < 2 and len(sql_names) >= 2:
            names = sql_names
    if any(k in q for k in ["compare", "vs", "versus", "between"]):
        unique_compare = []
        seen_compare = set()
        for n in names:
            key = (n or "").strip().lower()
            if not key or key in seen_compare:
                continue
            seen_compare.add(key)
            unique_compare.append(n.strip())
        if len(unique_compare) < 2:
            q_text = _extract_current_question_text(user_input)
            caps = re.findall(r"\b([A-Z][a-zA-Z\.\-]+(?:\s+[A-Z][a-zA-Z\.\-]+))\b", q_text)
            for c in caps:
                c_clean = _normalize_player_candidate(c)
                if not c_clean:
                    continue
                key = c_clean.lower()
                if key in seen_compare:
                    continue
                if key in {"compare jayson", "this season", "current season"}:
                    continue
                seen_compare.add(key)
                unique_compare.append(c_clean)
                if len(unique_compare) >= 2:
                    break
        if len(unique_compare) >= 2:
            names = unique_compare
    where_clause = _player_where_clause_for_names(names)
    if where_clause:
        where_clause = re.sub(r"(?i)(?<!\.)\bplayer\b", "pg.player", where_clause)
    else:
        where_match = re.search(r"(?is)\bwhere\b(.*?)(\border\s+by\b|\blimit\b|$)", sql_query)
        where_clause = (where_match.group(1).strip() if where_match else "")
        where_clause = re.sub(r"(?i)\bplayer_name\b", "pg.player", where_clause)
        where_clause = re.sub(r"(?i)(?<!\.)\bplayer\b", "pg.player", where_clause)

    select_cols = ['pg.player', 'pg.team', 'pg.gp']
    if wants_offense:
        select_cols.extend(['pg.pts', 'pg.reb', 'pg.ast', 'pg."fg%"', 'pg."3p%"', 'pg."ft%"'])
    if wants_advanced:
        select_cols.extend(['adv.ts', 'adv.usg', 'adv.offrtg', 'adv.defrtg', 'adv.netrtg'])
    if wants_defense:
        select_cols.extend(['defn.dreb', 'defn.stl', 'defn.blk', 'defn.def_rtg', 'defn.opp_pts'])

    limit_clause = "LIMIT 50"
    lim_match = re.search(r"(?i)\blimit\s+(\d+)", sql_query)
    if lim_match:
        limit_clause = f"LIMIT {lim_match.group(1)}"

    parts = [
        "SELECT " + ", ".join(select_cols),
        f"FROM {_qualified_public_table_ref(base_table)} pg",
    ]
    if wants_advanced and adv_table:
        parts.append(f"LEFT JOIN {_qualified_public_table_ref(adv_table)} adv ON pg.player = adv.player AND pg.team = adv.team")
    if wants_defense and def_table:
        parts.append(f"LEFT JOIN {_qualified_public_table_ref(def_table)} defn ON pg.player = defn.player AND pg.team = defn.team")
    if where_clause:
        parts.append(f"WHERE {where_clause}")
    parts.append(limit_clause)
    base_sql = " ".join(parts).strip() + ";"

    return base_sql


def _remap_known_table_families_to_existing(sql_query: str, user_input: str, schema_description: str = "") -> str:
    """
    Ensure known table-family references always point to existing physical tables
    from the live schema (latest season for that family/context).
    """
    if not sql_query:
        return sql_query
    existing_tables = _extract_schema_table_names(schema_description)
    if not existing_tables:
        return sql_query

    q = sql_query
    q_lower = _extract_current_question_text(user_input).lower()
    is_playoff_ask = any(k in q_lower for k in ["playoff", "postseason"])

    def _pick(prefixes: list[str]) -> str | None:
        for prefix in prefixes:
            t = _pick_latest_table_for_prefix(schema_description, prefix)
            if t:
                return t
        return None

    refs = set(
        re.findall(
            r"(?i)\b(?:from|join)\s+(?:public\.)?\"?([a-zA-Z_][a-zA-Z0-9_]*)\"?",
            q,
        )
    )
    for ref in refs:
        if ref in existing_tables:
            continue
        replacement = None
        low = ref.lower()
        if low.startswith("clutch_totals_") or low.startswith("players_clutch_"):
            replacement = _pick(
                ["clutch_totals_playoffseason_", "players_clutch_playoffseason_", "players_clutch_playoffs_"]
                if is_playoff_ask
                else ["clutch_totals_regularseason_", "players_clutch_regularseason_"]
            )
        elif low.startswith("violations_totals_"):
            replacement = _pick(
                ["violations_totals_playoffseason_", "violations_totals_playoffs_"]
                if is_playoff_ask
                else ["violations_totals_regularseason_", "violations_totals_regular_"]
            )
        elif low.startswith("player_pergame_regularseason_"):
            replacement = _pick(["player_pergame_regularseason_"])
        elif low.startswith("player_totals_playoffseason_"):
            replacement = _pick(["player_totals_playoffseason_"])
        elif low.startswith("player_totals_regularseason_"):
            replacement = _pick(["player_totals_regularseason_"])
        elif low.startswith("advance_totals_playoffseason_"):
            replacement = _pick(["advance_totals_playoffseason_"])
        elif low.startswith("advance_totals_regularseason_"):
            replacement = _pick(["advance_totals_regularseason_"])
        elif low.startswith("defense_totals_regularseason_"):
            replacement = _pick(["defense_totals_regularseason_"])

        if replacement:
            q = re.sub(rf"(?i)\b{re.escape(ref)}\b", replacement, q)
    return q


def _enforce_extended_family_table_mapping(sql_query: str, user_input: str, schema_description: str = "") -> str:
    """
    Route non-basic intents (clutch/lineups/schedule/standings/shot chart/defense totals)
    to the right table family, defaulting to latest available season (2025_2026 when present).
    """
    if not sql_query:
        return sql_query
    q = _extract_current_question_text(user_input).lower()
    is_playoff_ask = any(k in q for k in ["playoff", "postseason"])

    # Team-level defense asks should route to team tables (player defense totals do not
    # contain team-name aggregates for prompts like "Celtics defense metrics").
    has_team_alias = any(team_key in q for team_key in _TEAM_ALIASES.keys())
    has_defense_term = any(t in q for t in ["defense", "defensive", "def rating", "def_rtg", "opponent points", "opp pts"])
    if has_team_alias and has_defense_term:
        start, end, _ = _extract_requested_season_window(user_input)
        team_prefix = "teams_pergame_playoffseason_" if is_playoff_ask else "teams_pergame_regularseason_"
        target_team_table = (
            f"{team_prefix}{start}_{end}"
            if _extract_schema_table_names(schema_description) and f"{team_prefix}{start}_{end}" in _extract_schema_table_names(schema_description)
            else _pick_latest_table_for_prefix(schema_description, team_prefix)
        )
        if target_team_table:
            return _replace_first_from_table(sql_query, target_team_table)

    # Violations intent: always route to violations totals family (regular/playoffs).
    if any(t in q for t in ["violation", "violations", "voilation", "voilations"]):
        candidate_prefixes = (
            ["violations_totals_playoffseason_", "violations_totals_playoffs_"]
            if is_playoff_ask
            else ["violations_totals_regularseason_", "violations_totals_regular_"]
        )
        for prefix in candidate_prefixes:
            target_table = _pick_latest_table_for_prefix(schema_description, prefix)
            if target_table:
                return _replace_first_from_table(sql_query, target_table)

    # Clutch intent: prefer clutch totals family (regular/playoffs based on context).
    if any(t in q for t in ["clutch", "close games", "last 5 minutes"]):
        clutch_prefixes = (
            ["clutch_totals_playoffseason_", "players_clutch_playoffseason_", "players_clutch_playoffs_"]
            if is_playoff_ask
            else ["clutch_totals_regularseason_", "players_clutch_regularseason_"]
        )
        for prefix in clutch_prefixes:
            target_table = _pick_latest_table_for_prefix(schema_description, prefix)
            if target_table:
                names = _extract_named_players(user_input, sql_query or "")
                where_clause = _player_where_clause_for_names(names) if names else ""
                if where_clause:
                    return (
                        "SELECT DISTINCT player, team, gp, pts, fgm, fga, fg, c_3pm, c_3pa, c_3p, ftm, fta, ft "
                        f"FROM {_qualified_public_table_ref(target_table)} "
                        f"WHERE {where_clause} "
                        "ORDER BY gp DESC NULLS LAST "
                        "LIMIT 1;"
                    )
                return _replace_first_from_table(sql_query, target_table)

    mapping: list[tuple[list[str], str]] = [
        (["shot chart", "shotchart", "heat map", "heatmap", "court location", "shot location"], "court_shots"),
        (["lineup", "5-man unit", "best lineups", "best lineup", "on/off 5"], "nba__lineups__"),
        (["standings", "seed", "conference rank", "conference standings", "record"], "nba__standings__"),
        (["schedule", "next games", "calendar"], "nba__schedule__"),
        (["defense totals", "defensive profile", "best defenders", "defense metrics"], "defense_totals_regularseason_"),
    ]

    target_prefix = None
    for terms, prefix in mapping:
        if any(t in q for t in terms):
            target_prefix = prefix
            break
    if target_prefix is None:
        return sql_query

    target_table = _pick_latest_table_for_prefix(schema_description, target_prefix)
    if not target_table:
        return sql_query

    routed = _replace_first_from_table(sql_query, target_table)
    if "recent" in q or "lately" in q or "last " in q:
        # Preserve game-log recency path for explicit recent-game asks.
        return sql_query
    return routed


def _enforce_defense_table_columns(sql_query: str) -> str:
    """
    Defense totals tables in this DB have a narrow column set (no pts/reb/tov).
    Normalize SELECT lists to valid defensive columns when routed there.
    """
    if not sql_query:
        return sql_query
    q_lower = sql_query.lower()
    if "defense_totals_regularseason_" not in q_lower:
        return sql_query
    if re.search(r"(?i)\bjoin\b", sql_query):
        return sql_query
    safe_select = "player, team, gp, min, def_rtg, dreb, stl, blk, opp_pts, def"
    return re.sub(r"(?is)\bselect\b.*?\bfrom\b", f"SELECT {safe_select} FROM", sql_query, count=1)


def _enforce_violations_table_columns(sql_query: str) -> str:
    """
    Violations totals tables do not have scoring/rebounding/assist columns.
    Normalize SELECT lists to valid violations-table columns.
    """
    if not sql_query:
        return sql_query
    q_lower = sql_query.lower()
    if "violations_totals_regularseason_" not in q_lower and "violations_totals_playoffseason_" not in q_lower:
        return sql_query
    safe_select = "player, team, age, gp, w, l, travel, double, disc, off, inbound, back, off_1, palming, off_2, def, charge, def_1, lane, jump, kicked"
    return re.sub(r"(?is)\bselect\b.*?\bfrom\b", f"SELECT {safe_select} FROM", sql_query, count=1)


_TEAM_ALIASES = {
    "hawks": ["ATLANTA HAWKS", "ATL"],
    "celtics": ["BOSTON CELTICS", "BOS"],
    "nets": ["BROOKLYN NETS", "BKN"],
    "hornets": ["CHARLOTTE HORNETS", "CHA"],
    "bulls": ["CHICAGO BULLS", "CHI"],
    "cavaliers": ["CLEVELAND CAVALIERS", "CLE"],
    "mavericks": ["DALLAS MAVERICKS", "DAL"],
    "nuggets": ["DENVER NUGGETS", "DEN"],
    "pistons": ["DETROIT PISTONS", "DET"],
    "warriors": ["GOLDEN STATE WARRIORS", "GSW"],
    "rockets": ["HOUSTON ROCKETS", "HOU"],
    "pacers": ["INDIANA PACERS", "IND"],
    "clippers": ["LOS ANGELES CLIPPERS", "LAC"],
    "lakers": ["LOS ANGELES LAKERS", "LAL", "LAKERS"],
    "grizzlies": ["MEMPHIS GRIZZLIES", "MEM"],
    "heat": ["MIAMI HEAT", "MIA"],
    "bucks": ["MILWAUKEE BUCKS", "MIL"],
    "timberwolves": ["MINNESOTA TIMBERWOLVES", "MIN"],
    "pelicans": ["NEW ORLEANS PELICANS", "NOP"],
    "knicks": ["NEW YORK KNICKS", "NYK"],
    "thunder": ["OKLAHOMA CITY THUNDER", "OKC"],
    "magic": ["ORLANDO MAGIC", "ORL"],
    "76ers": ["PHILADELPHIA 76ERS", "PHI", "SIXERS"],
    "suns": ["PHOENIX SUNS", "PHX"],
    "blazers": ["PORTLAND TRAIL BLAZERS", "POR", "TRAIL BLAZERS"],
    "kings": ["SACRAMENTO KINGS", "SAC"],
    "spurs": ["SAN ANTONIO SPURS", "SAS"],
    "raptors": ["TORONTO RAPTORS", "TOR"],
    "jazz": ["UTAH JAZZ", "UTA"],
    "wizards": ["WASHINGTON WIZARDS", "WAS"],
}


def _is_team_record_request(user_input: str) -> bool:
    q = _extract_current_question_text(user_input).lower()
    has_team_word = any(k in q for k in [" team ", "teams", "lakers", "celtics", "warriors", "knicks", "bucks"])
    has_record_word = any(k in q for k in ["record", "win-loss", "wins", "losses", "standing", "seed"])
    return has_team_word and has_record_word


def _enforce_team_table_mapping(sql_query: str, user_input: str, schema_description: str = "") -> str:
    if not sql_query or not _is_team_record_request(user_input):
        return sql_query
    q = _extract_current_question_text(user_input).lower()
    is_playoffs = "playoff" in q or "postseason" in q
    prefix = "teams_pergame_playoffseason_" if is_playoffs else "teams_pergame_regularseason_"
    target_table = _pick_latest_table_for_prefix(schema_description, prefix)
    if not target_table:
        return sql_query
    return _replace_first_from_table(sql_query, target_table)


def _enforce_team_table_columns(sql_query: str, user_input: str) -> str:
    if not sql_query:
        return sql_query
    q_lower = sql_query.lower()
    if "teams_pergame_regularseason_" not in q_lower and "teams_pergame_playoffseason_" not in q_lower:
        return sql_query
    # These team tables came from CSV with case-sensitive column names.
    safe_select = '"Team", "GP", "W", "L", "WIN%", "PTS", "FG%", "3P%", "FT%", "+/-"'
    out = re.sub(r"(?is)\bselect\b.*?\bfrom\b", f"SELECT {safe_select} FROM", sql_query, count=1)

    user_q = _extract_current_question_text(user_input).lower()
    team_candidates = []
    for key, aliases in _TEAM_ALIASES.items():
        if key in user_q:
            team_candidates.extend(aliases)
    if team_candidates:
        clause = " OR ".join([f"\"Team\" ILIKE '%{a}%'" for a in dict.fromkeys(team_candidates)])
        if re.search(r"(?i)\bwhere\b", out):
            out = re.sub(r"(?is)\bwhere\b.*?(?=\border\s+by\b|\blimit\b|$)", f"WHERE ({clause}) ", out, count=1)
        else:
            out = out.rstrip(";") + f" WHERE ({clause})"
    elif _is_team_record_request(user_input):
        out = re.sub(r"(?is)\bwhere\b.*?(?=\border\s+by\b|\blimit\b|$)", " ", out, count=1)
    # Prefer the most complete/latest row when duplicate team labels exist.
    has_order = re.search(r"(?i)\border\s+by\b", out) is not None
    has_limit = re.search(r"(?i)\blimit\b", out) is not None
    if not has_order and has_limit:
        lim_match = re.search(r"(?is)\blimit\s+\d+\b", out)
        lim_clause = lim_match.group(0) if lim_match else "LIMIT 1"
        out = re.sub(r"(?is)\blimit\s+\d+\b", "", out).strip().rstrip(";")
        out = out + ' ORDER BY "GP" DESC NULLS LAST ' + lim_clause
    elif not has_order:
        out = out.rstrip(";") + ' ORDER BY "GP" DESC NULLS LAST'
    if _is_team_record_request(user_input) and not team_candidates:
        out = re.sub(
            r'(?is)\border\s+by\b.*?(?=\blimit\b|$)',
            'ORDER BY "W" DESC NULLS LAST, "WIN%" DESC NULLS LAST ',
            out,
            count=1,
        )
    if not has_limit:
        out = out.rstrip(";") + " LIMIT 1"
    return out


def _enforce_best_team_record_query(sql_query: str, user_input: str, schema_description: str = "") -> str:
    q = _extract_current_question_text(user_input).lower()
    if not _is_team_record_request(user_input):
        return sql_query
    if not any(k in q for k in ["best", "top", "highest"]):
        return sql_query
    if any(team_key in q for team_key in _TEAM_ALIASES.keys()):
        return sql_query
    target_table = _pick_latest_table_for_prefix(schema_description, "teams_pergame_regularseason_")
    if not target_table:
        return sql_query
    return (
        f'SELECT "Team", "GP", "W", "L", "WIN%", "PTS", "FG%", "3P%", "FT%", "+/-" '
        f'FROM {_qualified_public_table_ref(target_table)} '
        'ORDER BY "W" DESC NULLS LAST, "WIN%" DESC NULLS LAST LIMIT 1;'
    )


def _enforce_playoff_compare_template(sql_query: str, user_input: str, schema_description: str = "") -> str:
    q = _extract_current_question_text(user_input).lower()
    if "playoff" not in q and "postseason" not in q:
        return sql_query
    if not any(k in q for k in ["compare", "versus", " vs ", "between"]):
        return sql_query
    names = _extract_named_players(user_input, sql_query)
    if len(names) < 2:
        return sql_query
    start, end, is_playoffs = _extract_requested_season_window(user_input)
    if not is_playoffs:
        return sql_query
    table = _resolve_table_for_season("player_totals_playoffseason_", start, end, schema_description)
    if not table:
        return sql_query
    where_clause = _player_where_clause_for_names(names[:2])
    if not where_clause:
        return sql_query
    return (
        'SELECT player, team, gp, pts, reb, ast, stl, blk, tov, fgm, fga, fg, c_3pm, c_3pa, c_3p, ftm, fta, ft '
        f'FROM {_qualified_public_table_ref(table)} WHERE {where_clause} LIMIT 50;'
    )


def _enforce_multi_family_stats_join(sql_query: str, user_input: str, schema_description: str = "") -> str:
    if not sql_query:
        return sql_query
    return _build_dynamic_multitable_sql(sql_query, user_input, schema_description)


def _enforce_compare_pts_reb_ast_ts_template(sql_query: str, user_input: str) -> str:
    # Legacy compare template disabled; dynamic multi-family router handles compare logic.
    return sql_query


def _enforce_game_log_column_mapping(sql_query: str, user_input: str) -> str:
    """
    Disable player_game_logs routing and remap any generated game-log SQL to the
    current regular-season summary table family.
    """
    if not sql_query:
        return sql_query

    q = sql_query
    if "player_game_logs" not in q.lower():
        return sql_query

    # Route away from game logs entirely.
    q = re.sub(
        r"(?i)\bplayer_game_logs\b",
        "player_pergame_regularseason_2025_2026",
        q,
    )
    # Align common game-log column names to summary-table columns.
    q = re.sub(r"(?i)\bplayer_name\b", "player", q)
    q = re.sub(r"(?i)\bteam_abbreviation\b", "team", q)
    q = re.sub(r"(?i)\bfg3m\b", '"3pm"', q)
    q = re.sub(r"(?i)\bfg3a\b", '"3pa"', q)
    q = re.sub(r"(?i)\bfg_pct\b", '"fg%"', q)
    q = re.sub(r"(?i)\bfg3_pct\b", '"3p%"', q)
    q = re.sub(r"(?i)\bft_pct\b", '"ft%"', q)
    # Remove game-log-only date/season clauses that are invalid on summary tables.
    q = re.sub(r"(?i)\bseason_id\s*=\s*'?\d+'?\s*(and\s*)?", "", q)
    q = re.sub(r"(?i)\bgame_date\b", "gp", q)
    q = re.sub(r"(?i)\bgame_id\b", "gp", q)
    q = re.sub(r"(?i)\bmatchup\b", "team", q)
    q = re.sub(r"(?i)\bseason_type\b", "team", q)
    q = re.sub(r"(?i)\bwl\b", "w", q)
    return q


def _enforce_advanced_table_mapping(sql_query: str, user_input: str, schema_description: str = "") -> str:
    if not sql_query or not _is_advanced_metrics_request(user_input):
        return sql_query
    # Do not rewrite table names on already-joined SQL; these queries are typically
    # intentional mixed-family pulls and over-rewriting can corrupt qualified refs.
    if re.search(r"(?i)\bjoin\b", sql_query):
        return sql_query
    # Preserve already-correct mixed-table join queries (basic + advanced).
    if re.search(r"(?i)\bjoin\b", sql_query) and re.search(r"(?i)\badv\.ts\b", sql_query):
        if re.search(r"(?i)\bpg\.(pts|reb|ast|gp)\b", sql_query):
            return sql_query
    user_q = _extract_current_question_text(user_input).lower()
    has_basic_stat_terms = any(
        k in user_q
        for k in ["points", "pts", "rebound", "reb", "assist", "ast", "fg%", "3p%", "ft%", "box score"]
    )
    # Do not collapse/retarget when the query is already multi-table for mixed stat asks.
    if has_basic_stat_terms and re.search(r"(?i)\bjoin\b", sql_query):
        return sql_query

    start, end, is_playoffs = _extract_requested_season_window(user_input)
    target = _pick_available_advanced_table(schema_description, start, end, is_playoffs)
    target_ref = _qualified_public_table_ref(target)
    q = sql_query

    is_true_shooting_request = any(k in user_q for k in ["true shooting", "ts%", "ts pct", "ts_pct"])
    is_multi_metric_or_compare = bool(
        re.search(r"(?i)\b(compare|versus|vs|between)\b", user_q)
        or re.search(r"(?i)\b(points?|rebounds?|assists?|pts|reb|ast)\b", user_q)
    )
    if not is_playoffs and not is_true_shooting_request and not is_multi_metric_or_compare:
        player_names = _extract_named_players(user_input, sql_query or "")
        where_clause = _player_where_clause_for_names(player_names) if player_names else ""
        if not where_clause:
            where_clause = "1=1"
        where_clause = re.sub(r"(?i)\bplayer_name\b", "player", where_clause)
        where_clause = re.sub(r"(?i)(?<!\.)\bplayer\b", "pg.player", where_clause)
        pergame_table = _qualified_public_table_ref(f"player_pergame_regularseason_{start}_{end}")
        return (
            "SELECT DISTINCT pg.player AS player_name, pg.team AS team_abbreviation, pg.gp, pg.min, pg.pts, pg.reb, pg.ast, "
            "adv.ts, adv.usg, adv.off_rtg, adv.def_rtg, adv.net_rtg, adv.pie "
            f"FROM {pergame_table} pg "
            f"LEFT JOIN {target_ref} adv ON pg.player = adv.player AND pg.team = adv.team "
            f"WHERE {where_clause} "
            "LIMIT 50;"
        )

    # Route any season-summary/game-log source to advanced table family for advanced-stat intents.
    q = re.sub(r"(?i)all_players_(regular|playoffs)_\d{4}_\d{4}", target_ref, q)
    q = re.sub(r"(?i)player_pergame_regularseason_\d{4}_\d{4}", target_ref, q)
    q = re.sub(r"(?i)player_totals_playoffseason_\d{4}_\d{4}", target_ref, q)
    q = re.sub(r"(?i)player_totals_regularseason_\d{4}_\d{4}", target_ref, q)
    q = re.sub(r"(?i)\bplayer_game_logs\b", target_ref, q)
    q = re.sub(
        r"(?i)nba_advanced_season_\d{4}_\d{2}_season_type_(regular_season|playoffs)_[a-z0-9_]+",
        target_ref,
        q,
    )
    q = re.sub(
        r"(?i)advance_totals_(?:regularseason|playoffseason)_\d{4}_\d{4}",
        target_ref,
        q,
    )

    # Only collapse to a TS-only query when TS is the primary ask.
    # If user also asks for basic box stats (pts/reb/ast/etc.), preserve multi-table SQL.
    if is_true_shooting_request and not has_basic_stat_terms and not is_multi_metric_or_compare:
        where_clause = None
        where_match = re.search(r"(?is)\bwhere\b(.*?)(\bgroup\s+by\b|\border\s+by\b|\blimit\b|$)", q)
        if where_match:
            maybe = where_match.group(1).strip()
            if re.search(r"(?i)\bplayer\b", maybe) or re.search(r"(?i)\bplayer_name\b", maybe):
                where_clause = re.sub(r"(?i)\bplayer_name\b", "player", maybe)
                # If we collapse to a single advanced table, strip source aliases like pg.player.
                where_clause = re.sub(r"(?i)\b[a-zA-Z_][a-zA-Z0-9_]*\.(player)\b", r"\1", where_clause)

        limit = 50
        lim_match = re.search(r"(?i)\blimit\s+(\d+)", q)
        if lim_match:
            try:
                limit = int(lim_match.group(1))
            except Exception:
                limit = 50

        parts = [
            "SELECT player, team, ts AS true_shooting_pct",
            f"FROM {target_ref}",
        ]
        if where_clause:
            parts.append(f"WHERE {where_clause}")
        parts.append("ORDER BY true_shooting_pct DESC")
        parts.append(f"LIMIT {limit}")
        q = " ".join(parts) + ";"

    return q


def _columns_for_by_season_question(question_text: str) -> list[str]:
    q = (question_text or "").lower()
    if any(k in q for k in ["shoot", "percentage", "fg%", "3 point", "3p", "true shooting"]):
        return ['"fg%"', '"3p%"', '"ft%"', "fgm", "fga", '"3pm"', '"3pa"', "ftm", "fta", "gp"]
    if any(k in q for k in ["rebound", "boards", "glass"]):
        return ["reb", "oreb", "dreb", "gp"]
    if any(k in q for k in ["assist", "playmaking", "passing"]):
        return ["ast", "tov", "gp"]
    if any(k in q for k in ["block", "rim protection"]):
        return ["blk", "gp"]
    if any(k in q for k in ["steal", "defense"]):
        return ["stl", "gp"]
    if any(k in q for k in ["score", "points", "scor", "offense"]):
        return ["pts", '"fg%"', '"3p%"', '"ft%"', "gp"]
    return ["pts", "reb", "ast", '"fg%"', '"3p%"', '"ft%"', "gp"]


def _is_over_time_request(question_text: str) -> bool:
    q = (question_text or "").lower()
    phrases = [
        "by season",
        "per season",
        "each season",
        "season by season",
        "over the years",
        "through the years",
        "across seasons",
        "year by year",
        "trend",
        "over time",
        "over his career",
        "over her career",
        "over their career",
        "throughout his career",
        "throughout her career",
        "throughout their career",
    ]
    if any(p in q for p in phrases):
        return True
    # Handle slight misspellings like "careeer" and flexible "over ... career" phrasing.
    if re.search(r"\bover\b.*\bcaree+r\b", q):
        return True
    if re.search(r"\bthroughout\b.*\bcaree+r\b", q):
        return True
    if re.search(r"\b(per\s+game|per-game)\b.*\bcaree+r\b", q) or re.search(r"\bcaree+r\b.*\b(per\s+game|per-game)\b", q):
        return True
    if "rookie year" in q:
        return True
    if re.search(r"\bfrom\b.+\bto\b\s*(19\d{2}|20\d{2})\b", q):
        return True
    if re.search(r"\b(19\d{2}|20\d{2})s\b", q) or "decade" in q:
        return True
    if re.search(r"\b(highest|best|most|peak)\b.+\bseason\b", q):
        return True
    return re.search(r"\b(19\d{2}|20\d{2})\s*(to|through|thru|-)\s*(19\d{2}|20\d{2})\b", q) is not None


def _is_explicit_total_request(question_text: str) -> bool:
    q = (question_text or "").lower()
    total_terms = [
        "total",
        "totals",
        "sum",
        "combined",
        "total minutes",
        "minutes played",
        "across",
        "overall number",
        "career total",
        "how many in his career",
        "how many in her career",
        "how many in their career",
    ]
    return any(t in q for t in total_terms)


def _is_explicit_per_game_request(question_text: str) -> bool:
    q = (question_text or "").lower()
    return any(
        t in q
        for t in [
            "per game",
            "per-game",
            "ppg",
            "rpg",
            "apg",
            "average",
            "averages",
        ]
    )


def _enforce_regular_totals_table_mapping(sql_query: str, user_input: str) -> str:
    """
    If the user explicitly asks for totals (e.g. total minutes), ensure regular-season
    queries use player_totals_regularseason_* instead of per-game tables.
    """
    if not sql_query:
        return sql_query
    question_text = _extract_current_question_text(user_input)
    if not _is_explicit_total_request(question_text):
        return sql_query
    if _is_explicit_per_game_request(question_text):
        return sql_query
    if re.search(r"(?i)\bplayoff|postseason\b", question_text):
        return sql_query

    # For direct total-minutes asks, force a deterministic totals-table query.
    # This avoids model drift back to per-game families on mutated prompts.
    if re.search(r"(?i)\bminutes?\b", question_text):
        start, end, is_playoffs = _extract_requested_season_window(user_input)
        if not is_playoffs:
            target_table = f"player_totals_regularseason_{start}_{end}"
            player_names = _extract_named_players(user_input, sql_query or "")
            where_clause = _player_where_clause_for_names(player_names) if player_names else ""
            if not where_clause:
                where_clause = "1=1"
            season_label = f"{start}-{str(end)[-2:]}"
            return (
                f"SELECT DISTINCT {start} AS season_start, '{season_label}' AS season_label, "
                f"player AS player_name, team AS team_abbreviation, gp, min "
                f"FROM {_qualified_public_table_ref(target_table)} "
                f"WHERE {where_clause} "
                "ORDER BY min DESC NULLS LAST "
                "LIMIT 50;"
            )

    q = sql_query
    q = re.sub(
        r"(?i)player_pergame_regularseason_(\d{4})_(\d{4})",
        r"player_totals_regularseason_\1_\2",
        q,
    )
    if "player_totals_regularseason_" in q.lower():
        q = _apply_totals_column_aliases(q)
    return q


def _has_explicit_comparison_timeframe(user_input: str) -> bool:
    """Season/year/rookie/nth-season cues — absent these, head-to-head defaults to career."""
    qt = _extract_current_question_text(user_input).lower()
    if re.search(r"\b(19\d{2}|20\d{2})\s*[-/_]\s*(\d{2}|19\d{2}|20\d{2})\b", qt):
        return True
    if _extract_bare_year_request(user_input) is not None:
        return True
    if any(
        k in qt
        for k in [
            "this season",
            "current season",
            "last season",
            "this playoff",
            "current playoff",
            "last playoff",
            "last year",
            "this year",
        ]
    ):
        return True
    if _extract_requested_nth_season(user_input) is not None:
        return True
    if "rookie year" in qt:
        return True
    if re.search(r"\b(rookie|sophomore|freshman)\s+(season|year)\b", qt):
        return True
    if re.search(r"\b\d{1,2}(st|nd|rd|th)\s+(season|year)\b", qt):
        return True
    for word in _ORDINAL_WORDS.keys():
        if re.search(rf"\b{word}\s+(season|year)\b", qt):
            return True
    if _extract_requested_season_start_span(user_input) is not None:
        return True
    return False


def _is_implicit_head_to_head_career_question(user_input: str) -> bool:
    """Stat-style comparison(s) between players with no explicit season window."""
    qt = _extract_current_question_text(user_input).lower()
    looks_like_pair_compare = (
        re.search(r"\bbetween\b.+\band\b", qt) is not None
        or bool(re.search(r"\b(who\s+(is|'s)|which)\s+.{0,40}\bbetter\b", qt))
        or ("compare " in qt)
        or (" vs " in qt)
        or (" versus " in qt)
        or ("better than " in qt)
        or (" than " in qt and re.search(r"\bbetter\b", qt))
    )
    if not looks_like_pair_compare:
        return False
    if _has_explicit_comparison_timeframe(user_input):
        return False
    # Playoff-era head-to-head without a year uses latest playoffs elsewhere; rewriter handles regular only first.
    return True


def _rewrite_implicit_head_to_head_to_career_sql(sql_query: str, user_input: str, conn) -> str:
    """Expand single-season multi-player compares to weighted career aggregates (regular season)."""
    if re.search(r"(?i)\bjoin\b", sql_query or ""):
        return sql_query
    q_text = _extract_current_question_text(user_input).lower()
    if ("compare" in q_text or "versus" in q_text or " vs " in q_text or "between" in q_text) and (
        "playoff" in q_text or "postseason" in q_text
    ):
        return sql_query
    if conn is None or not _is_implicit_head_to_head_career_question(user_input):
        return sql_query
    raw = sql_query or ""
    q_low = raw.lower().strip()

    named = _extract_named_players(user_input, raw)
    if len(named) < 2:
        return sql_query

    if "union all" in q_low and raw.lower().count("player_pergame_regularseason_") > 1:
        return sql_query

    plays = ("playoff" in _extract_current_question_text(user_input).lower()) or (
        "postseason" in _extract_current_question_text(user_input).lower()
    )
    table_kind = "playoffs" if plays else "regular"
    prefix = (
        "player_totals_playoffseason_"
        if table_kind == "playoffs"
        else "player_pergame_regularseason_"
    )

    esc_p = re.escape(prefix)
    from_m = re.search(rf"(?is)\bfrom\s+(?:public\.)?\"(?P<t>{esc_p}\d{{4}}_\d{{4}})\"\b", raw)
    if not from_m:
        from_m = re.search(rf"(?is)\bfrom\s+(?:public\.)?(?P<t>{esc_p}\d{{4}}_\d{{4}})\b", raw)
    if not from_m:
        return sql_query

    wm = re.search(r"(?is)\bwhere\b", raw)
    if not wm:
        return sql_query
    tail = raw[wm.end() :]
    stops = []
    for pat in (r"\border\s+by\b", r"\bgroup\s+by\b", r"\blimit\b"):
        mm = re.search(pat, tail, flags=re.IGNORECASE)
        if mm:
            stops.append(mm.start())
    where_clause = tail[: min(stops) if stops else len(tail)].strip().rstrip(";")
    where_clause = re.sub(r"(?i)\bplayer_name\b", "player", where_clause)

    avail = _available_season_starts(conn, table_kind)
    if len(avail) < 2:
        return sql_query

    legs = []
    for start_year in avail:
        end_full = start_year + 1
        tnam = f"{prefix}{start_year}_{end_full}"
        ref = _qualified_public_table_ref(tnam)
        if table_kind == "playoffs":
            legs.append(
                f"SELECT {start_year} AS season_start, player, team, gp, w, l, min, pts, reb, ast, "
                "fgm, fga, fg, c_3pm, c_3pa, c_3p, ftm, fta, ft, oreb, dreb, stl, blk, tov, pf "
                f"FROM {ref} WHERE ({where_clause})"
            )
        else:
            legs.append(
                f'SELECT {start_year} AS season_start, player, team, gp, w, l, min, pts, reb, ast, '
                'fgm, fga, "fg%", "3pm", "3pa", "3p%", ftm, fta, "ft%", stl, blk, tov, pf, "+/-" '
                f"FROM {ref} WHERE ({where_clause})"
            )

    # Raw rows only: one row per player per season; all stats come from the tables (no SUM/CAST in SQL).
    union_inner = " UNION ALL ".join(legs)
    return union_inner + " LIMIT 500"


def _rewrite_career_aggregate_to_by_season(sql_query: str, user_input: str, conn=None) -> str:
    question_text = _extract_current_question_text(user_input)
    q_input = question_text.lower()
    nth_request = _extract_requested_nth_season(question_text)
    rookie_year_request = "rookie year" in q_input
    asks_over_time = _is_over_time_request(question_text)
    mentions_career = re.search(r"\bcaree+r\b", q_input) is not None
    wants_total = _is_explicit_total_request(question_text)
    q = sql_query or ""
    if re.search(r"(?i)\bjoin\b", q):
        return q
    q_lower = q.lower()
    has_union_sum_rollup = ("union all" in q_lower) and ("sum(" in q_lower) and (
        "group by player_name" in q_lower or "group by player" in q_lower
    )
    # Default non-total career asks to by-season rows rather than SUM rollups.
    if mentions_career and not wants_total:
        asks_over_time = True
    if has_union_sum_rollup and not wants_total:
        asks_over_time = True

    named_players = _extract_named_players(user_input, q)
    requested_span = _extract_requested_season_start_span(user_input)
    asks_full_row_player_slice = bool(named_players) and (
        requested_span is not None or nth_request is not None or rookie_year_request
    )
    if not asks_over_time and not asks_full_row_player_slice:
        return sql_query

    if not re.search(
        r"(?i)\b(player_pergame_regularseason_|player_totals_playoffseason_|player_totals_regularseason_|all_players_(?:regular|playoffs)_)\d{4}_\d{4}\b",
        q,
    ):
        return q

    table_type = (
        "playoffs"
        if (
            ("playoff" in q_input)
            or ("postseason" in q_input)
            or ("player_totals_playoffseason_" in q_lower)
            or ("all_players_playoffs_" in q_lower)
        )
        else "regular"
    )

    # Build from detected season tables directly (no SUM/COUNT) for stable season-trend output.
    table_matches = re.findall(
        r"(?i)\b(player_pergame_regularseason_\d{4}_\d{4}|player_totals_playoffseason_\d{4}_\d{4}|"
        r"player_totals_regularseason_\d{4}_\d{4}|all_players_regular_\d{4}_\d{4}|all_players_playoffs_\d{4}_\d{4})\b",
        q,
    )
    unique_tables: dict[str, tuple[int, int]] = {}
    for table_name in table_matches:
        m = re.search(r"_(\d{4})_(\d{4})$", table_name)
        if not m:
            continue
        try:
            unique_tables[table_name] = (int(m.group(1)), int(m.group(2)))
        except Exception:
            continue
    if named_players:
        full_cols = (
            _player_totals_season_columns()
            if (table_type == "playoffs" or wants_total)
            else _player_pergame_regularseason_columns()
        )
        col_sql = ", ".join(full_cols)
        available_starts = _available_season_starts(conn, table_type) if conn is not None else []
        legs = []

        for player_name in named_players:
            player_where = _player_where_clause_for_names([player_name])
            if not player_where:
                continue

            season_starts: list[int] = []
            if nth_request is not None and conn is not None:
                first_start = _first_season_start_for_where(conn, table_type, player_where, available_starts)
                if first_start is not None:
                    target_start = first_start + (nth_request - 1)
                    if not available_starts or target_start in available_starts:
                        season_starts = [target_start]
            elif rookie_year_request and conn is not None:
                first_start = _first_season_start_for_where(conn, table_type, player_where, available_starts)
                if first_start is not None:
                    if requested_span is not None:
                        _, end_year, span_playoffs = requested_span
                        table_type = "playoffs" if span_playoffs else table_type
                        season_starts = [
                            season_start
                            for season_start in range(first_start, end_year + 1)
                            if not available_starts or season_start in available_starts
                        ]
                    else:
                        season_starts = [first_start]
            elif requested_span is not None:
                start_year, end_year, span_playoffs = requested_span
                table_type = "playoffs" if span_playoffs else table_type
                season_starts = [
                    season_start
                    for season_start in range(start_year, end_year + 1)
                    if not available_starts or season_start in available_starts
                ]
            elif unique_tables:
                season_starts = sorted({start for start, _ in unique_tables.values()})

            for start in season_starts:
                end = start + 1
                if table_type == "playoffs":
                    phys = _physical_playoff_summary_table(start, end)
                elif wants_total:
                    phys = f"player_totals_regularseason_{start}_{end}"
                else:
                    phys = _physical_regular_summary_table(start, end)
                ref = _qualified_public_table_ref(phys)
                season_label = f"{start}-{str(end)[-2:]}"
                legs.append(
                    f"SELECT {start} AS season_start, '{season_label}' AS season_label, "
                    f"{col_sql} FROM {ref} WHERE {player_where}"
                )

        if legs:
            return (
                "SELECT DISTINCT season_start, season_label, "
                f"{col_sql} FROM ({' UNION ALL '.join(legs)}) AS by_season LIMIT 500;"
            )

    if not unique_tables:
        return q

    where_match = re.search(
        r"(?is)\bwhere\b\s*(?P<where>.*?)(\bunion\s+all\b|\bgroup\s+by\b|\border\s+by\b|\blimit\b|$)",
        q,
    )
    if not where_match:
        return q
    where_clause = (where_match.group("where") or "").strip()
    if not where_clause:
        return q
    where_clause = re.sub(r"(?i)\bplayer_name\b", "player", where_clause)

    cols = _columns_for_by_season_question(question_text)
    col_sql = ", ".join(cols)
    sorted_tables = sorted(unique_tables.items(), key=lambda kv: kv[1][0])

    legs = []
    for table_name, (start, end) in sorted_tables:
        season_label = f"{start}-{str(end)[-2:]}"
        low = table_name.lower()
        if "playoffseason" in low or "all_players_playoffs" in low:
            phys = _physical_playoff_summary_table(start, end)
        elif "player_totals_regularseason" in low:
            phys = f"player_totals_regularseason_{start}_{end}"
        else:
            phys = _physical_regular_summary_table(start, end)
        ref = _qualified_public_table_ref(phys)
        legs.append(
            f"SELECT {start} AS season_start, '{season_label}' AS season_label, "
            f"player, {col_sql} FROM {ref} WHERE {where_clause}"
        )

    rebuilt = (
        f"SELECT DISTINCT season_start, season_label, player, {col_sql} "
        f"FROM ({' UNION ALL '.join(legs)}) AS by_season "
        f"ORDER BY season_start ASC LIMIT 50;"
    )
    return rebuilt


def _ensure_rebounding_leaderboard_columns(sql_query: str, user_input: str) -> str:
    q_input = _extract_current_question_text(user_input).lower()
    asks_top = any(k in q_input for k in ["top ", "best ", "leading ", "leaders", "leaderboard"])
    asks_reb = any(k in q_input for k in ["rebound", "boards", "glass", "rebounding"])
    if not asks_top or not asks_reb:
        return sql_query

    q = sql_query or ""
    select_match = re.search(r"(?is)\bselect\b(?P<select_part>.*?)\bfrom\b", q)
    if not select_match:
        return q

    select_part = select_match.group("select_part")
    start, end = select_match.span("select_part")
    required = ["player", "team", "reb", "oreb", "dreb", "gp"]
    missing = [col for col in required if not re.search(rf"(?i)\b{re.escape(col)}\b", select_part)]
    if not missing:
        return q

    injected = select_part.rstrip() + ", " + ", ".join(missing) + " "
    q = q[:start] + injected + q[end:]

    if "order by" not in q.lower():
        q = q.rstrip().rstrip(";") + " ORDER BY reb DESC NULLS LAST;"
    return q


def _ensure_assist_leaderboard_columns(sql_query: str, user_input: str) -> str:
    q_input = _extract_current_question_text(user_input).lower()
    asks_top = any(k in q_input for k in ["top ", "best ", "leading ", "leaders", "leaderboard"])
    asks_ast = any(k in q_input for k in ["assist", "playmaker", "passing"])
    if not asks_top or not asks_ast:
        return sql_query

    q = sql_query or ""
    select_match = re.search(r"(?is)\bselect\b(?P<select_part>.*?)\bfrom\b", q)
    if not select_match:
        return q
    select_part = select_match.group("select_part")
    start, end = select_match.span("select_part")
    required = ["player", "team", "ast", "tov", "gp"]
    missing = [col for col in required if not re.search(rf"(?i)\b{re.escape(col)}\b", select_part)]
    if not missing:
        return q
    injected = select_part.rstrip() + ", " + ", ".join(missing) + " "
    q = q[:start] + injected + q[end:]
    if "order by" not in q.lower():
        q = q.rstrip().rstrip(";") + " ORDER BY ast DESC NULLS LAST;"
    return q


def _enforce_numeric_leaderboard_sort(sql_query: str, user_input: str) -> str:
    q_input = _extract_current_question_text(user_input).lower()
    asks_top = any(k in q_input for k in ["top ", "best ", "leading ", "leaders", "leaderboard", "most ", "highest "])
    q = sql_query or ""
    leaderboard_sql_match = re.search(
        r'(?is)\bfrom\b.*?(player_pergame_regularseason_|player_totals_regularseason_|player_totals_playoffseason_).*?\border\s+by\s+("?)(pts|reb|ast)\1\b',
        q,
    )
    if not asks_top and not leaderboard_sql_match:
        return sql_query
    sort_col = None
    if any(k in q_input for k in ["scorer", "scoring", "points", "ppg"]):
        sort_col = "pts"
    elif any(k in q_input for k in ["rebound", "boards", "glass", "rebounding"]):
        sort_col = "reb"
    elif any(k in q_input for k in ["assist", "playmaker", "passing"]):
        sort_col = "ast"
    if not sort_col and leaderboard_sql_match:
        sort_col = (leaderboard_sql_match.group(2) or "").lower()
    if not sort_col:
        return q

    # Ensure numeric ordering even if DB column type is text.
    numeric_expr = (
        f"NULLIF(regexp_replace({sort_col}::text, '[^0-9\\.-]', '', 'g'), '')::double precision"
    )
    if re.search(r"(?i)\border\s+by\b", q):
        # Replace any existing ORDER BY expression for leaderboard asks so text-typed
        # numeric columns (e.g. 'pts' as TEXT) cannot sort lexicographically.
        q = re.sub(
            r"(?is)\border\s+by\b.*?(?=\blimit\b|$)",
            f"ORDER BY {numeric_expr} DESC NULLS LAST ",
            q,
            count=1,
        )
    else:
        q = q.rstrip().rstrip(";") + f" ORDER BY {numeric_expr} DESC NULLS LAST"
    if not re.search(r"(?i)\blimit\b", q):
        q += " LIMIT 10"
    return q.rstrip() + ";"


def _enforce_top_scorers_query(sql_query: str, user_input: str, schema_description: str = "") -> str:
    q_text = _extract_current_question_text(user_input).lower()
    asks_top = any(k in q_text for k in ["top ", "best ", "leading ", "leaders", "leaderboard", "most ", "highest "])
    asks_scorers = any(k in q_text for k in ["scorer", "scoring", "points", "ppg", "point"])
    if not (asks_top and asks_scorers):
        return sql_query

    start, end, is_playoffs = _extract_requested_season_window(user_input)
    prefix = "player_totals_playoffseason_" if is_playoffs else "player_pergame_regularseason_"
    table = _resolve_table_for_season(prefix, start, end, schema_description)
    if not table:
        return sql_query

    lim = 10
    m = re.search(r"(?i)\btop\s+(\d+)\b", q_text)
    if m:
        try:
            lim = max(1, min(100, int(m.group(1))))
        except Exception:
            lim = 10

    pts_num = "NULLIF(regexp_replace(pts::text, '[^0-9\\.-]', '', 'g'), '')::double precision"
    return (
        "SELECT player, team, pts, gp, \"fg%\", \"3p%\", \"3pm\", \"3pa\", ftm, fta, \"ft%\" "
        f"FROM {_qualified_public_table_ref(table)} "
        f"ORDER BY {pts_num} DESC NULLS LAST LIMIT {lim};"
    )


def _enforce_hidden_efficiency_query(sql_query: str, user_input: str, schema_description: str = "") -> str:
    q_text = _extract_current_question_text(user_input).lower()
    asks_hidden_efficiency = (
        ("hidden" in q_text and "efficien" in q_text)
        or ("non-star" in q_text and "impact" in q_text)
        or ("box score" in q_text and "impact" in q_text and "efficien" in q_text)
    )
    if not asks_hidden_efficiency:
        return sql_query

    start, end, _ = _extract_requested_season_window(user_input)
    existing = _extract_schema_table_names(schema_description)
    adv_table = None
    pg_table = None

    # Prefer a season suffix where both required families exist to avoid empty joins.
    candidate_pairs = [(start, end), (start - 1, end - 1), (start + 1, end + 1)]
    for s, e in candidate_pairs:
        adv_candidate = f"advance_totals_regularseason_{s}_{e}"
        pg_candidate = f"player_pergame_regularseason_{s}_{e}"
        if adv_candidate in existing and pg_candidate in existing:
            adv_table = adv_candidate
            pg_table = pg_candidate
            break

    if not adv_table or not pg_table:
        adv_table = _resolve_table_for_season(
            "advance_totals_regularseason_", start, end, schema_description
        )
        pg_table = _resolve_table_for_season(
            "player_pergame_regularseason_", start, end, schema_description
        )
    if not adv_table or not pg_table:
        return sql_query

    # "Hidden efficiency monsters": high impact/efficiency but not top-scoring raw box output.
    return (
        "SELECT pg.player AS player_name, pg.team AS team_abbreviation, pg.gp, pg.pts, pg.reb, pg.ast, "
        "adv.ts, adv.usg, adv.offrtg, adv.defrtg, adv.netrtg, adv.pie "
        f"FROM {_qualified_public_table_ref(pg_table)} pg "
        f"JOIN {_qualified_public_table_ref(adv_table)} adv ON pg.player = adv.player AND pg.team = adv.team "
        "WHERE NULLIF(regexp_replace(pg.gp::text, '[^0-9\\.-]', '', 'g'), '')::double precision >= 20 "
        "AND NULLIF(regexp_replace(pg.pts::text, '[^0-9\\.-]', '', 'g'), '')::double precision < 28 "
        "AND NULLIF(regexp_replace(adv.ts::text, '[^0-9\\.-]', '', 'g'), '')::double precision >= 54 "
        "AND NULLIF(regexp_replace(adv.pie::text, '[^0-9\\.-]', '', 'g'), '')::double precision IS NOT NULL "
        "ORDER BY NULLIF(regexp_replace(adv.pie::text, '[^0-9\\.-]', '', 'g'), '')::double precision DESC NULLS LAST "
        "LIMIT 12;"
    )


def _enforce_high_usg_ts_query(sql_query: str, user_input: str, schema_description: str = "") -> str:
    q_text = _extract_current_question_text(user_input).lower()
    asks_combo = (
        (("usg" in q_text or "usage" in q_text) and ("ts" in q_text or "true shooting" in q_text))
        or ("high usg" in q_text and "high ts" in q_text)
    )
    if not asks_combo:
        return sql_query

    start, end, _ = _extract_requested_season_window(user_input)
    table = _resolve_table_for_season(
        "advance_totals_regularseason_", start, end, schema_description
    )
    if not table:
        return sql_query

    return (
        "SELECT player, team, gp, ts, usg, offrtg, defrtg, netrtg, pie "
        f"FROM {_qualified_public_table_ref(table)} "
        "WHERE NULLIF(regexp_replace(gp::text, '[^0-9\\.-]', '', 'g'), '')::double precision >= 20 "
        "AND NULLIF(regexp_replace(ts::text, '[^0-9\\.-]', '', 'g'), '')::double precision >= 56 "
        "AND NULLIF(regexp_replace(usg::text, '[^0-9\\.-]', '', 'g'), '')::double precision >= 26 "
        "ORDER BY NULLIF(regexp_replace(ts::text, '[^0-9\\.-]', '', 'g'), '')::double precision DESC NULLS LAST, "
        "NULLIF(regexp_replace(usg::text, '[^0-9\\.-]', '', 'g'), '')::double precision DESC NULLS LAST "
        "LIMIT 12;"
    )


def _enforce_rebounders_query(sql_query: str, user_input: str, schema_description: str = "") -> str:
    q_text = _extract_current_question_text(user_input).lower()
    asks_rebounders = (
        ("rebounder" in q_text or "rebounders" in q_text or "best rebound" in q_text or "top rebound" in q_text)
        and ("regular season" in q_text or "season" in q_text or re.search(r"\b(19|20)\d{2}", q_text))
    )
    if not asks_rebounders:
        return sql_query

    start, end, _ = _extract_requested_season_window(user_input)
    table = _resolve_table_for_season(
        "player_pergame_regularseason_", start, end, schema_description
    )
    if not table:
        return sql_query

    return (
        "SELECT player, team, gp, reb, oreb, dreb, pts, ast "
        f"FROM {_qualified_public_table_ref(table)} "
        "WHERE NULLIF(regexp_replace(gp::text, '[^0-9\\.-]', '', 'g'), '')::double precision >= 20 "
        "ORDER BY NULLIF(regexp_replace(reb::text, '[^0-9\\.-]', '', 'g'), '')::double precision DESC NULLS LAST "
        "LIMIT 10;"
    )


def _ensure_all_players_broad_columns(sql_query: str, user_input: str) -> str:
    q = sql_query or ""
    q_lower = q.lower()
    # Restrict to simple single-season season-summary selects.
    if (
        "player_pergame_regularseason_" not in q_lower
        and "player_totals_playoffseason_" not in q_lower
        and "player_totals_regularseason_" not in q_lower
        and "all_players_regular_" not in q_lower
        and "all_players_playoffs_" not in q_lower
    ):
        return q
    # Never inject base-table columns into derived/subquery shapes like by_season.
    if " as by_season" in q_lower or re.search(r"(?is)\bfrom\s*\(", q):
        return q
    if any(k in q_lower for k in [" sum(", " avg(", " count(", " group by ", " union all ", " order by ", " limit "]):
        return q

    select_match = re.search(r"(?is)\bselect\b(?P<select_part>.*?)\bfrom\b", q)
    if not select_match:
        return q
    select_part = select_match.group("select_part")
    start, end = select_match.span("select_part")
    required_columns = (
        _player_totals_season_columns()
        if "player_totals_" in q_lower
        else _player_pergame_regularseason_columns()
    )
    missing = [c for c in required_columns if not _sql_select_mentions_column(select_part, c)]
    if not missing:
        return q
    injected = select_part.rstrip() + ", " + ", ".join(missing) + " "
    return q[:start] + injected + q[end:]


def _expand_player_name_filters_for_encoding(sql_query: str) -> str:
    """
    Expand exact full-name ILIKE filters with a robust fallback on the `player` column:
      player ILIKE '%First Last%'
    becomes:
      (player ILIKE '%First Last%' OR
       (player ILIKE '%First%' AND player ILIKE '%LastPrefix%'))

    This helps match mojibake/diacritics corruption in DB values
    (e.g., Jokić stored as JokiÄ) without modifying database data.
    """
    if not sql_query:
        return sql_query

    pattern = re.compile(r"(?i)(?:(?P<alias>[a-zA-Z_][a-zA-Z0-9_]*)\.)?(?P<col>player|player_name)\s+ILIKE\s+'%([^%']+)%'")

    def repl(match: re.Match) -> str:
        alias = match.group("alias")
        base_col = match.group("col")
        col = f"{alias}.{base_col}" if alias else base_col
        raw_name = match.group(3).strip()
        parts = [p for p in raw_name.split() if p]
        if len(parts) < 2:
            alias_variants = _PLAYER_ALIAS_MAP.get(raw_name.lower())
            if not alias_variants:
                return match.group(0)
            clauses = []
            seen = set()
            for variant in alias_variants:
                v = (variant or "").strip()
                if not v:
                    continue
                key = v.lower()
                if key in seen:
                    continue
                seen.add(key)
                clauses.append(f"{col} ILIKE '%{v}%'")
            if not clauses:
                return match.group(0)
            return "(" + " OR ".join(clauses) + ")"

        first = parts[0]
        last = parts[-1]

        # Keep only letters for prefix logic, but preserve original full-name match too.
        last_clean = re.sub(r"[^A-Za-z]", "", last)
        if len(last_clean) < 3:
            return match.group(0)

        last_prefix = last_clean[:4]
        full_clause = f"{col} ILIKE '%{raw_name}%'"
        fallback_clause = f"({col} ILIKE '%{first}%' AND {col} ILIKE '%{last_prefix}%')"
        return f"({full_clause} OR {fallback_clause})"

    return pattern.sub(repl, sql_query)


def _ensure_profile_columns_in_sql(sql_query: str, user_input: str) -> str:
    if not _is_single_player_profile_request(user_input):
        return sql_query

    q = sql_query or ""
    q_lower = q.lower()
    # Restrict safeguard to simple single-season summary-table profile selects.
    if "all_players_regular_" not in q_lower and "all_players_playoffs_" not in q_lower:
        return q
    if "player_name ilike" not in q_lower:
        return q
    if any(k in q_lower for k in [" group by ", " union ", "sum(", " order by ", " limit "]):
        return q

    select_match = re.search(r"(?is)\bselect\b(?P<select_part>.*?)\bfrom\b", q)
    if not select_match:
        return q

    select_part = select_match.group("select_part")
    start, end = select_match.span("select_part")
    if "player_totals_" in q_lower:
        required_columns = ["min", "fgm", "fga", "c_3pm", "c_3pa", "ftm", "fta"]
    else:
        required_columns = ["min", "fgm", "fga", '"3pm"', '"3pa"', "ftm", "fta", '"fg%"', '"3p%"', '"ft%"']
    missing = []
    for col in required_columns:
        if not _sql_select_mentions_column(select_part, col):
            missing.append(col)

    if not missing:
        return q

    injected = select_part.rstrip() + ", " + ", ".join(missing) + " "
    return q[:start] + injected + q[end:]


def _ensure_season_columns_in_sql(sql_query: str) -> str:
    """
    Ensure season context is always present for season-summary table queries so
    the analyzer can name the exact referenced season from returned rows.
    """
    q = sql_query or ""
    if not q:
        return q

    leg_pattern = re.compile(
        r"(?is)(select\s+)(?P<select_part>.*?)(\s+from\s+)"
        r"(?P<table_name>(?:player_pergame_regularseason|player_totals_playoffseason|player_totals_regularseason|all_players_(?:regular|playoffs))_(?P<start>\d{4})_(?P<end>\d{4}))"
        r"(?P<rest>\s+where\s+.*?)(?=(\bunion\s+all\b|$))"
    )

    def _rewrite_leg(match: re.Match):
        select_part = match.group("select_part") or ""
        start = int(match.group("start"))
        end = int(match.group("end"))
        season_label = f"{start}-{str(end)[-2:]}"

        updated_select = select_part
        if not re.search(r"(?i)\bseason_start\b", updated_select):
            updated_select = updated_select.rstrip() + f", {start} AS season_start"
        if not re.search(r"(?i)\bseason_label\b", updated_select):
            updated_select = updated_select.rstrip() + f", '{season_label}' AS season_label"

        return (
            f"{match.group(1)}{updated_select}{match.group(3)}"
            f"{match.group('table_name')}{match.group('rest')}"
        )

    return leg_pattern.sub(_rewrite_leg, q)


def _enforce_raw_data_only_sql(sql_query: str) -> str:
    """
    Enforce raw-data SQL:
    - no ORDER BY/GROUP BY/HAVING
    - no aggregate/math/window functions in SELECT
    """
    q = (sql_query or "").strip()
    if not q:
        return q

    q = q.rstrip(";")

    summary_tables = re.search(
        r"(?i)player_pergame_regularseason_|player_totals_(?:playoff|regular)season_",
        q,
    )
    if not re.search(r"(?i)\w+_rank\b", q) and not summary_tables:
        q = re.sub(r"(?is)\border\s+by\b.*?(?=(\blimit\b|$))", " ", q)
    
    q = re.sub(r"(?is)\bgroup\s+by\b.*?(?=(\bhaving\b|\blimit\b|$))", " ", q)
    q = re.sub(r"(?is)\bhaving\b.*?(?=(\blimit\b|$))", " ", q)

    select_match = re.search(r"(?is)\bselect\b(?P<select_part>.*?)\bfrom\b", q)
    if not select_match:
        raise ValueError("Generated SQL is missing SELECT/FROM structure.")

    select_part = select_match.group("select_part")
    select_lower = select_part.lower()
    banned_functions = [
        "sum(",
        "avg(",
        "count(",
        "min(",
        "max(",
        "cast(",
        "coalesce(",
        "nullif(",
        "round(",
        "extract(",
        "date_trunc(",
        "row_number(",
        "rank(",
        "dense_rank(",
        "lag(",
        "lead(",
        "over(",
    ]
    if any(fn in select_lower for fn in banned_functions):
        raise ValueError("Generated SQL contains disallowed functions in SELECT.")

    # Strip string literals and double-quoted identifiers (e.g. "+/-", "fg%") so we do not
    # treat punctuation inside column names as arithmetic.
    select_without_strings = re.sub(r"'[^']*'", "", select_part)
    select_without_strings = re.sub(r'"[^"]*"', "", select_without_strings)
    # Preserve plus/minus stat token if it appears unquoted in model output.
    # Example: SELECT player, +/- FROM ...
    select_without_strings = re.sub(
        r"(?i)(?<!\w)(?:[a-z_][a-z0-9_]*\.)?\+/\-(?!\w)",
        "PLUS_MINUS_TOKEN",
        select_without_strings,
    )
    arithmetic_pattern = re.compile(
        r"(?i)(?:\b[a-z_][a-z0-9_\.]*\b|\b\d+(?:\.\d+)?\b)\s*[+\-*/]\s*(?:\b[a-z_][a-z0-9_\.]*\b|\b\d+(?:\.\d+)?\b)"
    )
    if arithmetic_pattern.search(select_without_strings):
        raise ValueError("Generated SQL contains disallowed arithmetic in SELECT.")

    q = re.sub(r"\s+", " ", q).strip()
    return q + ";"


# 5.5 repair SQL error
def repair_sql_error(original_sql, error_message, schema_description, user_input):
    r_prompt = f"""
The following SQL query failed:

Database schema:
{schema_description}

User request:
"{user_input}"

Failed SQL:
{original_sql}

Database error:
{error_message}

Fix the SQL to match the schema exactly while keeping a RAW DATA query shape.
STRICT RULES:
- ONLY select direct columns from the tables (JOIN across multiple tables is OK when keys exist in schema)
- DO NOT use SUM, AVG, COUNT, MIN, MAX, CAST, NULLIF, COALESCE, window functions, or arithmetic expressions
- DO NOT use GROUP BY, HAVING, or ORDER BY
- Keep WHERE/JOIN only when needed to fetch correct rows and columns
Return ONLY a valid PostgreSQL SELECT query.
Do NOT include any additional text or markdown.
"""

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "Return ONLY valid SQL."},
            {"role": "user", "content": r_prompt}
        ],
        temperature=0,
        max_tokens=1500
    )

    fixed_sql = response.choices[0].message.content.strip()
    fixed_sql = fixed_sql.replace("```sql", "").replace("```", "").strip()
    fixed_sql = _canonicalize_nba_stats_sql(fixed_sql)

    return fixed_sql


# 6. Convert natural language → SQL
def natural_language_to_sql(user_input_param: str):

    user_input_lower = user_input_param.strip().lower()
    if user_input_lower.startswith(('select ', 'with ', 'insert ', 'update ', 'delete ', 'create ', 'drop ', 'alter ')):
        raise ValueError("Error")

    conn = get_connection()
    schema_description = get_db_schema(conn)
    schema_routing_guide = _build_schema_routing_guide(schema_description)

    prompt = f"""
You are a senior SQL data engineer specializing in NBA statistics databases.
Your ONLY task is to convert a natural language request into a VALID PostgreSQL SELECT query.
You MUST follow every rule below without exception. There is no ambiguity — if a rule applies, follow it exactly.

GLOBAL OVERRIDE (HIGHEST PRIORITY):
- Return RAW DATA queries only.
- SELECT only direct existing columns from source table(s), optionally **table-qualified** (e.g. pg.pts, adv.ts).
- When the question clearly needs facts from **more than one table** (different columns or different families), use **one SELECT** with **INNER JOIN** or **LEFT JOIN** between those tables — still no SQL-side aggregates or arithmetic on stats.
- Any compatible table families may be joined together when schema keys exist (not just basic + advanced).
- NEVER use `player_game_logs`; use season-summary tables only.
- DO NOT use SUM, AVG, COUNT, MIN, MAX, CAST, NULLIF, COALESCE, window functions, or arithmetic expressions.
- DO NOT use GROUP BY, HAVING, or ORDER BY.
- The analyzer layer handles sorting, ranking, and math after query execution.

════════════════════════════════════════════════════════════════════════
SECTION 1: THE TWO TABLE TYPES — UNDERSTAND THEM COMPLETELY
════════════════════════════════════════════════════════════════════════

TYPE A — SEASON SUMMARY TABLES (NBA-STATS):
  Regular-season per-game summaries: `player_pergame_regularseason_STARTYEAR_ENDYEAR` (ENDYEAR = STARTYEAR + 1).
  Playoff summaries (totals-style column names): `player_totals_playoffseason_STARTYEAR_ENDYEAR`.
  Optional regular-season totals (if listed in schema): `player_totals_regularseason_STARTYEAR_ENDYEAR`.
  Advanced season totals (TS%, USG, ratings, etc.): `advance_totals_regularseason_STARTYEAR_ENDYEAR` or
    `advance_totals_playoffseason_STARTYEAR_ENDYEAR` — always confirm columns from DATABASE SCHEMA below.

  One row per player per season. Filter players with `player ILIKE '%Name%'` (column is `player`, not `player_name`).
  Team abbreviation column is `team` (not `team_abbreviation`).

  On `player_pergame_regularseason_*`, quote identifiers that contain % or + or /:
    "fg%", "3pm", "3pa", "3p%", "ft%", "+/-"
  The "+/-" column is **plus-minus** (team point differential while that player was on the floor for the season)—a real
  box-score-style stat, not arithmetic. In PostgreSQL it must be referenced exactly as the quoted identifier `"+/-"`.
  Base counters: fgm, fga, ftm, fta, oreb, dreb, reb, ast, tov, stl, blk, pf, pts, min, gp, w, l, age, fp, double_double, triple_double.

  On `player_totals_*season_*`, shooting columns are named fg, c_3pm, c_3pa, c_3p, ft (not fg% / 3pm style).
  These summary tables do NOT expose legacy `*_rank` or `player_name` / `team_abbreviation` column names.

  Which season years exist: use the DATABASE SCHEMA block at the end of this prompt (do not invent table years).

  COLUMNS THAT DO NOT EXIST in season summary tables — NEVER USE THEM:
    ❌ season_id       (the season is encoded in the TABLE NAME itself)
    ❌ game_date       (these are season summaries, not individual games)
    ❌ game_id         (no individual game tracking)
    ❌ matchup         (no opponent info)
    ❌ wl              (w and l are separated into two columns)
    ❌ season_type     (regular vs playoffs is encoded in the TABLE NAME)

TYPE B — GAME LOGS TABLE:
  Table name: `player_game_logs` (only ONE table, not split by year)
  What it contains: One row per player per game. Raw box score per game.
  IMPORTANT: This table is the MOST UP TO DATE data source, containing game logs
  through February 2026. It covers the full 2025-26 season currently in progress.
  Use this table whenever the user wants current, recent, or live-season data.

  COLUMNS THAT EXIST in player_game_logs:
    player_id, player_name, team_abbreviation, game_id, game_date, season_id,
    season_type, matchup, wl, pts, ast, reb, stl, blk, tov, fgm, fga,
    fg3m, fg3a, ftm, fta, min

  COLUMNS THAT DO NOT EXIST in player_game_logs — NEVER USE THEM:
    ❌ fg_pct / fg3_pct / ft_pct as SQL expressions — not in this table; select fgm, fga, fg3m, fg3a, ftm, fta only (or use a season summary table for "fg%" columns).
    ❌ age             (not tracked per game)
    ❌ gp              (not a column — count rows instead: COUNT(*) AS games_played)
    ❌ oreb            (offensive rebounds not tracked separately)
    ❌ dreb            (defensive rebounds not tracked separately)
    ❌ nickname        (not in this table)
    ❌ +/- (plus-minus) — not in player_game_logs; for season-level plus-minus use player_pergame_regularseason_*
    ❌ double_double, triple_double        (not in this table)
    ❌ any _rank columns (none of the rank columns exist here)
    ❌ team_id         (not in this table)
    ❌ team_count      (not in this table)

TYPE C — EXTENDED NBA DATA FAMILIES (from current_working_data schema):
  These tables exist and should be used when user intent clearly matches them:
  - `nba__advanced__...`  (advanced metrics / impact context)
  - `nba__clutch__...`    (late-game / clutch situations)
  - `nba__hustle__...`    (hustle events: deflections, contested stats, etc.)
  - `nba__lineups__...`   (lineup combinations and lineup performance)
  - `nba__schedule__...`  (game schedules)
  - `nba__standings__...` (team standings / rank / records)

  IMPORTANT:
  - These tables are highly structured by name (season, season_type, per_mode, endpoint naming).
  - ALWAYS rely on DATABASE SCHEMA below to pick exact columns; never invent columns.
  - Clutch intent maps to last-5-minutes game context.
  - `clutch_totals_*` values are totals from clutch situations; treat them as totals (do not reinterpret as per-game unless explicitly requested).
  - If user asks for clutch/hustle/lineup/schedule/standings, do NOT force player_pergame_regularseason_* or player_game_logs.
  - Use table-name pattern matching by intent first, then schema-confirmed columns.

════════════════════════════════════════════════════════════════════════
SECTION 2: TABLE SELECTION RULES — FOLLOW IN ORDER, FIRST MATCH WINS
════════════════════════════════════════════════════════════════════════

RULE 0 — INTENT ROUTING FOR EXTENDED TABLE FAMILIES:
  If question clearly targets one of these domains, use that family first:
  - "clutch", "in close games", "last 5 minutes"        → `clutch_totals_regularseason_*` / `clutch_totals_playoffseason_*`
                                                          (clutch means last 5 minutes context; table values are totals, not per-game rates)
  - "hustle", "deflections", "box outs", "contested"    → `nba__hustle__...`
  - "lineup", "5-man unit", "best lineup", "on/off 5"   → `nba__lineups__...`
  - "schedule", "next games", "calendar"                → `nba__schedule__...`
  - "standings", "seed", "conference rank", "record"    → `nba__standings__...`
  - "advanced metrics", "advanced stats profile"         → `advance_totals_*` tables from schema
  - "defense metrics", "defensive profile", "defense totals" → `defense_totals_regularseason_*` tables from schema
  - "violations", "violation totals", "voilations"       → `violations_totals_regularseason_*` or `violations_totals_playoffseason_*` based on context
  Use player_pergame_regularseason_* / player_totals_playoffseason_* for season-summary player stats.
  Use player_game_logs only when question is game-by-game recency/log context.
  If no season/year is provided for extended families, default to the latest available season in schema
  (for this project default target is 2025_2026 when present).

RULE 1 — MOST RECENT PLAYOFF PERFORMANCE (most common case):
  Trigger phrases: "playoff performance", "playoffs", "analyze playoffs", "postseason",
                   "how did X do in the playoffs", "X playoff stats", "X in the playoffs"
  With NO specific year or "all time" mentioned:
  → ALWAYS use the latest `player_totals_playoffseason_YYYY_YYYY` listed in DATABASE SCHEMA (e.g. 2025_2026 when present)
  → For a single season row per player, SELECT columns directly (no SUM/GROUP BY on one table).
  → NEVER use player_game_logs for this
  → NEVER guess an older year like 2018_2019 or 2023_2024
  Example question: "Analyze Giannis playoff performance"
  Example question: "How did Jayson Tatum do in the playoffs"
  Example question: "Show me Steph Curry's playoff stats"
  Correct table pattern: player_totals_playoffseason_2025_2026 (adjust year to schema)

RULE 2 — MOST RECENT REGULAR SEASON PERFORMANCE (season summary):
  Trigger phrases: "season stats", "season averages", "how did X do this season",
                   "season performance", "season totals", top scorers, leaderboards,
                   questions needing oreb, dreb, "+/-", age, gp
  With NO specific year mentioned:
  → Use the latest `player_pergame_regularseason_YYYY_YYYY` from DATABASE SCHEMA
  → Order leaderboards with ORDER BY stat DESC (there are no *_rank columns)
  → NEVER use player_game_logs for leaderboards or rank-based questions
  Example question: "Who are the top 10 scorers this season"
  Example question: "Show me the league leaders in assists"
  Correct table pattern: player_pergame_regularseason_2025_2026 (adjust year to schema)

RULE 3 — CURRENT FORM / RECENT ACTIVITY (use player_game_logs):
  Trigger phrases: "lately", "recently", "how is X playing", "current form",
                   "this season so far", "how has X been", "is X hot", "is X cold",
                   "last X games", "past X games", "recent games", "game log",
                   "game by game", "hot streak", "cold streak", "this week",
                   "last week", "last night", "tonight", "last month",
                   "matchup history", "vs [team]", "against [team]"
  → ALWAYS use player_game_logs — it has data through February 2026
  → For 2025-26 season: WHERE season_id = '22025' AND season_type = 'Regular Season'
  → For 2025 playoffs: WHERE season_id = '22025' AND season_type = 'Playoffs'
  → ALWAYS ORDER BY game_date DESC when recency matters
  → SELECT raw box columns only (fgm, fga, fg3m, fg3a, ftm, fta, pts, etc.) — do NOT use CAST, division, or made-up fg_pct columns in SQL.
  → Use LIMIT to restrict to the number of games requested (e.g., last 10 → LIMIT 10)
  Example question: "Show me LeBron's last 10 games"
  Example question: "How has Steph been playing lately"
  Example question: "Is Giannis on a hot streak"
  Correct table: player_game_logs WHERE season_id = '22025'

RULE 4 — SPECIFIC YEAR OR SEASON REQUESTED:
  If user mentions a specific year like "2019", "2022-23", "last year", "2018 playoffs":
  → Map to the correct table using this logic:
      START-YEAR RULE (regular season): a bare year means the season that STARTS that year.
        "2020" or "2020 season"        → player_pergame_regularseason_2020_2021
        "2016 season"                  → player_pergame_regularseason_2016_2017
      PLAYOFF EXCEPTION: a playoff year refers to playoffs at the END of that season.
        "2016 playoffs"                → player_totals_playoffseason_2015_2016
        "2020 playoffs"                → player_totals_playoffseason_2019_2020
      Explicit ranges still map directly by start and end:
        "2018-19 playoffs"             → player_totals_playoffseason_2018_2019
        "2022-23 season"               → player_pergame_regularseason_2022_2023
      Relative references:
        "last season" (current year is 2026)   → player_pergame_regularseason_2024_2025 (if in schema)
  → Regular per-game: player_pergame_regularseason_START_END; Playoffs: player_totals_playoffseason_START_END
  → The END year is always START + 1 (four-digit years in the table name).
  Example question: "How did Kobe do in the 2009 playoffs"
  Correct table: player_totals_playoffseason_2008_2009

RULE 5 — CAREER / ALL TIME STATS:
  Trigger phrases: "career", "all time", "entire career", "over his career",
                   "throughout his career", "historically", "all seasons"
  → Use UNION ALL across ALL available yearly tables for that player's era
  → NEVER use player_game_logs for career stats
  → Return raw rows only (same columns each leg); do NOT wrap in SUM()/AVG()/GROUP BY in SQL.
  Example question: "Show me LeBron's career regular season stats"
  → UNION ALL across every player_pergame_regularseason_* table in schema for that era

RULE 5B — OVER-TIME / BY-SEASON TRENDS (NOT CAREER AGGREGATE):
  Trigger phrases: "by season", "per season", "season by season", "over the years",
                   "through the years", "year by year", "across seasons", "trend over time",
                   "over his career" / "throughout his career" when asking for rate stats,
                   "rookie year to YYYY", "2010s decade", or explicit ranges like "from 2012 to 2024"
  → Use UNION ALL across relevant season tables, but return ONE ROW PER SEASON (no rollup).
  → Do NOT use SUM(), AVG(), COUNT(*), or GROUP BY player for this intent.
  → If model produced SUM()+GROUP BY over UNION for these asks, rewrite to per-season direct columns.
  → Pull per-season columns directly from each season table (including gp for games played).
  Example question: "Show me LeBron's blocks per season"
  → Return season_start, season_label, player, blk, gp by season order.

RULE 6 — COMPARING TWO OR MORE PLAYERS WHEN A SEASON IS SPECIFIED ("this season", "last season", explicit year/range):
  → Use **one primary** season summary table for all players in that query (same season suffix in table names).
  → If the question also needs columns only on another table (e.g. advanced row for the same season), **JOIN** that second table on keys listed in DATABASE SCHEMA (typically same `player` / `team` and matching `_YYYY_YYYY` table pair).
  → Use OR with ILIKE for multiple players:
     player ILIKE '%LeBron%' OR player ILIKE '%Curry%'
  → NEVER use player_game_logs for season-level comparisons
  Example question: "Compare LeBron and Curry this season"
  Correct table: player_pergame_regularseason_2025_2026 (adjust to schema)

RULE 6B — WHO IS BETTER / BETWEEN A AND B WITH NO YEAR OR SEASON CONTEXT:
  If the question compares players or stats — who is better, better rebounder/scorer/player, between X and Y, vs., than —
  and it does NOT mention any specific season/year, ordinal season (first/second season, rookie/sophomore year),
  decade, or dated range ("last season", "2023-24", "this year"):
  → Treat it as CAREER comparison (regular season unless they clearly mean playoffs).
  → UNION ALL across every `player_pergame_regularseason_*` table in the schema for those players' careers.
  → Return raw per-season rows per player (no SQL aggregation); the app compares rows downstream.
  Example question: "Who is the better rebounder between Bol Bol and Anthony Davis"
  → UNION ALL of per-season reb, gp, pts, etc. for each player across seasons.

RULE 7 — COMPARING TWO PLAYERS (different eras / career):
  → Use UNION ALL — one SELECT per player from their respective era tables
  → Do not add an outer SELECT with SUM or GROUP BY
  Example question: "Compare LeBron and Jordan career stats"
  → LeBron from player_pergame_regularseason_2003_2004 through latest in schema
  → Jordan from player_pergame_regularseason_1996_1997 through 2002_2003

RULE 8 — TOP PLAYERS / LEADERBOARD QUESTIONS:
  → Use season summary tables only (player_pergame_regularseason_* or player_totals_playoffseason_*)
  → NEVER use player_game_logs for rankings
  → For single-season leaderboard questions, query **one** season stats table directly **unless** the user asks for extra fields only on another table — then JOIN that table (same season/year in names) without changing leaderboard logic.
  → Do NOT use SUM(), GROUP BY, or UNION unless the user explicitly asks for career/all-time across seasons.
  → There are no *_rank columns — ORDER BY the stat column DESC (e.g. ORDER BY pts DESC NULLS LAST).
  → For top scorers specifically, use pts, "fg%", "3p%", gp from that season table.
  → If the user asks for "top scorers", "best scorers", or "leading scorers":
      - SELECT from one player_pergame_regularseason_YYYY_YYYY table (add JOIN only if the question explicitly needs non-overlapping columns from another table)
      - include player, team, pts, gp, "fg%", "3p%", "3pm", "3pa", ftm, fta, "ft%"
      - use DISTINCT to prevent duplicate player rows when source data has repeats
      - ORDER BY pts DESC NULLS LAST
      - use LIMIT requested by user; if no number is provided, default to LIMIT 5
  Example question: "Who are the top 10 scorers this season"
  Correct: SELECT player, team, pts, gp, "fg%", "3p%"
           FROM player_pergame_regularseason_2025_2026
           ORDER BY pts DESC NULLS LAST
           LIMIT 10

RULE 9 — PLAYER DID NOT PLAY / ZERO ROWS RETURNED:
  → Do NOT switch tables or guess a different year
  → Return the query as-is and let the application handle the empty result
  → A player not appearing in a playoffs table means they did not make the playoffs that year

RULE 10 — WHEN player_game_logs IS BETTER THAN SEASON SUMMARY TABLES:
  player_game_logs is updated through February 2026 and is the freshest data available.
  Prefer it over season summary tables when:
  - The user wants anything about the current 2025-26 season on a game-by-game level
  - The user uses words like "lately", "recently", "now", "currently", "this year so far"
  - The user wants "last X games" regardless of season
  - The user wants game dates, opponents, win/loss results
  HOWEVER, keep using player_pergame_regularseason_2025_2026 (or latest in schema) when:
  - The user wants season rankings or leaderboards (ORDER BY stat on summary table)
  - The user needs oreb, dreb, "+/-", age, or gp columns
  - The user asks for season shooting columns directly ("fg%", "3p%", "ft%" on per-game tables)

RULE 11 — SINGLE PLAYER GENERAL STATS PROFILE (season summary):
  Trigger phrases: "what were X stats", "X stats in 20YY", "show X season stats",
                   "player profile", "general stats"
  If this is for one player and one season table:
  → Use ONE player_pergame_regularseason_YYYY_YYYY (or player_totals_playoffseason_* if playoffs) as the base row.
  → If the user also wants advanced-only columns (TS%, USG, ratings, etc.), **JOIN** `advance_totals_*` for the **same** season suffix on join keys from DATABASE SCHEMA (never invent column names).
  → Do NOT use SUM(), GROUP BY, or UNION
  → Return a broad stat set for downstream profile tables:
     player, team, age, gp, min, w, l,
     pts, reb, ast, tov, stl, blk, pf, fgm, fga, "fg%", "3pm", "3pa", "3p%", ftm, fta, "ft%",
     dreb, oreb, double_double, triple_double, "+/-"
  → Use DISTINCT if needed to avoid duplicate player rows

RULE 12 — MULTIPLE TABLES IN ONE QUERY (JOIN):
  Use a JOIN when the user question **clearly combines** information that lives on different tables, for example:
  - Same-season **basic per-game** (`player_pergame_regularseason_YYYY_YYYY`) **plus** **advanced totals** (`advance_totals_regularseason_YYYY_YYYY`) for one player or many players.
  - **NBA__** family table (clutch, hustle, lineups, schedule, standings) **plus** a player/team identifier table from schema when both are required to answer.
  - **player_game_logs** plus another table: only when DATABASE SCHEMA shows a shared key (e.g. `game_id` on both sides). Otherwise use the single table that best matches the question — do not invent join keys.
  Rules:
  - **SELECT list:** only direct columns (qualified aliases allowed: `pg.pts`, `adv.ts` as stored in DB). No aggregates, no arithmetic on stats.
  - **ON clause:** use real columns from schema on both sides; same entity + same season usually means matching `_YYYY_YYYY` across joined NBA-STATS tables and `player` = `player` (or documented equivalent).
  - **UNION ALL** = same-shaped rows across **years** or players in disjoint tables; **JOIN** = **different columns** about the **same** season/entity from compatible tables. Choose the right tool; do not UNION tables with incompatible column sets unless you align columns explicitly with literals only where the prompt already allows (e.g. season_label).
  - Avoid CROSS JOIN and unbounded many-to-many joins; if join cardinality is unclear, narrow with WHERE and LIMIT per user request.

{schema_routing_guide}

════════════════════════════════════════════════════════════════════════
SECTION 3: MANDATORY QUERY CONSTRUCTION RULES
════════════════════════════════════════════════════════════════════════

PLAYER NAME MATCHING:
  - ALWAYS use ILIKE with wildcards on BOTH sides: player ILIKE '%Giannis%'
  - For full names use: player ILIKE '%LeBron James%'
  - On player_game_logs the column may still be player_name — follow DATABASE SCHEMA for that table.
  - NEVER use exact match (=) for player names
  - NEVER use ILIKE 'Jordan%' — this matches Jordan Poole, DeAndre Jordan, etc.
  - For last-name-only queries use a leading space: player ILIKE '% Harris%'
    to reduce false matches like "Gary Harris" when searching just "Harris"
  - Expand ALL nicknames to full names before searching:
      "Steph" or "Steph Curry"   → player ILIKE '%Stephen Curry%' OR player ILIKE '%Steph Curry%'
      "Bron" or "King James"     → player ILIKE '%LeBron James%'
      "Greek Freak"              → player ILIKE '%Giannis%'
      "KD"                       → player ILIKE '%Kevin Durant%'
      "AD"                       → player ILIKE '%Anthony Davis%'
      "Kawhi"                    → player ILIKE '%Kawhi Leonard%'
      "CP3"                      → player ILIKE '%Chris Paul%'
      "Dame"                     → player ILIKE '%Damian Lillard%'
      "Russ"                     → player ILIKE '%Russell Westbrook%'
      "PG" or "PG13"             → player ILIKE '%Paul George%'

AGGREGATION RULES for season summary tables:
  - Do NOT use SUM, AVG, COUNT, GROUP BY, CAST, or division in SQL — every stat must come from a table column.
  - For career or multi-season questions, use UNION ALL of identical SELECT lists (one per season table).
  - On per-game summary tables, quote shooting columns: "fg%", "3p%", "ft%". On player_totals_* use fg, c_3p, ft.

STANDARD PLAYER PERFORMANCE (multi-season, raw rows only):
  Repeat this shape per season and join with UNION ALL (no outer aggregate):
    SELECT player, gp, pts, reb, ast, fgm, fga, "fg%", "3pm", "3pa", "3p%", ftm, fta, "ft%"
    FROM player_pergame_regularseason_YYYY_YYYY
    WHERE player ILIKE '%Name%'

STANDARD SINGLE-SEASON LEADERBOARD BLOCK (NO AGGREGATION):
  Use this for questions like "top scorers in 2001-02", "best scorers this season", "league leaders in points":
    player,
    team,
    pts,
    gp,
    "fg%",
    "3p%",
    "3pm",
    "3pa",
    ftm,
    fta,
    "ft%"
  FROM one player_pergame_regularseason_YYYY_YYYY table only
  ORDER BY pts DESC NULLS LAST

STANDARD GAME LOG SELECT BLOCK:
  Use this exact block when querying player_game_logs for recent/current games:
    player_name,
    game_date,
    matchup,
    wl,
    pts,
    reb,
    ast,
    stl,
    blk,
    tov,
    fgm,
    fga,
    fg3m,
    fg3a,
    ftm,
    fta,
    min

GENERAL:
  - **Multi-table:** Prefer JOIN over omitting part of the question when schema shows join keys and the user asks for stats from two sources (see RULE 12).
  - Do NOT add a generic LIMIT (e.g. LIMIT 50) unless the user asks for top-N, last-X games, or a bounded sample.
  - For shot charts, game logs, or "all shots / full picture" asks, omit LIMIT so results are not arbitrarily truncated (heavy queries are still bounded by server cost/timeout).
  - NEVER use SELECT * — always name columns explicitly
  - NEVER invent column names that are not listed in this prompt
  - NEVER add ORDER BY game_date to season summary tables (game_date does not exist there)
  - NEVER add WHERE season_id = ... to season summary tables (season_id does not exist there)
  - NEVER add WHERE season_type = ... to season summary tables (season_type does not exist there)
  - If a question is ambiguous between recency and season summary, default to player_game_logs
    with season_id = '22025' since it is the most current data available

════════════════════════════════════════════════════════════════════════
SECTION 4: SEASON AND YEAR REFERENCE MAP
════════════════════════════════════════════════════════════════════════

  "current season" or no year specified (regular)  → player_pergame_regularseason_2025_2026 (if in schema)
    **Exception:** Head-to-head "who is better / between X and Y" with NO season/year wording → RULE 6B career UNION ALL (not only latest season).
  "current playoffs" or no year specified (playoff) → player_totals_playoffseason_2024_2025 (latest completed playoffs)
  "clutch"/"hustle"/"lineups"/"schedule"/"standings"/"defense totals" with no year
                                                    → use the matching family table for 2025_2026 when that season suffix exists;
                                                      otherwise use the latest available suffix shown in DATABASE SCHEMA.
  Bare year uses START-YEAR mapping for regular season:
  "2020"                                            → player_pergame_regularseason_2020_2021
  "2018"                                            → player_pergame_regularseason_2018_2019
  "2016"                                            → player_pergame_regularseason_2016_2017

  Playoff year uses END-YEAR mapping:
  "2020 playoffs"                                   → player_totals_playoffseason_2019_2020
  "2016 playoffs"                                   → player_totals_playoffseason_2015_2016

  "last season" / "2024-25"                         → player_pergame_regularseason_2024_2025
  "2023-24" / "last year"                           → player_pergame_regularseason_2023_2024
  "2022-23"                                         → player_pergame_regularseason_2022_2023
  "2021-22"                                         → player_pergame_regularseason_2021_2022
  "2020-21"                                         → player_pergame_regularseason_2020_2021
  "bubble" / "2019-20"                              → player_pergame_regularseason_2019_2020
  "2018-19"                                         → player_pergame_regularseason_2018_2019
  "2017-18"                                         → player_pergame_regularseason_2017_2018
  "2016-17"                                         → player_pergame_regularseason_2016_2017
  "2015-16"                                         → player_pergame_regularseason_2015_2016

  season_id values inside player_game_logs:
    2025-26 season (CURRENT — use this by default): '22025'
    2024-25 season:                                 '22024'
    2023-24 season:                                 '22023'
    2022-23 season:                                 '22022'
  season_type values (EXACT strings, case-sensitive):
    'Regular Season'
    'Playoffs'

════════════════════════════════════════════════════════════════════════
SECTION 5: WORKED EXAMPLES OF CORRECT QUERIES
════════════════════════════════════════════════════════════════════════

Q: "Analyze Giannis playoff performance"
→ RULE 1. No year. Use player_totals_playoffseason_2025_2026 (latest in schema).
SELECT player, team, gp, pts, reb, ast, stl, blk, tov, fgm, fga, fg, c_3pm, c_3pa, c_3p, ftm, fta, ft
FROM player_totals_playoffseason_2025_2026
WHERE player ILIKE '%Giannis%'
LIMIT 50;

Q: "Analyze Garry Harris playoff performance"
→ RULE 1. No year. Use player_totals_playoffseason_2025_2026.
SELECT player, team, gp, pts, reb, ast, stl, blk, tov, fgm, fga, fg, c_3pm, c_3pa, c_3p, ftm, fta, ft
FROM player_totals_playoffseason_2025_2026
WHERE player ILIKE '%Garry Harris%'
LIMIT 50;

Q: "How did Giannis do in the 2019 playoffs"
→ RULE 4. Year specified: 2019 → player_totals_playoffseason_2018_2019.
SELECT player, team, gp, pts, reb, ast, fgm, fga, fg, c_3pm, c_3pa, c_3p, ftm, fta, ft
FROM player_totals_playoffseason_2018_2019
WHERE player ILIKE '%Giannis%'
LIMIT 50;

Q: "Show me LeBron's last 10 games"
→ RULE 3. Recency. Use player_game_logs, season_id = '22025', ORDER BY game_date DESC.
SELECT player_name, game_date, matchup, wl, pts, reb, ast, stl, blk, tov,
  fgm, fga, fg3m, fg3a, ftm, fta, min
FROM player_game_logs
WHERE player_name ILIKE '%LeBron James%'
  AND season_id = '22025'
ORDER BY game_date DESC LIMIT 10;

Q: "How has Steph been playing lately"
→ RULE 3 / RULE 10. Recency keyword. Use player_game_logs, season_id = '22025'.
SELECT player_name, game_date, matchup, wl, pts, reb, ast,
  fgm, fga, fg3m, fg3a, ftm, fta, min
FROM player_game_logs
WHERE (player_name ILIKE '%Stephen Curry%' OR player_name ILIKE '%Steph Curry%')
  AND season_id = '22025'
  AND season_type = 'Regular Season'
ORDER BY game_date DESC LIMIT 15;

Q: "Compare LeBron and Curry this season"
→ RULE 6. Two players, same era, season summary. Use player_pergame_regularseason_2025_2026.
SELECT DISTINCT player, team, gp, pts, reb, ast, fgm, fga, "fg%", "3pm", "3pa", "3p%", ftm, fta, "ft%"
FROM player_pergame_regularseason_2025_2026
WHERE player ILIKE '%LeBron James%' OR player ILIKE '%Stephen Curry%'
LIMIT 50;

Q: "What were Kevin Durant's stats 2015"
→ RULE 4 + RULE 11. Single-player season profile, no aggregation.
SELECT DISTINCT player, team, age, gp, min, w, l,
  pts, reb, ast, tov, stl, blk, pf, fgm, fga, "fg%", "3pm", "3pa", "3p%", ftm, fta, "ft%",
  dreb, oreb, double_double, triple_double, "+/-"
FROM player_pergame_regularseason_2015_2016
WHERE player ILIKE '%Kevin Durant%'
LIMIT 50;

Q: "Who are the top 10 scorers this season"
→ RULE 8. Leaderboard. Use player_pergame_regularseason_2025_2026.
SELECT DISTINCT player, team, pts, gp, "fg%", "3p%", "3pm", "3pa", ftm, fta, "ft%"
FROM player_pergame_regularseason_2025_2026
ORDER BY pts DESC NULLS LAST LIMIT 10;

Q: "Who are the best scorers from 2000-2001"
→ RULE 4 + RULE 8. Specific season leaderboard from one table, no aggregation.
SELECT DISTINCT player, team, pts, gp, "fg%", "3p%", "3pm", "3pa", ftm, fta, "ft%"
FROM player_pergame_regularseason_2000_2001
ORDER BY pts DESC NULLS LAST LIMIT 5;

Q: "Show me Steph Curry's career stats"
→ RULE 5. Career = UNION ALL across yearly tables (raw rows, no SQL aggregates).
SELECT player, gp, pts, reb, ast, fgm, fga, "fg%", "3pm", "3pa", "3p%" FROM player_pergame_regularseason_2012_2013 WHERE player ILIKE '%Stephen Curry%'
UNION ALL
SELECT player, gp, pts, reb, ast, fgm, fga, "fg%", "3pm", "3pa", "3p%" FROM player_pergame_regularseason_2013_2014 WHERE player ILIKE '%Stephen Curry%'
UNION ALL
SELECT player, gp, pts, reb, ast, fgm, fga, "fg%", "3pm", "3pa", "3p%" FROM player_pergame_regularseason_2014_2015 WHERE player ILIKE '%Stephen Curry%'
UNION ALL
SELECT player, gp, pts, reb, ast, fgm, fga, "fg%", "3pm", "3pa", "3p%" FROM player_pergame_regularseason_2025_2026 WHERE player ILIKE '%Stephen Curry%'
LIMIT 500;

Q: "Is Giannis on a hot streak"
→ RULE 3. Streak = game log recency. Use player_game_logs.
SELECT player_name, game_date, matchup, wl, pts, reb, ast, fgm, fga, fg3m, fg3a, ftm, fta, min
FROM player_game_logs
WHERE player_name ILIKE '%Giannis%'
  AND season_id = '22025'
  AND season_type = 'Regular Season'
ORDER BY game_date DESC LIMIT 10;

Q: "Compare LeBron and Jordan career stats"
→ RULE 7. Different eras. UNION ALL per table (raw rows).
SELECT player, gp, pts, reb, ast, fgm, fga, "fg%" FROM player_pergame_regularseason_1996_1997 WHERE player ILIKE '%Michael Jordan%'
UNION ALL
SELECT player, gp, pts, reb, ast, fgm, fga, "fg%" FROM player_pergame_regularseason_1997_1998 WHERE player ILIKE '%Michael Jordan%'
UNION ALL
SELECT player, gp, pts, reb, ast, fgm, fga, "fg%" FROM player_pergame_regularseason_2003_2004 WHERE player ILIKE '%LeBron James%'
UNION ALL
SELECT player, gp, pts, reb, ast, fgm, fga, "fg%" FROM player_pergame_regularseason_2025_2026 WHERE player ILIKE '%LeBron James%'
LIMIT 500;

Q: "Giannis 2024-25 per-game stats and his advanced numbers for that season"
→ RULE 11 + RULE 12. JOIN same-season per-game and advance_totals (confirm join column names in DATABASE SCHEMA; often `player` on both sides).
SELECT pg.player, pg.team, pg.gp, pg.pts, pg.reb, pg.ast, pg."fg%", adv.ts
FROM player_pergame_regularseason_2024_2025 pg
INNER JOIN advance_totals_regularseason_2024_2025 adv ON pg.player = adv.player
WHERE pg.player ILIKE '%Giannis%'
LIMIT 50;

════════════════════════════════════════════════════════════════════════
SECTION 6: COMMON MISTAKES — NEVER DO THESE
════════════════════════════════════════════════════════════════════════

❌ SELECT season_id FROM player_totals_playoffseason_2025_2026    -- does not exist in summary tables
❌ SELECT game_date FROM player_pergame_regularseason_2025_2026     -- does not exist in summary tables
❌ SELECT fg_pct FROM player_game_logs                     -- not a column; use fgm, fga or a summary table
❌ SELECT oreb FROM player_game_logs                       -- does not exist in game logs
❌ WHERE season_type = 'Playoffs' on a summary table       -- column does not exist there
❌ WHERE season_id = '22025' on a summary table            -- column does not exist there
❌ player ILIKE 'Jordan%' on summary tables                 -- matches wrong players
❌ player ILIKE 'Harris'                                   -- missing wildcards
❌ fgm / fga or CAST(...)/NULLIF in SQL                    -- no arithmetic in SELECT (use table columns)
❌ SELECT * FROM any table                                  -- always name columns explicitly
❌ Using wrong playoff season table when no year given       -- default to latest player_totals_playoffseason_* in schema
❌ Using player_game_logs for "analyze playoff performance" -- use season summary tables
❌ SUM(...), GROUP BY, or AVG(...) in SQL                  -- read stats from table columns only
❌ ORDER BY game_date on a season summary table            -- game_date does not exist there
❌ WHERE season_id = '22024' for current 2025-26 games     -- current season is '22025'
❌ Using player_pergame_regularseason_2024_2025 for "last X games"  -- no game_date column there
❌ Assuming player_game_logs is outdated                   -- it has data through Feb 2026
❌ SELECT gp FROM player_game_logs                         -- use COUNT(*) AS games_played
❌ Ignoring a second table when the user explicitly asks for fields only found there -- use RULE 12 JOIN if schema keys match

════════════════════════════════════════════════════════════════════════
OUTPUT FORMAT
════════════════════════════════════════════════════════════════════════
Return ONLY the raw SQL query.
No explanation. No markdown. No backticks. No comments. No preamble.
The query must be directly executable in PostgreSQL as-is.

DATABASE SCHEMA:
{schema_description}

USER REQUEST:
{user_input_param}

Generate the SQL:"""

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are a SQL query generator. Return ONLY valid SQL queries."},
                {"role": "user", "content": prompt}
            ],
            temperature=0,
            max_tokens=1500
        )

        sql_query = response.choices[0].message.content.strip()
        sql_query = sql_query.replace("```sql", "").replace("```", "").strip()
        logger.info("Generated SQL from model:\n%s", sql_query)

    except Exception as e:
        logger.error("OpenAI API error: %s", e)
        return None

    sql_query = _canonicalize_nba_stats_sql(sql_query)
    sql_query = _enforce_start_year_table_mapping(sql_query, user_input_param)
    sql_query = _enforce_current_regular_season_default(sql_query, user_input_param)
    sql_query = _enforce_regular_totals_table_mapping(sql_query, user_input_param)
    sql_query = _enforce_no_year_playoff_player_fallback(sql_query, user_input_param, conn)
    sql_query = _enforce_nth_season_table_mapping(sql_query, user_input_param, conn)
    sql_query = _rewrite_nth_season_comparison_sql(sql_query, user_input_param, conn)
    sql_query = _rewrite_implicit_head_to_head_to_career_sql(sql_query, user_input_param, conn)
    sql_query = _enforce_advanced_table_mapping(sql_query, user_input_param, schema_description)
    sql_query = _enforce_multi_family_stats_join(sql_query, user_input_param, schema_description)
    sql_query = _enforce_top_scorers_query(sql_query, user_input_param, schema_description)
    sql_query = _enforce_high_usg_ts_query(sql_query, user_input_param, schema_description)
    sql_query = _enforce_rebounders_query(sql_query, user_input_param, schema_description)
    sql_query = _enforce_hidden_efficiency_query(sql_query, user_input_param, schema_description)
    sql_query = _enforce_team_table_mapping(sql_query, user_input_param, schema_description)
    sql_query = _enforce_best_team_record_query(sql_query, user_input_param, schema_description)
    sql_query = _enforce_playoff_compare_template(sql_query, user_input_param, schema_description)
    sql_query = _enforce_extended_family_table_mapping(sql_query, user_input_param, schema_description)
    sql_query = _remap_known_table_families_to_existing(sql_query, user_input_param, schema_description)
    sql_query = _remap_unknown_tables_to_existing(sql_query, schema_description)
    sql_query = _enforce_team_table_columns(sql_query, user_input_param)
    sql_query = _enforce_defense_table_columns(sql_query)
    sql_query = _enforce_violations_table_columns(sql_query)
    sql_query = _enforce_game_log_column_mapping(sql_query, user_input_param)
    sql_query = _rewrite_career_aggregate_to_by_season(sql_query, user_input_param, conn)
    sql_query = _ensure_rebounding_leaderboard_columns(sql_query, user_input_param)
    sql_query = _ensure_assist_leaderboard_columns(sql_query, user_input_param)
    sql_query = _enforce_numeric_leaderboard_sort(sql_query, user_input_param)
    sql_query = _ensure_all_players_broad_columns(sql_query, user_input_param)
    sql_query = _expand_player_name_filters_for_encoding(sql_query)
    sql_query = _ensure_profile_columns_in_sql(sql_query, user_input_param)
    sql_query = _ensure_season_columns_in_sql(sql_query)
    sql_query = _enforce_regular_totals_table_mapping(sql_query, user_input_param)
    # Skip raw-data policy for advanced metrics — those tables have pre-computed stats as direct columns
    if not _is_advanced_metrics_request(user_input_param):
        try:
            sql_query = _enforce_raw_data_only_sql(sql_query)
        except ValueError as policy_error:
            logger.warning("Raw-data policy violation in generated SQL. Attempting repair: %s", policy_error)
            sql_query = repair_sql_error(
                original_sql=sql_query,
                error_message=f"Raw-data policy violation: {policy_error}",
                schema_description=schema_description,
                user_input=user_input_param
            )
            sql_query = _canonicalize_nba_stats_sql(sql_query)
            sql_query = _enforce_start_year_table_mapping(sql_query, user_input_param)
            sql_query = _enforce_current_regular_season_default(sql_query, user_input_param)
            sql_query = _enforce_regular_totals_table_mapping(sql_query, user_input_param)
            sql_query = _enforce_no_year_playoff_player_fallback(sql_query, user_input_param, conn)
            sql_query = _enforce_nth_season_table_mapping(sql_query, user_input_param, conn)
            sql_query = _rewrite_nth_season_comparison_sql(sql_query, user_input_param, conn)
            sql_query = _rewrite_implicit_head_to_head_to_career_sql(sql_query, user_input_param, conn)
            sql_query = _enforce_advanced_table_mapping(sql_query, user_input_param, schema_description)
            sql_query = _enforce_multi_family_stats_join(sql_query, user_input_param, schema_description)
            sql_query = _enforce_top_scorers_query(sql_query, user_input_param, schema_description)
            sql_query = _enforce_high_usg_ts_query(sql_query, user_input_param, schema_description)
            sql_query = _enforce_rebounders_query(sql_query, user_input_param, schema_description)
            sql_query = _enforce_hidden_efficiency_query(sql_query, user_input_param, schema_description)
            sql_query = _enforce_team_table_mapping(sql_query, user_input_param, schema_description)
            sql_query = _enforce_best_team_record_query(sql_query, user_input_param, schema_description)
            sql_query = _enforce_playoff_compare_template(sql_query, user_input_param, schema_description)
            sql_query = _enforce_extended_family_table_mapping(sql_query, user_input_param, schema_description)
            sql_query = _remap_known_table_families_to_existing(sql_query, user_input_param, schema_description)
            sql_query = _remap_unknown_tables_to_existing(sql_query, schema_description)
            sql_query = _enforce_team_table_columns(sql_query, user_input_param)
            sql_query = _enforce_defense_table_columns(sql_query)
            sql_query = _enforce_violations_table_columns(sql_query)
            sql_query = _enforce_game_log_column_mapping(sql_query, user_input_param)
            sql_query = _expand_player_name_filters_for_encoding(sql_query)
            sql_query = _ensure_profile_columns_in_sql(sql_query, user_input_param)
            sql_query = _ensure_season_columns_in_sql(sql_query)
            sql_query = _enforce_regular_totals_table_mapping(sql_query, user_input_param)
            if not _is_advanced_metrics_request(user_input_param):
                sql_query = _enforce_raw_data_only_sql(sql_query)
    sql_query = _rewrite_nth_season_comparison_sql(sql_query, user_input_param, conn)
    sql_query = _rewrite_implicit_head_to_head_to_career_sql(sql_query, user_input_param, conn)
    sql_query = _enforce_best_team_record_query(sql_query, user_input_param, schema_description)
    sql_query = _enforce_playoff_compare_template(sql_query, user_input_param, schema_description)
    sql_query = limit_rows(sql_query)

    try:
        sql_query = validate_and_normalize_sql(sql_query)
    except ValueError as e:
        logger.error("Validation error: %s", e)
        return None

    max_attempts = 3

    for attempt in range(max_attempts):
        try:
            sql_query = _canonicalize_nba_stats_sql(sql_query)
            sql_query = _enforce_team_table_mapping(sql_query, user_input_param, schema_description)
            sql_query = _enforce_multi_family_stats_join(sql_query, user_input_param, schema_description)
            sql_query = _enforce_top_scorers_query(sql_query, user_input_param, schema_description)
            sql_query = _enforce_high_usg_ts_query(sql_query, user_input_param, schema_description)
            sql_query = _enforce_rebounders_query(sql_query, user_input_param, schema_description)
            sql_query = _enforce_hidden_efficiency_query(sql_query, user_input_param, schema_description)
            sql_query = _enforce_best_team_record_query(sql_query, user_input_param, schema_description)
            sql_query = _enforce_playoff_compare_template(sql_query, user_input_param, schema_description)
            sql_query = _enforce_extended_family_table_mapping(sql_query, user_input_param, schema_description)
            sql_query = _remap_known_table_families_to_existing(sql_query, user_input_param, schema_description)
            sql_query = _enforce_team_table_columns(sql_query, user_input_param)
            sql_query = _enforce_defense_table_columns(sql_query)
            sql_query = _enforce_violations_table_columns(sql_query)
            logger.debug("Attempt %d executing query...", attempt + 1)
            logger.info("Final SQL being executed:\n%s", sql_query)
            tables_used = _extract_tables_from_sql(sql_query)
            global _LAST_TABLES_USED
            _LAST_TABLES_USED = list(tables_used)
            if tables_used:
                logger.info("TABLES USED: %s", ", ".join(tables_used))
                print(f"TABLES USED: {', '.join(tables_used)}")
            else:
                logger.info("TABLES USED: <none detected>")
                print("TABLES USED: <none detected>")
            result_df = execute_query(conn, sql_query)
            if (
                result_df is not None
                and not result_df.empty
                and _is_team_record_request(user_input_param)
                and all(
                    c in result_df.columns for c in ["GP", "W", "L"]
                )
            ):
                gp_all_null = result_df["GP"].isna().all()
                w_all_null = result_df["W"].isna().all()
                l_all_null = result_df["L"].isna().all()
                if gp_all_null and w_all_null and l_all_null:
                    team_fallback = _build_team_record_fallback_sql(user_input_param, conn)
                    if team_fallback:
                        logger.info("Team row had null GP/W/L; trying team fallback SQL:\n%s", team_fallback)
                        _LAST_TABLES_USED = _extract_tables_from_sql(team_fallback)
                        try:
                            fb_df = execute_query(conn, team_fallback)
                            if fb_df is not None and not fb_df.empty:
                                return fb_df
                        except Exception:
                            pass
            if result_df is not None and not result_df.empty:
                return result_df

            q_lower = _extract_current_question_text(user_input_param).lower()
            # Do not auto-fallback playoff queries to latest regular-season data.
            # Returning empty is safer than silently switching seasons/splits.
            if (_is_recency_request(user_input_param) or _is_team_record_request(user_input_param)):
                fallback_sql = _build_team_record_fallback_sql(user_input_param, conn) or _build_regular_summary_fallback_sql(user_input_param, conn)
                if fallback_sql:
                    logger.info("Primary query empty; trying regular-season fallback SQL:\n%s", fallback_sql)
                    fallback_tables = _extract_tables_from_sql(fallback_sql)
                    _LAST_TABLES_USED = list(fallback_tables)
                    return execute_query(conn, fallback_sql)
            return result_df

        except Exception as e:
            error_message = str(e)
            logger.error("SQL execution error: %s", error_message)
            if (
                "player_game_logs" in error_message.lower()
                and "does not exist" in error_message.lower()
                and _is_recency_request(user_input_param)
            ):
                fallback_sql = _build_regular_summary_fallback_sql(user_input_param, conn)
                if fallback_sql:
                    logger.info("player_game_logs missing; using regular-season fallback SQL:\n%s", fallback_sql)
                    _LAST_TABLES_USED = _extract_tables_from_sql(fallback_sql)
                    try:
                        return execute_query(conn, fallback_sql)
                    except Exception:
                        pass

            if any(keyword in error_message.lower()
                   for keyword in ["does not exist", "column", "relation"]):

                logger.debug("Attempting schema self-repair...")

                sql_query = repair_sql_error(
                    original_sql=sql_query,
                    error_message=error_message,
                    schema_description=schema_description,
                    user_input=user_input_param
                )
                sql_query = _canonicalize_nba_stats_sql(sql_query)

                sql_query = _enforce_start_year_table_mapping(sql_query, user_input_param)
                sql_query = _enforce_current_regular_season_default(sql_query, user_input_param)
                sql_query = _enforce_regular_totals_table_mapping(sql_query, user_input_param)
                sql_query = _enforce_no_year_playoff_player_fallback(sql_query, user_input_param, conn)
                sql_query = _enforce_nth_season_table_mapping(sql_query, user_input_param, conn)
                sql_query = _rewrite_nth_season_comparison_sql(sql_query, user_input_param, conn)
                sql_query = _rewrite_implicit_head_to_head_to_career_sql(sql_query, user_input_param, conn)
                sql_query = _enforce_advanced_table_mapping(sql_query, user_input_param, schema_description)
                sql_query = _enforce_multi_family_stats_join(sql_query, user_input_param, schema_description)
                sql_query = _enforce_top_scorers_query(sql_query, user_input_param, schema_description)
                sql_query = _enforce_high_usg_ts_query(sql_query, user_input_param, schema_description)
                sql_query = _enforce_rebounders_query(sql_query, user_input_param, schema_description)
                sql_query = _enforce_hidden_efficiency_query(sql_query, user_input_param, schema_description)
                sql_query = _enforce_team_table_mapping(sql_query, user_input_param, schema_description)
                sql_query = _enforce_best_team_record_query(sql_query, user_input_param, schema_description)
                sql_query = _enforce_playoff_compare_template(sql_query, user_input_param, schema_description)
                sql_query = _enforce_extended_family_table_mapping(sql_query, user_input_param, schema_description)
                sql_query = _remap_known_table_families_to_existing(sql_query, user_input_param, schema_description)
                sql_query = _remap_unknown_tables_to_existing(sql_query, schema_description)
                sql_query = _enforce_team_table_columns(sql_query, user_input_param)
                sql_query = _enforce_defense_table_columns(sql_query)
                sql_query = _enforce_violations_table_columns(sql_query)
                sql_query = _enforce_game_log_column_mapping(sql_query, user_input_param)
                sql_query = _rewrite_career_aggregate_to_by_season(sql_query, user_input_param, conn)
                sql_query = _ensure_rebounding_leaderboard_columns(sql_query, user_input_param)
                sql_query = _ensure_assist_leaderboard_columns(sql_query, user_input_param)
                sql_query = _enforce_numeric_leaderboard_sort(sql_query, user_input_param)
                sql_query = _ensure_all_players_broad_columns(sql_query, user_input_param)
                sql_query = _expand_player_name_filters_for_encoding(sql_query)
                sql_query = _ensure_profile_columns_in_sql(sql_query, user_input_param)
                sql_query = _ensure_season_columns_in_sql(sql_query)
                sql_query = _enforce_regular_totals_table_mapping(sql_query, user_input_param)
                if not _is_advanced_metrics_request(user_input_param):
                    sql_query = _enforce_raw_data_only_sql(sql_query)
                sql_query = _rewrite_nth_season_comparison_sql(sql_query, user_input_param, conn)
                sql_query = _rewrite_implicit_head_to_head_to_career_sql(sql_query, user_input_param, conn)
                sql_query = limit_rows(sql_query)

                try:
                    sql_query = validate_and_normalize_sql(sql_query)
                except ValueError as e:
                    logger.error("Repaired SQL is unsafe: %s", e)
                    return None

                continue
            else:
                logger.error("Non-repairable error.")
                return None

    logger.error("Max repair attempts reached.")
    return None


def run_query(question: str):
    return natural_language_to_sql(question)


def debug_query_routing(user_input: str, model_sql: str):
    """
    Debug helper to validate table routing and active DB identity without relying
    on a second OpenAI SQL generation pass.
    """
    conn = get_connection()
    cursor = conn.cursor()
    try:
        schema_description = get_db_schema(conn)
        cursor.execute(
            "SELECT current_database(), current_user, current_schema(), current_setting('search_path'), inet_server_addr(), inet_server_port();"
        )
        db_identity = cursor.fetchone()

        canon_sql = _canonicalize_nba_stats_sql(model_sql or "")
        sql_after_year = _enforce_start_year_table_mapping(canon_sql, user_input or "")
        sql_after_nth = _enforce_nth_season_table_mapping(sql_after_year, user_input or "", conn)
        sql_after_advanced = _enforce_advanced_table_mapping(
            sql_after_nth, user_input or "", schema_description
        )
        sql_after_remap = _remap_known_table_families_to_existing(sql_after_advanced, user_input or "", schema_description)
        sql_after_name = _expand_player_name_filters_for_encoding(sql_after_remap)
        sql_after_profile = _ensure_profile_columns_in_sql(sql_after_name, user_input or "")
        final_sql = _enforce_raw_data_only_sql(sql_after_profile)
        final_sql = limit_rows(final_sql)

        try:
            final_sql = validate_and_normalize_sql(final_sql)
        except Exception as e:
            final_sql = f"[INVALID SQL AFTER REWRITE] {e} | SQL: {final_sql}"

        start, end, is_playoffs = _extract_requested_season_window(user_input or "")
        expected_advanced_table = _advanced_table_name_for_window(start, end, is_playoffs)

        cursor.execute(
            """
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = %s
            );
            """,
            (expected_advanced_table,),
        )
        expected_advanced_table_exists = bool(cursor.fetchone()[0])

        cursor.execute("SELECT to_regclass(%s);", (f"public.{expected_advanced_table}",))
        regclass_public = cursor.fetchone()[0]
        cursor.execute("SELECT to_regclass(%s);", (expected_advanced_table,))
        regclass_unqualified = cursor.fetchone()[0]

        cursor.execute(
            """
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_name = %s
            ORDER BY table_schema, table_name;
            """,
            (expected_advanced_table,),
        )
        exact_table_matches = [
            {"table_schema": row[0], "table_name": row[1]} for row in cursor.fetchall()
        ]

        cursor.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND (table_name ILIKE 'advance%%totals%%' OR table_name ILIKE 'nba%%advanced%%')
            ORDER BY table_name
            LIMIT 100;
            """
        )
        advanced_table_candidates = [row[0] for row in cursor.fetchall()]

        return {
            "db_identity": {
                "database": db_identity[0],
                "user": db_identity[1],
                "schema": db_identity[2],
                "search_path": db_identity[3],
                "server_addr": str(db_identity[4]) if db_identity[4] is not None else None,
                "server_port": db_identity[5],
            },
            "routing": {
                "is_advanced_metrics_request": _is_advanced_metrics_request(user_input or ""),
                "requested_window": {
                    "start_year": start,
                    "end_year": end,
                    "is_playoffs": is_playoffs,
                },
                "expected_advanced_table": expected_advanced_table,
                "expected_advanced_table_exists": expected_advanced_table_exists,
                "regclass_public": str(regclass_public) if regclass_public is not None else None,
                "regclass_unqualified": str(regclass_unqualified) if regclass_unqualified is not None else None,
                "exact_table_matches": exact_table_matches,
            },
            "sql": {
                "input_model_sql": model_sql,
                "rewritten_sql": final_sql,
            },
            "advanced_table_candidates": advanced_table_candidates,
        }
    finally:
        cursor.close()
        conn.close()