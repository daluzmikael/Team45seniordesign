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
client = OpenAI(api_key=api_key,
                base_url="https://us.api.openai.com/v1"
                )


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
            "top ", "best ", "highest", "most ", "leading ",
            "compare", "versus", " vs ", "between",
            "leaderboard", "by season", "per season",
            "trend", "over time", "over the years", "through the years",
            "decade", "rookie year", "from ", " to ", "career",
            "better", "worse", "as well as", " or ",
            "who was", "who is", "who's", "of the two",
            "follow-up constraint",   # set by main.py when continuing a comparison
        ]
    )
    return has_profile_intent and not has_exclusions


def _extract_bare_year_request(user_input: str):
    q = _extract_current_question_text(user_input).lower()
    # If explicit season range is provided (e.g., 2024-25), do not override.
    if re.search(r"\b(19\d{2}|20\d{2})\s*[-/]\s*(\d{2}|19\d{2}|20\d{2})\b", q):
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


def _available_season_starts(conn, table_type: str):
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_name LIKE %s
            ORDER BY table_name ASC;
            """,
            (f"all_players_{table_type}_%",),
        )
        starts = []
        for (table_name,) in cursor.fetchall():
            m = re.match(rf"all_players_{table_type}_(\d{{4}})_(\d{{4}})$", table_name or "")
            if m:
                starts.append(int(m.group(1)))
        return sorted(set(starts))
    except Exception:
        return []


def _first_season_start_for_where(conn, table_type: str, where_clause: str, starts: list[int]):
    if not where_clause or not starts:
        return None
    cleaned_where = where_clause.strip().rstrip(";")
    for start in starts:
        end = start + 1
        table_name = f'all_players_{table_type}_{start}_{end}'
        sql = f'SELECT 1 FROM public."{table_name}" WHERE {cleaned_where} LIMIT 1'
        try:
            cursor = conn.cursor()
            cursor.execute(sql)
            if cursor.fetchone() is not None:
                return start
        except Exception:
            continue
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
        r"(?P<table_name>all_players_(?P<table_type>regular|playoffs)_(?P<start>\d{4})_(?P<end>\d{4}))"
        r"(?P<rest>\s+where\s+.*?)(?=(\bunion\s+all\b|$))"
    )

    def _rewrite_leg(match: re.Match):
        select_part = match.group("select_part")
        table_type = (match.group("table_type") or "").lower()
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
        target_table = f"all_players_{table_type}_{target_start}_{target_end}"
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
        where_clause = f"player_name ILIKE '%{safe_name}%'"
        first_start = _first_season_start_for_where(conn, "regular", where_clause, regular_starts)
        if first_start is None:
            continue
        target_start = first_start + (nth - 1)
        if target_start not in regular_starts:
            continue
        target_end = target_start + 1
        season_label = f"{target_start}-{str(target_end)[-2:]}"
        table_name = f'all_players_regular_{target_start}_{target_end}'
        legs.append(
            "SELECT DISTINCT "
            f"{target_start} AS season_start, '{season_label}' AS season_label, "
            "player_name, pts, reb, ast, fg_pct, fg3_pct, ft_pct, gp "
            f'FROM public."{table_name}" WHERE {where_clause}'
        )

    if len(legs) < 2:
        return sql_query
    return (
        "SELECT season_start, season_label, player_name, pts, reb, ast, fg_pct, fg3_pct, ft_pct, gp "
        f"FROM ({' UNION ALL '.join(legs)}) AS comparison LIMIT 50;"
    )


def _enforce_start_year_table_mapping(sql_query: str, user_input: str) -> str:
    if not sql_query:
        return sql_query
    if "union" in sql_query.lower():
        return sql_query

    req = _extract_bare_year_request(user_input)
    if req is None:
        return sql_query
    year, is_playoffs = req

    if is_playoffs:
        start = year - 1
        end = year
        target = f"all_players_playoffs_{start}_{end}"
        replaced = re.sub(r"(?i)all_players_(regular|playoffs)_\d{4}_\d{4}", target, sql_query)
        return replaced

    start = year
    end = year + 1
    target = f"all_players_regular_{start}_{end}"
    replaced = re.sub(r"(?i)all_players_(regular|playoffs)_\d{4}_\d{4}", target, sql_query)
    return replaced


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
    ]
    return any(term in q for term in advanced_terms)


def _extract_requested_season_window(user_input: str):
    q = _extract_current_question_text(user_input).lower()
    is_playoffs = ("playoff" in q) or ("postseason" in q)

    range_match = re.search(r"\b(19\d{2}|20\d{2})\s*[-/]\s*(\d{2}|19\d{2}|20\d{2})\b", q)
    if range_match:
        start = int(range_match.group(1))
        end_raw = range_match.group(2)
        end = int(f"{str(start)[:2]}{end_raw}") if len(end_raw) == 2 else int(end_raw)
        return start, end, is_playoffs

    bare = _extract_bare_year_request(user_input)
    if bare is not None:
        year, playoffs_flag = bare
        if playoffs_flag:
            return year - 1, year, True
        return year, year + 1, False

    if "this season" in q or "current season" in q or "last season" in q:
        return 2024, 2025, False
    if "this playoff" in q or "current playoff" in q or "last playoff" in q:
        return 2024, 2025, True

    return 2024, 2025, is_playoffs


def _all_players_full_row_columns() -> list[str]:
    return [
        "player_id", "player_name", "nickname", "team_id", "team_abbreviation", "age", "gp", "w", "l", "w_pct",
        "min", "fgm", "fga", "fg_pct", "fg3m", "fg3a", "fg3_pct", "ftm", "fta", "ft_pct",
        "oreb", "dreb", "reb", "ast", "tov", "stl", "blk", "blka", "pf", "pfd", "pts", "plus_minus",
        "gp_rank", "w_rank", "l_rank", "w_pct_rank", "min_rank", "fgm_rank", "fga_rank", "fg_pct_rank",
        "fg3m_rank", "fg3a_rank", "fg3_pct_rank", "ftm_rank", "fta_rank", "ft_pct_rank", "oreb_rank",
        "dreb_rank", "reb_rank", "ast_rank", "tov_rank", "stl_rank", "blk_rank", "blka_rank", "pf_rank",
        "pfd_rank", "pts_rank", "plus_minus_rank", "dd2", "td3", "dd2_rank", "td3_rank", "team_count",
    ]


_PLAYER_ALIAS_MAP = {
    "steph": ["Stephen Curry", "Steph Curry"],
    "steph curry": ["Stephen Curry", "Steph Curry"],
    "bron": ["LeBron James"],
    "lebron": ["LeBron James"],
    "king james": ["LeBron James"],
    "greek freak": ["Giannis Antetokounmpo", "Giannis"],
    "giannis": ["Giannis Antetokounmpo"],
    "kd": ["Kevin Durant"],
    "ad": ["Anthony Davis"],
    "kawhi": ["Kawhi Leonard"],
    "cp3": ["Chris Paul"],
    "dame": ["Damian Lillard"],
    "russ": ["Russell Westbrook"],
    "pg": ["Paul George"],
    "pg13": ["Paul George"],
    "luka": ["Luka Doncic"],
    "kobe": ["Kobe Bryant"],
    "shaq": ["Shaquille O'Neal"],
    "magic": ["Magic Johnson"],
    "jokic": ["Nikola Jokic"],
    "embiid": ["Joel Embiid"],
    "tatum": ["Jayson Tatum"],
    "sga": ["Shai Gilgeous-Alexander"],
}


# Words that are never part of a player name. Any candidate token that lowercases
# to one of these is dropped. Eliminates ghosts like "Compare LeBron James" and
# "Michael Jordan career stat" produced by the loose regex extractors.
_NAME_BLOCK_WORDS = {
    "compare", "vs", "versus", "between", "and", "than", "better", "best",
    "worse", "worst", "career", "careers", "stats", "stat", "statistic",
    "statistics", "season", "seasons", "year", "years", "playoff",
    "playoffs", "postseason", "performance", "profile", "show", "tell",
    "give", "me", "us", "the", "a", "an", "for", "of", "in", "during",
    "from", "to", "by", "per", "over", "across", "through", "thru", "all",
    "time", "rookie", "history", "historical", "alltime",
    "did", "does", "do", "was", "were", "is", "are", "what", "who", "which",
    "how", "when", "where", "why", "him", "her", "them", "his", "their",
    "this", "that", "these", "those",
}


def _normalize_player_candidate(raw: str) -> str:
    candidate = (raw or "").strip()
    if not candidate:
        return ""
    candidate = candidate.strip(" ,.;:!?'\"")
    if not candidate:
        return ""
    parts = [p for p in re.split(r"\s+", candidate) if p]

    # Strip possessive 's (straight and curly) from each token BEFORE
    # the block-word filter and validation. Bug case: "Stephen Curry's"
    # in "Compare LeBron James and Stephen Curry's career stats" was
    # surviving as a candidate, then failing DB validation silently.
    cleaned_tokens = []
    for tok in parts:
        # strip trailing 's, 's, s' (curly + straight, plus stray apostrophe)
        tok = re.sub(r"(['’]s|['’])$", "", tok)
        if tok and tok.lower() not in _NAME_BLOCK_WORDS:
            cleaned_tokens.append(tok)

    if not cleaned_tokens:
        return ""
    if len(cleaned_tokens) < 2:
        return ""
    if not all(re.match(r"^[A-Za-z][A-Za-z\.\-']*$", p) for p in cleaned_tokens):
        return ""
    if not all(p[0].isupper() for p in cleaned_tokens):
        return ""
    return " ".join(cleaned_tokens).strip()

def _career_by_season_columns() -> list[str]:
    """
    Lean column set for career / by-season / multi-season views. Drops the
    _rank columns (per-season ranks don't aggregate meaningfully) and rare
    stats. Reduces analyzer prompt size by ~75% on career compares.
    """
    return [
        "player_id", "player_name", "team_abbreviation", "age",
        "gp", "w", "l", "w_pct", "min",
        "fgm", "fga", "fg_pct", "fg3m", "fg3a", "fg3_pct",
        "ftm", "fta", "ft_pct",
        "oreb", "dreb", "reb", "ast", "tov", "stl", "blk",
        "pf", "pts", "plus_minus",
    ]


def _extract_player_names_from_question(question_text: str) -> list[str]:
    q = (question_text or "").strip()
    if not q:
        return []

    names: list[str] = []
    lower_q = q.lower()

    # Aliases first — these are unambiguous shorthands.
    for alias, expanded in _PLAYER_ALIAS_MAP.items():
        if re.search(rf"(?i)\b{re.escape(alias)}\b", q):
            names.extend(expanded)

    # Pre-strip filler tokens that pollute regex grabs. Removing these here
    # (rather than relying on per-candidate cleanup alone) lets the pair
    # patterns terminate at the right boundary.
    cleaned_q = re.sub(
        r"(?i)\b(career|careers|stats|stat|statistics?|profile|performance|"
        r"history|historical|all[- ]?time|season|seasons|year|years)\b",
        " ",
        q,
    )
    cleaned_q = re.sub(r"\s+", " ", cleaned_q).strip()

    # Pair patterns: "compare X and Y", "between X and Y", "X vs Y", "X better than Y"
    pair_patterns = [
        re.compile(r"(?i)\bcompare\s+(.+?)\s+(?:and|vs|versus)\s+(.+?)(?:\bfrom\b|\bin\b|\bduring\b|\bover\b|$)"),
        re.compile(r"(?i)\bbetween\s+(.+?)\s+and\s+(.+?)(?:\bfrom\b|\bin\b|\bduring\b|\bover\b|$)"),
        re.compile(r"(?i)\b([A-Z][\w\.\-]+(?:\s+[A-Z][\w\.\-]+){0,2})\s+(?:vs|versus)\s+([A-Z][\w\.\-]+(?:\s+[A-Z][\w\.\-]+){0,2})"),
        re.compile(r"(?i)\b([A-Z][\w\.\-]+(?:\s+[A-Z][\w\.\-]+){0,2})\s+\bthan\b\s+([A-Z][\w\.\-]+(?:\s+[A-Z][\w\.\-]+){0,2})"),
    ]
    for pattern in pair_patterns:
        match = pattern.search(cleaned_q)
        if not match:
            continue
        for idx in (1, 2):
            candidate = _normalize_player_candidate(match.group(idx))
            if candidate:
                names.append(candidate)

    # Possessives: "LeBron's", "Curry's"
    possessive_pattern = re.compile(
        r"(?i)\b([A-Z][a-zA-Z\.\-]*(?:\s+[A-Z][a-zA-Z\.\-]*){0,2})(?:['’]s)\b"
    )
    for match in possessive_pattern.finditer(cleaned_q):
        candidate = _normalize_player_candidate(match.group(1))
        if candidate:
            names.append(candidate)

    # Capitalized 2-3 token sequences — fallback when no pair pattern hit.
    capitalized_pattern = re.compile(r"\b([A-Z][a-zA-Z\.\-]+(?:\s+[A-Z][a-zA-Z\.\-]+){1,2})\b")
    for match in capitalized_pattern.finditer(cleaned_q):
        candidate = _normalize_player_candidate(match.group(1))
        if candidate:
            names.append(candidate)

    if not names and re.search(r"(?i)\b(player|players)\b", lower_q):
        return []

    # Dedupe + drop substring duplicates: if "LeBron James" and "James" both
    # show up, keep only the longer canonical form.
    by_length = sorted(set(names), key=len, reverse=True)
    kept = []
    for name in by_length:
        nlow = name.lower()
        if any(nlow in existing.lower() and nlow != existing.lower() for existing in kept):
            continue
        kept.append(name)

    # Restore original first-appearance order
    ordered: list[str] = []
    seen_lower: set[str] = set()
    for n in names:
        if n in kept and n.lower() not in seen_lower:
            ordered.append(n)
            seen_lower.add(n.lower())
    return ordered


def _filter_valid_player_names(conn, candidates: list[str]) -> list[str]:
    if not candidates or conn is None:
        return list(candidates or [])
    cursor = conn.cursor()
    valid: list[str] = []
    
    for name in candidates:
        raw_name = name.replace('%', '').replace('_', '')
        parts = [p for p in raw_name.split() if p]
        
        if len(parts) >= 2:
            first = parts[0]
            # Safely grab the first 3 letters of the last name to bypass any accents/special characters
            # (e.g., "Doncic" or "Dončić" -> "Don", "Jokic" -> "Jok")
            last_prefix = parts[-1][:3]
            
            query = """
                SELECT 1 WHERE EXISTS (
                    SELECT 1 FROM public.all_players_regular_2024_2025
                    WHERE player_name ILIKE %s OR (player_name ILIKE %s AND player_name ILIKE %s)
                    UNION ALL
                    SELECT 1 FROM public.all_players_regular_1996_1997
                    WHERE player_name ILIKE %s OR (player_name ILIKE %s AND player_name ILIKE %s)
                ) LIMIT 1;
            """
            params = (
                f"%{raw_name}%", f"%{first}%", f"%{last_prefix}%",
                f"%{raw_name}%", f"%{first}%", f"%{last_prefix}%"
            )
        else:
            like = f"%{raw_name}%"
            query = """
                SELECT 1 WHERE EXISTS (
                    SELECT 1 FROM public.all_players_regular_2024_2025
                    WHERE player_name ILIKE %s
                    UNION ALL
                    SELECT 1 FROM public.all_players_regular_1996_1997
                    WHERE player_name ILIKE %s
                ) LIMIT 1;
            """
            params = (like, like)

        try:
            cursor.execute(query, params)
            if cursor.fetchone():
                valid.append(name)
            else:
                logger.debug("Dropped unverified player candidate: %r", name)
        except Exception:
            valid.append(name) # Fallback to keeping it if query fails
            logger.exception("Error occurred while filtering player name: %r", name)
    return valid


def _extract_player_names_from_sql(sql_query: str) -> list[str]:
    q = sql_query or ""
    matches = [
        m.strip()
        for m in re.findall(r"(?i)player_name\s+ILIKE\s+'%([^%']+)%'", q)
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


def _extract_named_players(user_input: str, sql_query: str, conn=None) -> list[str]:
    question_text = _extract_current_question_text(user_input)
    names = _extract_player_names_from_question(question_text)

    # If the question alone yielded fewer than 2 names, supplement with names
    # parsed out of the model's emitted ILIKE filters. Comparison questions in
    # particular often have one name fully spelled (e.g. "LeBron James") and
    # another given as a bare surname ("Jordan") that we won't alias-map.
    sql_names = _extract_player_names_from_sql(sql_query) if sql_query else []
    if len(names) < 2 and sql_names:
        existing_lower = {n.lower() for n in names}
        for s in sql_names:
            slow = s.lower()
            if slow in existing_lower:
                continue
            # Don't pull obvious junk (single tokens with no full-name shape)
            if " " not in s.strip():
                continue
            names.append(s)
            existing_lower.add(slow)

    if conn is not None and names:
        names = _filter_valid_player_names(conn, names)
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
            subclauses.append(f"player_name ILIKE '%{safe_name}%'")
        if subclauses:
            clauses.append("(" + " OR ".join(subclauses) + ")")
    return " OR ".join(clauses)


def _extract_per_player_year_spans(user_input: str, named_players: list[str]) -> dict:
    """For comparison questions where each player has a different year attached
    ("Compare LeBron 2012 and Durant 2015"), return a dict mapping player name
    to (start_year, end_year, is_playoffs).

    Returns an empty dict when:
      - There aren't multiple named players.
      - The question uses a shared range (e.g. "from 2018 to 2024") rather than
        per-player years.
      - We can't find at least 2 distinct year mentions.

    Heuristic: walk the question, find each player-name occurrence, and pair
    it with the closest year/season-label that follows the name (within the
    same span before the next player). The 'between' or 'shared range'
    keyword guards prevent us from misclaiming "from 2018 to 2024" as a per-
    player split.
    """
    if not named_players or len(named_players) < 2:
        return {}

    q = _extract_current_question_text(user_input)
    if not q:
        return {}
    ql = q.lower()

    # Bail out for shared-range phrasings — those are NOT per-player.
    if re.search(r"\bfrom\s+(19\d{2}|20\d{2})\b.+?\bto\s+(19\d{2}|20\d{2})\b", ql):
        return {}
    if re.search(r"\b(19\d{2}|20\d{2})(?:\s+season)?\s*(?:to|through|thru|-)\s*(19\d{2}|20\d{2})\b", ql):
        return {}

    # Build a list of (position, kind, value) markers for player names + years.
    markers: list[tuple[int, str, object]] = []

    # Player name positions (case-insensitive). For each named player, find
    # the FIRST occurrence in the question. We check the full name, then
    # surname, then first name as fallbacks — users often drop one.
    for player in named_players:
        plower = player.lower()
        idx = ql.find(plower)
        if idx == -1:
            parts = plower.split()
            # Try surname first (more distinctive)
            if len(parts) >= 2:
                idx = ql.find(parts[-1])
            # Then first name
            if idx == -1 and parts:
                idx = ql.find(parts[0])
        if idx != -1:
            markers.append((idx, "player", player))

    # Year markers: 4-digit years AND season-label patterns YYYY-YY
    for m in re.finditer(r"\b(19\d{2}|20\d{2})\s*[-/]\s*(\d{2})\b", ql):
        markers.append((m.start(), "season_label", (int(m.group(1)),)))
    for m in re.finditer(r"\b(19\d{2}|20\d{2})\b", ql):
        # Skip year tokens already inside a season-label match
        already_in_label = any(
            kind == "season_label" and pos <= m.start() <= pos + 7
            for pos, kind, _ in markers
        )
        if not already_in_label:
            markers.append((m.start(), "year", int(m.group(1))))

    if not markers:
        return {}
    markers.sort(key=lambda x: x[0])

    # Walk markers — assign year to most recent player seen.
    is_playoffs = ("playoff" in ql) or ("postseason" in ql)
    spans: dict[str, tuple[int, int, bool]] = {}
    current_player: str | None = None
    for _, kind, value in markers:
        if kind == "player":
            current_player = value  # type: ignore[assignment]
        elif kind in ("year", "season_label") and current_player is not None:
            year = value if kind == "year" else value[0]  # type: ignore[index]
            # Only set if this player doesn't already have a year — first
            # year mentioned after the name wins.
            if current_player not in spans:
                spans[current_player] = (year, year, is_playoffs)

    # Require at least 2 of the named players to have distinct years.
    distinct_years = {s[0] for s in spans.values()}
    if len(spans) < 2 or len(distinct_years) < 2:
        return {}
    return spans


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

    season_label_match = re.search(r"\b(19\d{2}|20\d{2})\s*[-/]\s*(\d{2})\b", q)
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
    yy = str(end)[-2:]
    if is_playoffs:
        return f"nba_advanced_season_{start}_{yy}_season_type_playoffs_per_mode_p"
    return f"nba_advanced_season_{start}_{yy}_season_type_regular_season_per"


def _qualified_public_table_ref(table_name: str) -> str:
    # Quote the identifier and qualify schema to avoid search_path mismatches.
    safe = (table_name or "").replace('"', '""')
    return f'public."{safe}"'


def _pick_available_advanced_table(
    schema_description: str, start: int, end: int, is_playoffs: bool
) -> str:
    if not schema_description:
        return _advanced_table_name_for_window(start, end, is_playoffs)

    pattern = re.compile(
        r"nba_advanced_season_(\d{4})_(\d{2})_season_type_(regular_season|playoffs)_[a-z0-9_]+",
        re.IGNORECASE,
    )
    matches = pattern.findall(schema_description)
    if not matches:
        return _advanced_table_name_for_window(start, end, is_playoffs)

    wanted_type = "playoffs" if is_playoffs else "regular_season"
    candidates = []
    for start_s, end_yy_s, season_type in matches:
        if season_type.lower() != wanted_type:
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
    if older_or_equal:
        best = max(older_or_equal, key=lambda c: c[0])
    else:
        best = max(candidates, key=lambda c: c[0])
    return _advanced_table_name_for_window(best[0], best[1], is_playoffs)


def _enforce_advanced_table_mapping(sql_query: str, user_input: str, schema_description: str = "") -> str:
    if not sql_query or not _is_advanced_metrics_request(user_input):
        return sql_query

    # Check whether the user's CURRENT question explicitly mentions a year.
    # If it doesn't, but the model's emitted SQL already references a valid
    # nba_advanced_* table, trust the model — it most likely picked up the
    # year from prior conversation context (e.g. "Breakdown his 2018 season"
    # → "What about his net rating" should keep 2018-19, not jump to current).
    current_q = _extract_current_question_text(user_input).lower()
    has_explicit_year = bool(re.search(r"\b(19\d{2}|20\d{2})\b", current_q))
    has_explicit_season_label = bool(re.search(r"\b(19\d{2}|20\d{2})\s*[-/]\s*\d{2}\b", current_q))
    has_explicit_phrasing = any(
        k in current_q for k in (
            "this season", "current season", "last season",
            "this playoff", "current playoff", "last playoff",
            "rookie", "first season", "second season", "third season",
        )
    )
    user_specified_year = has_explicit_year or has_explicit_season_label or has_explicit_phrasing

    if not user_specified_year:
        # Check if the model's SQL already cites a real nba_advanced_* table.
        existing_advanced = re.search(
            r"(?i)nba_advanced_season_\d{4}_\d{2}_season_type_(?:regular_season|playoffs)_[a-z0-9_]+",
            sql_query,
        )
        if existing_advanced:
            # Trust the model — likely picked up year from context.
            return sql_query

    start, end, is_playoffs = _extract_requested_season_window(user_input)
    target = _pick_available_advanced_table(schema_description, start, end, is_playoffs)
    target_ref = _qualified_public_table_ref(target)
    q = sql_query

    # Route any season-summary/game-log source to advanced table family for advanced-stat intents.
    q = re.sub(r"(?i)all_players_(regular|playoffs)_\d{4}_\d{4}", target_ref, q)
    q = re.sub(r"(?i)\bplayer_game_logs\b", target_ref, q)
    q = re.sub(
        r"(?i)nba_advanced_season_\d{4}_\d{2}_season_type_(regular_season|playoffs)_[a-z0-9_]+",
        target_ref,
        q,
    )

    user_q = _extract_current_question_text(user_input).lower()
    is_true_shooting_request = any(k in user_q for k in ["true shooting", "ts%", "ts pct", "ts_pct"])
    if is_true_shooting_request:
        where_clause = None
        where_match = re.search(r"(?is)\bwhere\b(.*?)(\bgroup\s+by\b|\border\s+by\b|\blimit\b|$)", q)
        if where_match:
            maybe = where_match.group(1).strip()
            if "player_name" in maybe.lower():
                where_clause = maybe

        limit = 50
        lim_match = re.search(r"(?i)\blimit\s+(\d+)", q)
        if lim_match:
            try:
                limit = int(lim_match.group(1))
            except Exception:
                limit = 50

        parts = [
            "SELECT player_name, team_abbreviation, CAST(ts_pct AS DOUBLE PRECISION) AS true_shooting_pct",
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
        return ["fg_pct", "fg3_pct", "ft_pct", "fgm", "fga", "fg3m", "fg3a", "ftm", "fta", "gp"]
    if any(k in q for k in ["rebound", "boards", "glass"]):
        return ["reb", "oreb", "dreb", "reb_rank", "oreb_rank", "dreb_rank", "gp"]
    if any(k in q for k in ["assist", "playmaking", "passing"]):
        return ["ast", "ast_rank", "tov", "tov_rank", "gp"]
    if any(k in q for k in ["block", "rim protection"]):
        return ["blk", "blk_rank", "gp"]
    if any(k in q for k in ["steal", "defense"]):
        return ["stl", "stl_rank", "gp"]
    if any(k in q for k in ["score", "points", "scor", "offense"]):
        return ["pts", "pts_rank", "fg_pct", "fg3_pct", "ft_pct", "gp"]
    return ["pts", "reb", "ast", "fg_pct", "fg3_pct", "ft_pct", "gp"]


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
        "overall number",
        "career total",
        "how many in his career",
        "how many in her career",
        "how many in their career",
    ]
    return any(t in q for t in total_terms)


def _has_explicit_comparison_timeframe(user_input: str) -> bool:
    """Season/year/rookie/nth-season cues — absent these, head-to-head defaults to career."""
    qt = _extract_current_question_text(user_input).lower()
    if re.search(r"\b(19\d{2}|20\d{2})\s*[-/]\s*(\d{2}|19\d{2}|20\d{2})\b", qt):
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
    if conn is None or not _is_implicit_head_to_head_career_question(user_input):
        return sql_query
    raw = sql_query or ""
    q_low = raw.lower().strip()

    named = _extract_named_players(user_input, raw, conn=conn)
    if len(named) < 2:
        logger.debug("rewriter:implicit_h2h skipped (only %d named player(s))", len(named))
        return sql_query

    if "union all" in q_low and raw.lower().count("all_players_regular_") > 1:
        logger.debug("rewriter:implicit_h2h skipped (model already emitted multi-table UNION)")
        return sql_query

    plays = ("playoff" in _extract_current_question_text(user_input).lower()) or (
        "postseason" in _extract_current_question_text(user_input).lower()
    )
    table_kind = "playoffs" if plays else "regular"
    prefix = f"all_players_{table_kind}_"

    esc_p = re.escape(prefix)
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

    avail = _available_season_starts(conn, table_kind)
    if len(avail) < 2:
        return sql_query

    legs = []
    for start_year in avail:
        end_full = start_year + 1
        tnam = f"{prefix}{start_year}_{end_full}"
        ref = _qualified_public_table_ref(tnam)
        legs.append(
            f"SELECT player_name, gp, reb, pts, ast, min, fgm, fga, fg3m, fg3a, ftm, fta FROM {ref} WHERE ({where_clause})"
        )

    union_inner = " UNION ALL ".join(legs)
    return (
        "SELECT player_name, "
        "SUM(gp)::bigint AS career_gp, "
        "SUM(reb * gp)::double precision / NULLIF(SUM(gp), 0) AS reb_per_game_career, "
        "SUM(pts * gp)::double precision / NULLIF(SUM(gp), 0) AS pts_per_game_career, "
        "SUM(ast * gp)::double precision / NULLIF(SUM(gp), 0) AS ast_per_game_career, "
        "SUM(min * gp)::double precision / NULLIF(SUM(gp), 0) AS min_per_game_career, "
        "(SUM(fgm)::double precision / NULLIF(SUM(fga), 0)) AS fg_pct_career, "
        "(SUM(fg3m)::double precision / NULLIF(SUM(fg3a), 0)) AS fg3_pct_career, "
        "(SUM(ftm)::double precision / NULLIF(SUM(fta), 0)) AS ft_pct_career "
        f"FROM ({union_inner}) AS career_union "
        "GROUP BY player_name LIMIT 50"
    )


def _rewrite_career_aggregate_to_by_season(sql_query: str, user_input: str, conn=None) -> str:
    question_text = _extract_current_question_text(user_input)
    q_input = question_text.lower()
    nth_request = _extract_requested_nth_season(question_text)
    rookie_year_request = "rookie year" in q_input
    asks_over_time = _is_over_time_request(question_text)
    mentions_career = re.search(r"\bcaree+r\b", q_input) is not None
    wants_total = _is_explicit_total_request(question_text)
    q = sql_query or ""
    q_lower = q.lower()
    has_union_sum_rollup = ("union all" in q_lower) and ("sum(" in q_lower) and ("group by player_name" in q_lower)
    # Default non-total career asks to by-season rows rather than SUM rollups.
    if mentions_career and not wants_total:
        asks_over_time = True
    if has_union_sum_rollup and not wants_total:
        asks_over_time = True

    named_players = _extract_named_players(user_input, q, conn=conn)
    requested_span = _extract_requested_season_start_span(user_input)

    # Per-player year override: e.g. "Compare LeBron 2012 and Durant 2015"
    # produces {"LeBron James": (2012, 2012, False), "Kevin Durant": (2015, 2015, False)}
    # When present, each player gets their OWN year inside the loop below
    # instead of all sharing requested_span.
    per_player_spans = _extract_per_player_year_spans(user_input, named_players)
    if per_player_spans:
        logger.debug(
            "rewriter:career_aggregate using per-player year spans: %s",
            per_player_spans,
        )

    asks_full_row_player_slice = bool(named_players) and (
        requested_span is not None
        or per_player_spans
        or nth_request is not None
        or rookie_year_request
    )
    if not asks_over_time and not asks_full_row_player_slice:
        logger.debug(
            "rewriter:career_aggregate skipped (no over-time/profile signal) "
            "named_players=%s mentions_career=%s",
            named_players, mentions_career,
        )
        return sql_query

    if "all_players_regular_" not in q_lower and "all_players_playoffs_" not in q_lower:
        # The model picked a non-season table (usually player_game_logs).
        # If we have named players AND a year range or career signal, the user
        # almost certainly wants a season-by-season breakdown — not 1000+ game
        # log rows. Override the model's table choice and build the UNION ALL
        # from scratch using the season tables.
        has_season_signal = (
            requested_span is not None
            or per_player_spans
            or mentions_career
            or nth_request is not None
            or rookie_year_request
        )
        if named_players and has_season_signal:
            logger.info(
                "rewriter:career_aggregate overriding model's table choice "
                "(player_game_logs → season tables) for named_players=%s span=%s",
                named_players, requested_span,
            )
            # Fall through to the UNION ALL builder below — unique_tables will
            # be empty, and the requested_span / career branches will generate
            # the correct legs from available_starts.
        else:
            logger.debug("rewriter:career_aggregate skipped (no season-summary table in SQL)")
            return q

    logger.debug(
        "rewriter:career_aggregate applying — named_players=%s "
        "mentions_career=%s wants_total=%s nth=%s rookie=%s span=%s",
        named_players, mentions_career, wants_total, nth_request,
        rookie_year_request, requested_span,
    )

    table_type = "playoffs" if (("playoff" in q_input) or ("postseason" in q_input) or ("all_players_playoffs_" in q_lower)) else "regular"

    # Build from detected season tables directly (no SUM/COUNT) for stable season-trend output.
    table_matches = re.findall(r"(?i)\b(all_players_(?:regular|playoffs)_(\d{4})_(\d{4}))\b", q)
    if not table_matches:
        table_matches = []
    unique_tables: dict[str, tuple[int, int]] = {}
    for table_name, start_s, end_s in table_matches:
        try:
            unique_tables[table_name] = (int(start_s), int(end_s))
        except Exception:
            continue
    if named_players:
        is_comparison_or_trend = (
            len(named_players) > 1
            or mentions_career
            or asks_over_time
        )
        full_cols = (
            _career_by_season_columns()
            if is_comparison_or_trend
            else _all_players_full_row_columns()
        )
        col_sql = ", ".join(full_cols)
        available_starts = _available_season_starts(conn, table_type) if conn is not None else []
        legs = []

        for player_name in named_players:
            player_where = _player_where_clause_for_names([player_name])
            if not player_where:
                continue

            # Resolve the year-window for THIS player. Per-player spans (from
            # questions like "LeBron 2012 vs Durant 2015") win over the shared
            # requested_span. This is critical so each leg queries the right
            # season table — not collapsing both players into one year.
            effective_span = per_player_spans.get(player_name, requested_span) if per_player_spans else requested_span

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
                    if effective_span is not None:
                        _, end_year, span_playoffs = effective_span
                        table_type = "playoffs" if span_playoffs else table_type
                        season_starts = [
                            season_start
                            for season_start in range(first_start, end_year + 1)
                            if not available_starts or season_start in available_starts
                        ]
                    else:
                        season_starts = [first_start]
            elif effective_span is not None:
                start_year, end_year, span_playoffs = effective_span
                table_type = "playoffs" if span_playoffs else table_type
                season_starts = [
                    season_start
                    for season_start in range(start_year, end_year + 1)
                    if not available_starts or season_start in available_starts
                ]
            elif mentions_career and available_starts:
                # PURE CAREER REQUEST: no nth, no rookie, no explicit span.
                # Use EVERY available season for this player. The model's
                # emitted SQL is ignored here because it's commonly a copy
                # of a truncated prompt example (e.g. only 4 of 28 seasons).
                # _first_season_start_for_where finds the player's first
                # actual appearance so we don't emit dozens of empty legs
                # for years the player wasn't in the league.
                first_start = (
                    _first_season_start_for_where(
                        conn, table_type, player_where, available_starts
                    )
                    if conn is not None
                    else None
                )
                if first_start is not None:
                    season_starts = [s for s in available_starts if s >= first_start]
                    logger.debug(
                        "rewriter:career_aggregate pure-career expansion for %r — "
                        "%d seasons starting at %d",
                        player_name, len(season_starts), first_start,
                    )
                else:
                    season_starts = list(available_starts)
                    logger.debug(
                        "rewriter:career_aggregate pure-career expansion for %r — "
                        "%d seasons (no first-start probe)",
                        player_name, len(season_starts),
                    )
            elif unique_tables:
                season_starts = sorted({start for start, _ in unique_tables.values()})

            for start in season_starts:
                end = start + 1
                table_name = f"all_players_{table_type}_{start}_{end}"
                season_label = f"{start}-{str(end)[-2:]}"
                legs.append(
                    f"SELECT {start} AS season_start, '{season_label}' AS season_label, "
                    f"{col_sql} FROM {table_name} WHERE {player_where}"
                )

        if legs:
            logger.info(
                "rewriter:career_aggregate emitted %d UNION legs across %d player(s)",
                len(legs), len(named_players),
            )
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

    cols = _columns_for_by_season_question(question_text)
    col_sql = ", ".join(cols)
    sorted_tables = sorted(unique_tables.items(), key=lambda kv: kv[1][0])

    legs = []
    for table_name, (start, end) in sorted_tables:
        season_label = f"{start}-{str(end)[-2:]}"
        legs.append(
            f"SELECT {start} AS season_start, '{season_label}' AS season_label, "
            f"player_name, {col_sql} FROM {table_name} WHERE {where_clause}"
        )

    rebuilt = (
        f"SELECT DISTINCT season_start, season_label, player_name, {col_sql} "
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
    required = ["player_name", "team_abbreviation", "reb", "reb_rank", "oreb", "oreb_rank", "dreb", "dreb_rank", "gp"]
    missing = [col for col in required if not re.search(rf"(?i)\b{re.escape(col)}\b", select_part)]
    if not missing:
        return q

    injected = select_part.rstrip() + ", " + ", ".join(missing) + " "
    q = q[:start] + injected + q[end:]

    if "order by" not in q.lower():
        q = q.rstrip().rstrip(";") + " ORDER BY reb_rank ASC NULLS LAST;"
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
    required = ["player_name", "team_abbreviation", "ast", "ast_rank", "tov", "tov_rank", "gp"]
    missing = [col for col in required if not re.search(rf"(?i)\b{re.escape(col)}\b", select_part)]
    if not missing:
        return q
    injected = select_part.rstrip() + ", " + ", ".join(missing) + " "
    q = q[:start] + injected + q[end:]
    if "order by" not in q.lower():
        q = q.rstrip().rstrip(";") + " ORDER BY ast_rank ASC NULLS LAST;"
    return q


def _ensure_all_players_broad_columns(sql_query: str, user_input: str) -> str:
    q = sql_query or ""
    q_lower = q.lower()
    # Restrict to simple single-season season-summary selects.
    if "all_players_regular_" not in q_lower and "all_players_playoffs_" not in q_lower:
        return q
    
    # Bail out if this is a team-grouped query, even if it hits player tables.
    if "distinct team" in q_lower or "group by team" in q_lower or "team_name" in q_lower:
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
    required_columns = _all_players_full_row_columns()
    missing = [c for c in required_columns if not re.search(rf"(?i)\b{re.escape(c)}\b", select_part)]
    if not missing:
        return q
    injected = select_part.rstrip() + ", " + ", ".join(missing) + " "
    return q[:start] + injected + q[end:]


def _expand_player_name_filters_for_encoding(sql_query: str) -> str:
    if not sql_query:
        return sql_query

    # Capture the column name (with or without quotes) in group 1
    pattern = re.compile(r"(?i)(\"?player_name\"?)\s+ILIKE\s+'%([^%']+)%'")

    def repl(match: re.Match) -> str:
        col_name = match.group(1) # Keeps "PLAYER_NAME" or player_name intact
        raw_name = match.group(2).strip()
        parts = [p for p in raw_name.split() if p]
        if len(parts) < 2:
            return match.group(0)

        first = parts[0]
        last_prefix = parts[-1][:3] # Safe 3-letter prefix for accents (Doncic, Jokic)
        
        full_clause = f"{col_name} ILIKE '%{raw_name}%'"
        fallback_clause = (
            f"({col_name} ILIKE '%{first}%' AND {col_name} ILIKE '%{last_prefix}%')"
        )
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
    required_columns = [
        "min",
        "fgm",
        "fga",
        "fgm_rank",
        "fga_rank",
        "fg3m_rank",
        "fg3a_rank",
        "ftm_rank",
        "fta_rank",
        "min_rank",
    ]
    missing = []
    for col in required_columns:
        if not re.search(rf"(?i)\b{re.escape(col)}\b", select_part):
            missing.append(col)

    if not missing:
        return q

    injected = select_part.rstrip() + ", " + ", ".join(missing) + " "
    return q[:start] + injected + q[end:]


def _ensure_season_columns_in_sql(sql_query: str) -> str:
    """
    Ensure season context is always present for season-summary table queries so
    the analyzer can name the exact referenced season from returned rows.

    UNION-aware: when the SQL is already a multi-leg UNION ALL whose first leg
    declares a season label/start column (or whose first leg already includes a
    string-literal that is positionally the season label), we leave it alone.
    Postgres inherits column names from the first leg, so injecting differently
    into legs 2..N will create a column-count mismatch and fail.
    """
    q = sql_query or ""
    if not q:
        return q

    # Detect UNION ALL queries — if any UNION ALL is present, only inject into
    # legs that don't ALREADY have either an explicit `AS season_label` clause
    # OR a same-position string literal (which inherits the column name from
    # the first leg). In practice the simplest safe rule is: if the SQL
    # contains UNION ALL and the FIRST leg already names season_start or
    # season_label, skip injection completely — the season columns are present.
    has_union = bool(re.search(r"(?i)\bunion\s+all\b", q))
    if has_union:
        first_leg_match = re.search(
            r"(?is)\bselect\b(?P<select_part>.*?)\bfrom\b\s+all_players_(?:regular|playoffs)_\d{4}_\d{4}",
            q,
        )
        if first_leg_match:
            first_select = first_leg_match.group("select_part")
            if (re.search(r"(?i)\bseason_start\b", first_select)
                    or re.search(r"(?i)\bseason_label\b", first_select)):
                # Season columns are already declared on the first leg and
                # inherited by the rest. Do NOT inject — that would unbalance
                # the UNION column counts.
                return q

    leg_pattern = re.compile(
        r"(?is)(select\s+)(?P<select_part>.*?)(\s+from\s+)"
        r"(?P<table_name>all_players_(?P<table_type>regular|playoffs)_(?P<start>\d{4})_(?P<end>\d{4}))"
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


def _sql_touches_text_storage_family(sql_query: str) -> bool:
    """True if the SQL references any of the families where columns are stored as TEXT.

    Those tables need ORDER BY + NULLIF/::numeric casts to be functional, so the
    raw-data-only enforcer must not strip ORDER BY or block casts for them.
    """
    if not sql_query:
        return False
    q = sql_query.lower()
    return any(fam in q for fam in (
        "nba_advanced_",
        "nba_clutch_",
        "nba_hustle_",
        "nba_player_tracking_pt_",
        "nba_lineups_",
        "team_advanced_",
        "nba_standings_",
    ))


def _enforce_raw_data_only_sql(sql_query: str) -> str:
    """
    Enforce raw-data SQL:
    - no ORDER BY/GROUP BY/HAVING
    - no aggregate/math/window functions in SELECT
    """
    q = (sql_query or "").strip()
    if not q:
        return q

    # Weighted career head-to-head aggregates (see _rewrite_implicit_head_to_head_to_career_sql).
    if re.search(r"(?i)\bcareer_union\b", q):
        return q.rstrip(";") + ";"

    q = q.rstrip(";")

    # Text-storage families REQUIRE ORDER BY + casts to be functional. Skip the
    # raw-data scrubbing entirely and let the original SQL pass through.
    if _sql_touches_text_storage_family(q):
        q = re.sub(r"\s+", " ", q).strip()
        return q + ";"

    if not re.search(r"(?i)\w+_rank\b", q):
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

    select_without_strings = re.sub(r"'[^']*'", "", select_part)
    if any(op in select_without_strings for op in ["/", "*", "+", "-"]):
        raise ValueError("Generated SQL contains disallowed arithmetic in SELECT.")

    q = re.sub(r"\s+", " ", q).strip()
    return q + ";"


# 5.5 repair SQL error
def repair_sql_error(original_sql, error_message, schema_description, user_input):
    # Detect whether the failing SQL touched a TEXT-stored family. If so, the
    # repair MUST be allowed to use casts — the columns are physically text and
    # any numeric WHERE/ORDER BY needs an explicit cast.
    text_storage_families = (
        "nba_advanced_", "nba_clutch_", "nba_hustle_",
        "nba_player_tracking_pt_", "nba_lineups_",
        "team_advanced_", "nba_standings_",
    )
    touches_text_family = any(fam in (original_sql or "") for fam in text_storage_families)

    if touches_text_family:
        cast_guidance = (
            "- The query touches a TEXT-storage family (nba_advanced_*, nba_clutch_*, "
            "nba_hustle_*, nba_player_tracking_pt_*, nba_lineups_*, team_advanced_*, "
            "nba_standings_*). Every column there is physically TEXT, even numeric stats.\n"
            "- You MUST use NULLIF(col, '')::numeric for any numeric WHERE/ORDER BY on "
            "those tables. Example: ORDER BY NULLIF(\"DRIVES\", '')::numeric DESC NULLS LAST.\n"
            "- KEEP the double quotes around UPPERCASE column names. Lowercasing them "
            "makes Postgres look up a different (non-existent) identifier.\n"
            "- You MAY use CAST/NULLIF/::numeric here. They are required, not forbidden."
        )
    else:
        cast_guidance = (
            "- ONLY select direct columns from the tables\n"
            "- DO NOT use SUM, AVG, COUNT, MIN, MAX, CAST, NULLIF, COALESCE, "
            "window functions, or arithmetic expressions\n"
            "- DO NOT use GROUP BY, HAVING, or ORDER BY"
        )

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
{cast_guidance}
- Keep WHERE/JOIN only when needed to fetch correct rows and columns
Return ONLY a valid PostgreSQL SELECT query.
Do NOT include any additional text or markdown.
"""

    response = client.chat.completions.create(
        model="gpt-5.4-mini",
        messages=[
            {"role": "system", "content": "Return ONLY valid SQL."},
            {"role": "user", "content": r_prompt}
        ],
        temperature=0,
        max_completion_tokens=1500
    )

    fixed_sql = response.choices[0].message.content.strip()
    fixed_sql = fixed_sql.replace("```sql", "").replace("```", "").strip()

    # Strip leading prose/comments. The repair model occasionally returns
    # "Here is the corrected SQL:\nSELECT ..." or similar. Find the first
    # WITH/SELECT and trim everything before it. (Only WITH and SELECT are
    # allowed by the executor, and our prompt rules only ask for SELECT, but
    # we tolerate a leading WITH CTE as well so the executor's validator can
    # decide.)
    select_match = re.search(r"(?i)\b(SELECT|WITH)\b", fixed_sql)
    if select_match:
        fixed_sql = fixed_sql[select_match.start():].strip()
    # Strip a trailing semicolon if there's prose after it (rare).
    semi_match = re.search(r";", fixed_sql)
    if semi_match:
        # Keep everything up to and including the first semicolon to avoid
        # multi-statement issues.
        fixed_sql = fixed_sql[: semi_match.end()].strip()

    return fixed_sql


# 6. Convert natural language → SQL
def natural_language_to_sql(user_input_param: str):

    user_input_lower = user_input_param.strip().lower()
    if user_input_lower.startswith(('select ', 'with ', 'insert ', 'update ', 'delete ', 'create ', 'drop ', 'alter ')):
        raise ValueError("Error")

    conn = get_connection()
    schema_description = get_db_schema(conn)

    prompt = f"""
You are a senior SQL data engineer specializing in NBA statistics databases.
Your ONLY task is to convert a natural language request into a VALID PostgreSQL SELECT query.
You MUST follow every rule below without exception. There is no ambiguity — if a rule applies, follow it exactly.

GLOBAL OVERRIDE (HIGHEST PRIORITY):
- Return RAW DATA queries only.
- SELECT only direct existing columns from source table(s).
- DO NOT use SUM, AVG, COUNT, MIN, MAX, CAST, NULLIF, COALESCE, window functions, or arithmetic expressions.
- DO NOT use GROUP BY, HAVING, or ORDER BY.
- The analyzer layer handles sorting, ranking, and math after query execution.

════════════════════════════════════════════════════════════════════════
SECTION 1: THE TWO TABLE TYPES — UNDERSTAND THEM COMPLETELY
════════════════════════════════════════════════════════════════════════

TYPE A — SEASON SUMMARY TABLES:
  Naming pattern: `all_players_regular_YYYY_YYYY` and `all_players_playoffs_YYYY_YYYY`
  What they contain: One row per player per season. Pre-aggregated season stats.
  IMPORTANT SCHEMA MEANING:
    - `pts`, `reb`, `ast`, `fg_pct`, `fg3_pct`, `ft_pct` are already season-level values for that player.
    - `gp` is games played in that season.
    - `_rank` columns (like `pts_rank`) are precomputed league ranks for that season.
    - Because each player is already one row in a season table, single-season leaderboards should NOT use SUM()+GROUP BY.
  Available regular season tables (oldest to newest):
    all_players_regular_1996_1997, all_players_regular_1997_1998, all_players_regular_1998_1999,
    all_players_regular_1999_2000, all_players_regular_2000_2001, all_players_regular_2001_2002,
    all_players_regular_2002_2003, all_players_regular_2003_2004, all_players_regular_2004_2005,
    all_players_regular_2005_2006, all_players_regular_2006_2007, all_players_regular_2007_2008,
    all_players_regular_2008_2009, all_players_regular_2009_2010, all_players_regular_2010_2011,
    all_players_regular_2011_2012, all_players_regular_2012_2013, all_players_regular_2013_2014,
    all_players_regular_2014_2015, all_players_regular_2015_2016, all_players_regular_2016_2017,
    all_players_regular_2017_2018, all_players_regular_2018_2019, all_players_regular_2019_2020,
    all_players_regular_2020_2021, all_players_regular_2021_2022, all_players_regular_2022_2023,
    all_players_regular_2023_2024, all_players_regular_2024_2025
  Available playoffs tables (oldest to newest):
    all_players_playoffs_2007_2008, all_players_playoffs_2008_2009, all_players_playoffs_2009_2010,
    all_players_playoffs_2010_2011, all_players_playoffs_2011_2012, all_players_playoffs_2012_2013,
    all_players_playoffs_2013_2014, all_players_playoffs_2014_2015, all_players_playoffs_2015_2016,
    all_players_playoffs_2016_2017, all_players_playoffs_2017_2018, all_players_playoffs_2018_2019,
    all_players_playoffs_2019_2020, all_players_playoffs_2020_2021, all_players_playoffs_2021_2022,
    all_players_playoffs_2022_2023, all_players_playoffs_2023_2024, all_players_playoffs_2024_2025

  COLUMNS THAT EXIST in all_players_regular_* AND all_players_playoffs_*:
    player_id, player_name, nickname, team_id, team_abbreviation, age, gp, w, l,
    w_pct, min, fgm, fga, fg_pct, fg3m, fg3a, fg3_pct, ftm, fta, ft_pct,
    oreb, dreb, reb, ast, tov, stl, blk, blka, pf, pfd, pts, plus_minus,
    nba_fantasy_pts, dd2, td3, gp_rank, w_rank, l_rank, w_pct_rank, min_rank,
    fgm_rank, fga_rank, fg_pct_rank, fg3m_rank, fg3a_rank, fg3_pct_rank,
    ftm_rank, fta_rank, ft_pct_rank, oreb_rank, dreb_rank, reb_rank, ast_rank,
    tov_rank, stl_rank, blk_rank, blka_rank, pf_rank, pfd_rank, pts_rank,
    plus_minus_rank, nba_fantasy_pts_rank, dd2_rank, td3_rank, wnba_fantasy_pts,
    wnba_fantasy_pts_rank, team_count

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
    ❌ fg_pct          (must be calculated: CAST(SUM(fgm) AS DOUBLE PRECISION) / NULLIF(SUM(fga), 0))
    ❌ fg3_pct         (must be calculated: CAST(SUM(fg3m) AS DOUBLE PRECISION) / NULLIF(SUM(fg3a), 0))
    ❌ ft_pct          (must be calculated: CAST(SUM(ftm) AS DOUBLE PRECISION) / NULLIF(SUM(fta), 0))
    ❌ age             (not tracked per game)
    ❌ gp              (not a column — count rows instead: COUNT(*) AS games_played)
    ❌ oreb            (offensive rebounds not tracked separately)
    ❌ dreb            (defensive rebounds not tracked separately)
    ❌ nickname        (not in this table)
    ❌ plus_minus      (not in this table)
    ❌ dd2, td3        (not in this table)
    ❌ any _rank columns (none of the rank columns exist here)
    ❌ team_id         (not in this table)
    ❌ team_count      (not in this table)

TYPE C — EXTENDED NBA DATA FAMILIES (from current_working_data schema):
  These tables exist and should be used when user intent clearly matches them:
  - `nba_advanced_...`  (advanced metrics / impact context)
  - `nba_clutch_...`    (late-game / clutch situations)
  - `nba_hustle_...`    (hustle events: deflections, contested stats, etc.)
  - `nba_lineups_...`   (lineup combinations and lineup performance)
  - `nba_schedule_...`  (game schedules)
  - `nba_standings_...` (team standings / rank / records)

  IMPORTANT:
  - These tables are highly structured by name (season, season_type, per_mode, endpoint naming).
  - ALWAYS rely on DATABASE SCHEMA below to pick exact columns; never invent columns.
  - If user asks for clutch/hustle/lineup/schedule/standings, do NOT force all_players_regular_* or player_game_logs.
  - Use table-name pattern matching by intent first, then schema-confirmed columns.

════════════════════════════════════════════════════════════════════════
SECTION 2: TABLE SELECTION RULES — FOLLOW IN ORDER, FIRST MATCH WINS
════════════════════════════════════════════════════════════════════════

RULE 0 — INTENT ROUTING FOR EXTENDED TABLE FAMILIES:
  If a question clearly targets one of these domains, use that family first:
  - "clutch", "in close games", "last 5 minutes"        → `nba_clutch_...`
  - "hustle", "deflections", "box outs", "charges"      → `nba_hustle_...`
  - "tracking", "drives", "touches", "distance", "speed"→ `nba_player_tracking_...`
  - "shot chart", "shot distance", "step back", "dunks" → `court_shots`
  - "lineup", "5-man unit", "best lineup"               → `nba_lineups_...`
  - "standings", "seed", "conference rank"              → `nba_standings_...`
  - "advanced metrics", "true shooting", "net rating"   → `nba_advanced_...`
  When routing to any of the families above:
  - "this season" / no year → 2025_26 (these families are current).
  - For all_players_regular / all_players_playoffs, "this season" still maps to 2024_2025
    because those tables are not produced yet for 2025-26.

RULE 1 — MOST RECENT PLAYOFF PERFORMANCE:
  Trigger phrases: "playoff performance", "playoffs", "analyze playoffs", "postseason"
  With NO specific year mentioned:
  → ALWAYS use: `all_players_playoffs_2024_2025`
  → ALWAYS use SUM() aggregation with GROUP BY player_name

RULE 2 — MOST RECENT REGULAR SEASON PERFORMANCE:
  Trigger phrases: "season stats", "season averages", "top scorers", "leaderboards"
  With NO specific year mentioned:
  → ALWAYS use: `all_players_regular_2024_2025`
  → Use this table when rank columns (_rank) or oreb, dreb, age, gp are needed.

RULE 3 — CURRENT FORM / RECENT ACTIVITY:
  Trigger phrases: "lately", "recently", "last X games", "hot streak", "game log"
  → ALWAYS use `player_game_logs` — it has data through February 2026.
  → For 2025-26 season: WHERE season_id = '22025' AND season_type = 'Regular Season'
  → ALWAYS ORDER BY game_date DESC LIMIT X.

RULE 4 — SPECIFIC YEAR OR SEASON REQUESTED:
  START-YEAR RULE: A bare year means the season that STARTS that year.
    "2020" or "2020 season" → all_players_regular_2020_2021
  PLAYOFF EXCEPTION: A playoff year refers to playoffs at the END of that season.
    "2020 playoffs" → all_players_playoffs_2019_2020

RULE 5 — CAREER STATS & TRENDS:
  → For "career" stats: UNION ALL across ALL available yearly tables for that player, then wrap in a subquery with SUM() and GROUP BY.
  → For "by season" or "trends over time": UNION ALL across relevant season tables, but return ONE ROW PER SEASON (do NOT use SUM or GROUP BY).

RULE 6 — COMPARING PLAYERS:
  → Specific season ("this season"): Use ONE `all_players_regular` table.
  → No year context ("Who is better X or Y?"): Treat as a CAREER comparison. UNION ALL across every table and calculate career weighted averages.
  → Different eras: UNION ALL per player from their respective era tables.

RULE 7 — TOP PLAYERS / LEADERBOARD QUESTIONS:
  → Use ONE season summary table directly. Do NOT use SUM(), GROUP BY, or UNION.
  → ALWAYS ORDER BY the requested stat (e.g., ORDER BY pts_rank ASC NULLS LAST).

RULE 8 — TEAM AND FRANCHISE QUERIES:
  Trigger phrases: "top teams", "best NBA teams", "team winrate", "team standings"
  → ALWAYS use `team_advanced_season_YYYY_YY_regular_season_pergame` or `nba_standings_season_YYYY_YY_leaguestandingsv3`
  → Advanced table columns are UPPERCASE: "TEAM_ID", "TEAM_NAME", "W_PCT", "OFF_RATING", "DEF_RATING" (Wrap in double quotes).
  → Standings columns are CamelCase: "TeamName", "WINS", "LOSSES".

RULE 9 — SHOT CHARTS, DISTANCE, AND PLAY-BY-PLAY (court_shots):
  Trigger phrases: "shot chart", "where does X shoot from", "average shot distance", "dunks", "layups", "step back"
  → ALWAYS use the `court_shots` table.
  → `court_shots` is ONE massive table. Do NOT append years to the table name.
  → Filter by `game_date` or `player_name` using standard WHERE clauses.
  → Columns are lowercase: game_id, player_name, team_name, game_date, action_type, shot_type, shot_zone_basic, shot_distance, loc_x, loc_y, shot_made_flag

RULE 10 — LINEUPS AND 5-MAN UNITS (PLAYOFFS-ONLY DATA):
  Trigger phrases: "best 5-man lineup", "lineup net rating", "best lineup"
  IMPORTANT: lineup data EXISTS ONLY FOR PLAYOFFS. There are no regular-season lineup tables.
  Available years: 2007-08, 2012-13, 2021-22, 2022-23, 2023-24, 2024-25.
  → Use ONLY: nba_lineups_group_5_season_YYYY_YY_season_type_playoffs_pe (ends in _pe)
  → If the user asks for regular-season lineups, answer that the data is playoff-only
    and ask whether they want the most recent playoff lineups instead. Return no other table.

RULE 11 — SPECIALTY STATS & TRACKING (STRICT 63-CHAR TABLE NAMES):

    CURRENT SEASON NOTE: Unlike all_players_*, the advanced / clutch / hustle / tracking
    families DO have 2025-26 tables. For "this season" or "current season" advanced/specialty
    questions, use the 2025_26 suffix (e.g. nba_advanced_season_2025_26_season_type_regular_season_per).
    Use 2024_25 only when the user explicitly asks for last season.

  CRITICAL: PostgreSQL forcefully truncated these table names at exactly 63 characters. 
  DO NOT write "regular_season" or "per_mode_per" for the tracking tables. You MUST copy these exact suffix patterns character-for-character:
  
  → Hustle ("deflections", "contested shots", "charges", "screen assists"):
      Regular Season: `nba_hustle_season_YYYY_YY_season_type_regular_season_per_mo`
      Playoffs: `nba_hustle_season_YYYY_YY_season_type_playoffs_per_mode_per`
      Columns to use: "SCREEN_ASSISTS", "SCREEN_AST_PTS", "DEFLECTIONS", "CHARGES_DRAWN"
      
  → Clutch ("clutch", "last 5 minutes"):
      Regular: `nba_clutch_season_YYYY_YY_season_type_regular_season_per_mo`
      Playoffs: `nba_clutch_season_YYYY_YY_season_type_playoffs_per_mode_per`
      
  → Drives ("drives", "drives per game"):
      Regular: `nba_player_tracking_pt_drives_season_YYYY_YY_season_type_re`
      Playoffs: `nba_player_tracking_pt_drives_season_YYYY_YY_season_type_pl`
      Available columns: "DRIVES", "DRIVE_FGM", "DRIVE_FGA", "DRIVE_FG_PCT",
                         "DRIVE_PTS", "DRIVE_PASSES", "DRIVE_AST", "DRIVE_TOV", "GP".
      CRITICAL: The `_re` and `_pl` tables are stored in PER-GAME mode by default.
      "DRIVES" is ALREADY the per-game value. DO NOT divide DRIVES by GP.
      For "most drives per game" → ORDER BY NULLIF("DRIVES", '')::numeric DESC NULLS LAST.
      Apply WHERE NULLIF("GP", '')::numeric >= 20 for leaderboards to exclude small-sample outliers.
      
  → Passing ("passing", "assists created"):
      Regular: `nba_player_tracking_pt_passing_season_YYYY_YY_season_type_r`
      Playoffs: `nba_player_tracking_pt_passing_season_YYYY_YY_season_type_p`

  → Defense ("defense", "opponent points at rim", "rim protection", "give up"):
      Regular: `nba_player_tracking_pt_defense_season_YYYY_YY_season_type_r`
      Columns to use: "DEF_RIM_FGM", "DEF_RIM_FGA", "DEF_RIM_FG_PCT", "STL", "BLK" (Do not use OPP_PTS, it does not exist).
      IMPORTANT: DO NOT use the `SUM(pts)` aggregation block for defense tables. Use RAW columns.
      
  → Tracking endpoints that DO NOT split by regular/playoffs (append this EXACT suffix to the season year):
      "catch and shoot": `nba_player_tracking_pt_catchshoot_season_YYYY_YY_season_typ`
      "post-ups": `nba_player_tracking_pt_posttouch_season_YYYY_YY_season_type`
      "paint touches": `nba_player_tracking_pt_painttouch_season_YYYY_YY_season_typ`
      "pull up": `nba_player_tracking_pt_pullupshot_season_YYYY_YY_season_typ`
      "efficiency" / "drive points": `nba_player_tracking_pt_efficiency_season_YYYY_YY_season_typ`
      → Tracking endpoints that DO NOT split by regular/playoffs:
      "possessions" / "seconds per touch" / "time of possession": `nba_player_tracking_pt_possessions_season_YYYY_YY_season_ty`
        → Use column "AVG_SEC_PER_TOUCH" for seconds per touch.
      "rebounding" / "rebound chances": `nba_player_tracking_pt_rebounding_season_YYYY_YY_season_typ`
      "speed and distance" / "miles run": `nba_player_tracking_pt_speeddistance_season_YYYY_YY_season`

  UPPERCASE COLUMNS: You MUST wrap the column names in double quotes for ALL these tables. 
  ALWAYS include `"TEAM_ABBREVIATION"` and `"GP"` (Games Played) in your SELECT statement for tracking tables so the user can distinguish between regular season rows, playoff rows, and mid-season trades. Example: SELECT "PLAYER_NAME", "TEAM_ABBREVIATION", "GP", "AVG_SEC_PER_TOUCH" ...

RULE 12 — ADVANCED METRICS (STRICT 63-CHAR TABLE NAMES):
  For "PER", "Win Shares", or "impact", use `"PIE"` or `"NET_RATING"` from the advanced tables.
  CRITICAL: PostgreSQL forcefully truncated the advanced table names. You MUST use these exact suffixes:
  → Regular Season: `nba_advanced_season_YYYY_YY_season_type_regular_season_per`
  → Playoffs: `nba_advanced_season_YYYY_YY_season_type_playoffs_per_mode_p`
  Remember to wrap UPPERCASE columns in double quotes.

RULE 13 — MATCHUPS AND OPPONENTS (VS / AGAINST):
  Trigger phrases: "vs [Team]", "against the [Team]", "when playing the [Team]"
  → ALWAYS use the `player_game_logs` table.
  → Use the `matchup` column to filter for the opponent using a wildcard. 
  → The matchup format is "TEAM vs. OPP" or "TEAM @ OPP". 
  → EXAMPLE: For "against the Nuggets", use `WHERE matchup ILIKE '%DEN%'`.
  → (Note: ALWAYS convert team mascots to their 3-letter abbreviations for the ILIKE filter).

RULE 14 — FANTASY BASKETBALL:
  Trigger phrases: "fantasy points", "best fantasy player", "fantasy value"
  → Use the `all_players_regular_YYYY_YYYY` tables.
  → Select the `nba_fantasy_pts` and `nba_fantasy_pts_rank` columns. Do NOT attempt to calculate fantasy points manually.

RULE 15 — SPECIFIC ADVANCED ACRONYMS (TS%, eFG%, USG%):
  If the user asks for specific advanced shooting/usage metrics:
  → Use the `nba_advanced_season_...` tables.
  → "True Shooting" or "TS%" = `"TS_PCT"`
  → "Effective Field Goal" or "eFG%" = `"EFG_PCT"`
  → "Usage Rate" or "USG%" = `"USG_PCT"`
  → "Assist Percentage" = `"AST_PCT"`
  → "Rebound Percentage" = `"REB_PCT"`
  → "PER" (Player Efficiency Rating, John Hollinger's metric):
       This database does NOT store classic PER. The closest equivalent is "PIE" in nba_advanced_*.
       Numbers in nba_advanced_* are TEXT-stored, so cast with NULLIF(col, '')::numeric.
       Map "PER" → SELECT "PLAYER_NAME", "TEAM_ABBREVIATION", "GP", "PIE"
                   FROM nba_advanced_season_YYYY_YY_season_type_regular_season_per
                   WHERE NULLIF("GP", '')::numeric >= 20
                   ORDER BY NULLIF("PIE", '')::numeric DESC NULLS LAST LIMIT 10;
       The analyzer will rename PIE to "PIE (PER equivalent)" in the rendered output —
       you do NOT need to alias it in the SQL.
  CRITICAL: You MUST wrap these uppercase column names in double quotes.

RULE 16 — TABLES YOU MUST NOT USE:
  These tables appear in the schema but are NOT to be queried by this system:
  - `nba_regular_season_totals_*`  (legacy import, columns differ from all_players_regular_*)
  - `nba_shot_locations_*`         (column names are unnamed/duplicated, unusable)
  - `team_advanced_staging`        (staging copy, not for production)
  ALWAYS prefer the canonical equivalent:
  - season totals → `all_players_regular_YYYY_YYYY`
  - shot location → `court_shots`
  - team advanced → `team_advanced_season_YYYY_YY_regular_season_pergame`

════════════════════════════════════════════════════════════════════════
SECTION 3: MANDATORY QUERY CONSTRUCTION RULES
════════════════════════════════════════════════════════════════════════

PLAYER NAME MATCHING:
  - ALWAYS use ILIKE with wildcards on BOTH sides: player_name ILIKE '%Giannis%'
  - For full names use: player_name ILIKE '%LeBron James%'
  - NEVER use exact match (=) for player names
  - NEVER use ILIKE 'Jordan%' — this matches Jordan Poole, DeAndre Jordan, etc.
  - For last-name-only queries use a leading space: player_name ILIKE '% Harris%'
    to reduce false matches like "Gary Harris" when searching just "Harris"
  - Expand ALL nicknames to full names before searching:
      "Steph" or "Steph Curry"   → player_name ILIKE '%Stephen Curry%' OR player_name ILIKE '%Steph Curry%'
      "Bron" or "King James"     → player_name ILIKE '%LeBron James%'
      "Greek Freak"              → player_name ILIKE '%Giannis%'
      "KD"                       → player_name ILIKE '%Kevin Durant%'
      "AD"                       → player_name ILIKE '%Anthony Davis%'
      "Kawhi"                    → player_name ILIKE '%Kawhi Leonard%'
      "CP3"                      → player_name ILIKE '%Chris Paul%'
      "Dame"                     → player_name ILIKE '%Damian Lillard%'
      "Russ"                     → player_name ILIKE '%Russell Westbrook%'
      "PG" or "PG13"             → player_name ILIKE '%Paul George%'

TRACKING TABLE PER-MODE RULE:
  - All `nba_player_tracking_pt_*` tables suffixed `_re` or `_pl` are in PER-GAME mode.
  - The numeric stat columns (DRIVES, DRIVE_PTS, POST_TOUCHES, AVG_SPEED, etc.) are
    already per-game values. NEVER divide them by GP.
  - The same applies to nba_hustle_*, nba_clutch_*, nba_advanced_*, nba_lineups_*,
    team_advanced_*, and nba_standings_* tables.
  - For "most X per game" questions on these tables, just ORDER BY the column directly
    (with the numeric cast described next).

TEXT-STORED NUMERIC COLUMNS (CRITICAL):
  Every column in nba_advanced_*, nba_clutch_*, nba_hustle_*, nba_player_tracking_pt_*,
  nba_lineups_*, team_advanced_*, and nba_standings_* tables is stored as TEXT in
  PostgreSQL — even numeric stats like GP, DRIVES, PIE, TS_PCT, etc.
  This means:
  - Numeric comparisons ALWAYS need an explicit cast:
      WHERE NULLIF("GP", '')::numeric >= 20
  - Numeric ORDER BY ALWAYS needs an explicit cast:
      ORDER BY NULLIF("DRIVES", '')::numeric DESC NULLS LAST
  - WITHOUT the cast, "9.5" sorts ABOVE "19.2" (lexicographic) and any `>= 20`
    comparison fails with `operator does not exist: text >= integer`.
  - Use NULLIF(col, '') so empty strings cast cleanly to NULL instead of erroring.
  - The all_players_regular_*, all_players_playoffs_*, player_game_logs, and
    court_shots tables DO have proper numeric types — no casts needed there.

AGGREGATION RULES for season summary tables:
  - ONLY use SUM()+GROUP BY when combining MULTIPLE rows per player
    (examples: UNION ALL across many seasons for career stats, or other explicit multi-season rollups).
  - For SINGLE-SEASON season-summary queries (one all_players_regular_YYYY_YYYY table),
    SELECT columns directly and DO NOT use SUM() or GROUP BY.
  - If percentages already exist in season summary tables (`fg_pct`, `fg3_pct`, `ft_pct`), select them directly.
  - Only calculate percentages via SUM() when aggregating multiple rows per player:
      (CAST(SUM(fgm) AS DOUBLE PRECISION) / NULLIF(SUM(fga), 0)) AS fg_pct
      (CAST(SUM(fg3m) AS DOUBLE PRECISION) / NULLIF(SUM(fg3a), 0)) AS fg3_pct
      (CAST(SUM(ftm) AS DOUBLE PRECISION) / NULLIF(SUM(fta), 0)) AS ft_pct

STANDARD PLAYER PERFORMANCE SELECT BLOCK:
  Use this exact block ONLY for career/all-time or explicit multi-season aggregation:
    player_name,
    SUM(pts)  AS total_pts,
    SUM(reb)  AS total_reb,
    SUM(ast)  AS total_ast,
    SUM(stl)  AS total_stl,
    SUM(blk)  AS total_blk,
    SUM(tov)  AS total_tov,
    SUM(fgm)  AS total_fgm,
    SUM(fga)  AS total_fga,
    SUM(fg3m) AS total_fg3m,
    SUM(fg3a) AS total_fg3a,
    SUM(ftm)  AS total_ftm,
    SUM(fta)  AS total_fta,
    (CAST(SUM(fgm)  AS DOUBLE PRECISION) / NULLIF(SUM(fga),  0)) AS fg_pct,
    (CAST(SUM(fg3m) AS DOUBLE PRECISION) / NULLIF(SUM(fg3a), 0)) AS fg3_pct,
    (CAST(SUM(ftm)  AS DOUBLE PRECISION) / NULLIF(SUM(fta),  0)) AS ft_pct

STANDARD SINGLE-SEASON LEADERBOARD BLOCK (NO AGGREGATION):
  Use this for questions like "top scorers in 2001-02", "best scorers this season", "league leaders in points":
    player_name,
    team_abbreviation,
    pts,
    pts_rank,
    gp,
    fg_pct,
    fg3_pct,
    fg3m,
    fg3a,
    ftm,
    fta,
    ft_pct
  FROM one season table only
  ORDER BY pts_rank ASC NULLS LAST (or pts DESC when rank unavailable)

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
  DO NOT INCLUDE MATH OR PERCENTAGE CALCULATIONS.

DIVISION SAFETY:
  - ALWAYS wrap division denominators with NULLIF(..., 0)
  - ALWAYS cast numerator to DOUBLE PRECISION before dividing
  - NEVER do raw division like fgm / fga — always use the safe pattern above

DATE FILTERING:
  - NEVER use EXTRACT() or DATE_TRUNC().
  - If a user asks for a specific month (e.g., "March 2024"), filter dates using standard string comparisons: WHERE game_date >= '2024-03-01' AND game_date <= '2024-03-31'

GENERAL:
  - Do NOT add a generic LIMIT (e.g. LIMIT 50) unless the user asks for top-N, last-X games, or a bounded sample.
  - For shot charts, game logs, or "all shots / full picture" asks, omit LIMIT so results are not arbitrarily truncated (heavy queries are still bounded by server cost/timeout).
  - NEVER use SELECT * — always name columns explicitly
  - NEVER invent column names that are not listed in this prompt
  - NEVER add ORDER BY game_date to season summary tables (game_date does not exist there)
  - NEVER add WHERE season_id = ... to season summary tables (season_id does not exist there)
  - NEVER add WHERE season_type = ... to season summary tables (season_type does not exist there)
  - If a question is ambiguous between recency and season summary, default to player_game_logs
    with season_id = '22025' since it is the most current data available
  - For "last N games" / "recent games" / "last X games" prompts, omit the
    season_id filter so cross-season tails work. Just ORDER BY game_date DESC LIMIT N.

════════════════════════════════════════════════════════════════════════
SECTION 4: SEASON AND YEAR REFERENCE MAP
════════════════════════════════════════════════════════════════════════

  "current season" or no year specified (regular)  → all_players_regular_2024_2025
    **Exception:** Head-to-head "who is better / between X and Y" with NO season/year wording → RULE 6B career UNION ALL (not only latest season).
  "current playoffs" or no year specified (playoff) → all_players_playoffs_2024_2025
  Bare year uses START-YEAR mapping for regular season:
  "2020"                                            → all_players_regular_2020_2021
  "2018"                                            → all_players_regular_2018_2019
  "2016"                                            → all_players_regular_2016_2017

  Playoff year uses END-YEAR mapping:
  "2020 playoffs"                                   → all_players_playoffs_2019_2020
  "2016 playoffs"                                   → all_players_playoffs_2015_2016

  "last season" / "2024-25"                         → all_players_regular_2024_2025
  "2023-24" / "last year"                           → all_players_regular_2023_2024
  "2022-23"                                         → all_players_regular_2022_2023
  "2021-22"                                         → all_players_regular_2021_2022
  "2020-21"                                         → all_players_regular_2020_2021
  "bubble" / "2019-20"                              → all_players_regular_2019_2020
  "2018-19"                                         → all_players_regular_2018_2019
  "2017-18"                                         → all_players_regular_2017_2018
  "2016-17"                                         → all_players_regular_2016_2017
  "2015-16"                                         → all_players_regular_2015_2016

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
→ RULE 1. No year. Use all_players_playoffs_2024_2025.
SELECT player_name,
  SUM(pts) AS total_pts, SUM(reb) AS total_reb, SUM(ast) AS total_ast,
  SUM(stl) AS total_stl, SUM(blk) AS total_blk, SUM(tov) AS total_tov,
  SUM(fgm) AS total_fgm, SUM(fga) AS total_fga,
  SUM(fg3m) AS total_fg3m, SUM(fg3a) AS total_fg3a,
  SUM(ftm) AS total_ftm, SUM(fta) AS total_fta,
  (CAST(SUM(fgm)  AS DOUBLE PRECISION) / NULLIF(SUM(fga),  0)) AS fg_pct,
  (CAST(SUM(fg3m) AS DOUBLE PRECISION) / NULLIF(SUM(fg3a), 0)) AS fg3_pct,
  (CAST(SUM(ftm)  AS DOUBLE PRECISION) / NULLIF(SUM(fta),  0)) AS ft_pct
FROM all_players_playoffs_2024_2025
WHERE player_name ILIKE '%Giannis%'
GROUP BY player_name LIMIT 50;

Q: "Analyze Garry Harris playoff performance"
→ RULE 1. No year. Use all_players_playoffs_2024_2025.
SELECT player_name,
  SUM(pts) AS total_pts, SUM(reb) AS total_reb, SUM(ast) AS total_ast,
  SUM(stl) AS total_stl, SUM(blk) AS total_blk, SUM(tov) AS total_tov,
  SUM(fgm) AS total_fgm, SUM(fga) AS total_fga,
  SUM(fg3m) AS total_fg3m, SUM(fg3a) AS total_fg3a,
  SUM(ftm) AS total_ftm, SUM(fta) AS total_fta,
  (CAST(SUM(fgm)  AS DOUBLE PRECISION) / NULLIF(SUM(fga),  0)) AS fg_pct,
  (CAST(SUM(fg3m) AS DOUBLE PRECISION) / NULLIF(SUM(fg3a), 0)) AS fg3_pct,
  (CAST(SUM(ftm)  AS DOUBLE PRECISION) / NULLIF(SUM(fta),  0)) AS ft_pct
FROM all_players_playoffs_2024_2025
WHERE player_name ILIKE '%Garry Harris%'
GROUP BY player_name LIMIT 50;

Q: "How did Giannis do in the 2019 playoffs"
→ RULE 4. Year specified: 2019 → all_players_playoffs_2018_2019.
SELECT player_name,
  SUM(pts) AS total_pts, SUM(reb) AS total_reb, SUM(ast) AS total_ast,
  (CAST(SUM(fgm)  AS DOUBLE PRECISION) / NULLIF(SUM(fga),  0)) AS fg_pct,
  (CAST(SUM(fg3m) AS DOUBLE PRECISION) / NULLIF(SUM(fg3a), 0)) AS fg3_pct,
  (CAST(SUM(ftm)  AS DOUBLE PRECISION) / NULLIF(SUM(fta),  0)) AS ft_pct
FROM all_players_playoffs_2018_2019
WHERE player_name ILIKE '%Giannis%'
GROUP BY player_name LIMIT 50;

Q: "Show me LeBron's last 10 games"
→ RULE 3. Recency. Use player_game_logs, season_id = '22025', ORDER BY game_date DESC.
SELECT player_name, game_date, matchup, wl, pts, reb, ast, stl, blk, tov,
  fgm, fga, fg3m, fg3a, ftm, fta, min,
  (CAST(fgm  AS DOUBLE PRECISION) / NULLIF(fga,  0)) AS fg_pct,
  (CAST(fg3m AS DOUBLE PRECISION) / NULLIF(fg3a, 0)) AS fg3_pct,
  (CAST(ftm  AS DOUBLE PRECISION) / NULLIF(fta,  0)) AS ft_pct
FROM player_game_logs
WHERE player_name ILIKE '%LeBron James%'
  AND season_id = '22025'
ORDER BY game_date DESC LIMIT 10;

Q: "How has Steph been playing lately"
→ RULE 3 / RULE 10. Recency keyword. Use player_game_logs, season_id = '22025'.
SELECT player_name, game_date, matchup, wl, pts, reb, ast,
  fgm, fga, fg3m, fg3a,
  (CAST(fgm  AS DOUBLE PRECISION) / NULLIF(fga,  0)) AS fg_pct,
  (CAST(fg3m AS DOUBLE PRECISION) / NULLIF(fg3a, 0)) AS fg3_pct
FROM player_game_logs
WHERE (player_name ILIKE '%Stephen Curry%' OR player_name ILIKE '%Steph Curry%')
  AND season_id = '22025'
  AND season_type = 'Regular Season'
ORDER BY game_date DESC LIMIT 15;

Q: "Compare LeBron and Curry this season"
→ RULE 6. Two players, same era, season summary. Use all_players_regular_2024_2025.
SELECT player_name,
  SUM(pts) AS total_pts, SUM(reb) AS total_reb, SUM(ast) AS total_ast,
  (CAST(SUM(fgm)  AS DOUBLE PRECISION) / NULLIF(SUM(fga),  0)) AS fg_pct,
  (CAST(SUM(fg3m) AS DOUBLE PRECISION) / NULLIF(SUM(fg3a), 0)) AS fg3_pct,
  (CAST(SUM(ftm)  AS DOUBLE PRECISION) / NULLIF(SUM(fta),  0)) AS ft_pct
FROM all_players_regular_2024_2025
WHERE player_name ILIKE '%LeBron James%' OR player_name ILIKE '%Stephen Curry%'
GROUP BY player_name LIMIT 50;

Q: "What were Kevin Durant's stats 2015"
→ RULE 4 + RULE 11. Single-player season profile, no aggregation.
SELECT DISTINCT player_name, team_abbreviation, age, gp, min, w_pct,
  pts, reb, ast, tov, stl, blk, pf, plus_minus, fgm, fga,
  fg_pct, fg3_pct, ft_pct, fg3m, fg3a, ftm, fta,
  pts_rank, fg_pct_rank, fg3_pct_rank, ft_pct_rank, fgm_rank, fga_rank, fg3m_rank, fg3a_rank, ftm_rank, fta_rank, min_rank,
  reb_rank, dreb_rank, oreb_rank, ast_rank, tov_rank, stl_rank, blk_rank, pf_rank,
  dreb, oreb, dd2, td3, dd2_rank, td3_rank
FROM all_players_regular_2015_2016
WHERE player_name ILIKE '%Kevin Durant%'
LIMIT 50;

Q: "Who are the top 10 scorers this season"
→ RULE 8. Leaderboard. Use all_players_regular_2024_2025.
SELECT player_name, team_abbreviation, pts, pts_rank, gp, fg_pct, fg3_pct, fg3m, fg3a, ftm, fta, ft_pct
FROM all_players_regular_2024_2025
ORDER BY pts_rank ASC NULLS LAST LIMIT 10;

Q: "Who are the best scorers from 2000-2001"
→ RULE 4 + RULE 8. Specific season leaderboard from one table, no aggregation.
SELECT DISTINCT player_name, team_abbreviation, pts, pts_rank, gp, fg_pct, fg3_pct, fg3m, fg3a, ftm, fta, ft_pct
FROM all_players_regular_2000_2001
ORDER BY pts_rank ASC NULLS LAST LIMIT 5;

Q: "Show me Steph Curry's career stats"
→ RULE 5. Career = UNION ALL across all yearly tables.
SELECT player_name,
  SUM(pts) AS total_pts, SUM(reb) AS total_reb, SUM(ast) AS total_ast,
  (CAST(SUM(fgm)  AS DOUBLE PRECISION) / NULLIF(SUM(fga),  0)) AS fg_pct,
  (CAST(SUM(fg3m) AS DOUBLE PRECISION) / NULLIF(SUM(fg3a), 0)) AS fg3_pct
FROM (
  SELECT player_name, pts, reb, ast, fgm, fga, fg3m, fg3a FROM all_players_regular_2012_2013 WHERE player_name ILIKE '%Stephen Curry%'
  UNION ALL
  SELECT player_name, pts, reb, ast, fgm, fga, fg3m, fg3a FROM all_players_regular_2013_2014 WHERE player_name ILIKE '%Stephen Curry%'
  UNION ALL
  SELECT player_name, pts, reb, ast, fgm, fga, fg3m, fg3a FROM all_players_regular_2014_2015 WHERE player_name ILIKE '%Stephen Curry%'
  UNION ALL
  SELECT player_name, pts, reb, ast, fgm, fga, fg3m, fg3a FROM all_players_regular_2024_2025 WHERE player_name ILIKE '%Stephen Curry%'
) AS career GROUP BY player_name LIMIT 50;

Q: "Is Giannis on a hot streak"
→ RULE 3. Streak = game log recency. Use player_game_logs.
SELECT player_name, game_date, matchup, wl, pts, reb, ast,
  (CAST(fgm  AS DOUBLE PRECISION) / NULLIF(fga,  0)) AS fg_pct
FROM player_game_logs
WHERE player_name ILIKE '%Giannis%'
  AND season_id = '22025'
  AND season_type = 'Regular Season'
ORDER BY game_date DESC LIMIT 10;

Q: "Compare LeBron and Jordan career stats"
→ RULE 7. Different eras. UNION ALL per player. CRITICAL: The shape below is
   shown ABBREVIATED for readability. You MUST emit ONE leg PER AVAILABLE
   SEASON listed in DATABASE SCHEMA above for each player's actual era.
   Do NOT copy these legs verbatim — the post-processor cannot add legs you
   did not emit. For Jordan, include every available all_players_regular_*
   table from 1996_1997 through 2002_2003. For LeBron, include every
   available all_players_regular_* table from 2003_2004 through 2024_2025
   (every season in between, no gaps).
SELECT player_name,
  SUM(pts) AS total_pts, SUM(reb) AS total_reb, SUM(ast) AS total_ast,
  (CAST(SUM(fgm)  AS DOUBLE PRECISION) / NULLIF(SUM(fga),  0)) AS fg_pct
FROM (
  -- Michael Jordan: ONE leg per available season in his era (1996-97 through 2002-03)
  SELECT player_name, pts, reb, ast, fgm, fga FROM all_players_regular_1996_1997 WHERE player_name ILIKE '%Michael Jordan%'
  UNION ALL
  SELECT player_name, pts, reb, ast, fgm, fga FROM all_players_regular_1997_1998 WHERE player_name ILIKE '%Michael Jordan%'
  UNION ALL
  -- ... include 2001_2002 and 2002_2003 as well ...
  UNION ALL
  -- LeBron James: ONE leg per available season (2003-04 through 2024-25, every season)
  SELECT player_name, pts, reb, ast, fgm, fga FROM all_players_regular_2003_2004 WHERE player_name ILIKE '%LeBron James%'
  UNION ALL
  SELECT player_name, pts, reb, ast, fgm, fga FROM all_players_regular_2004_2005 WHERE player_name ILIKE '%LeBron James%'
  UNION ALL
  -- ... continue for 2005_2006, 2006_2007, ..., 2023_2024 ...
  UNION ALL
  SELECT player_name, pts, reb, ast, fgm, fga FROM all_players_regular_2024_2025 WHERE player_name ILIKE '%LeBron James%'
) AS combined GROUP BY player_name LIMIT 50;

Q: "Show me Kevin Durant's points trend from 2015 to 2023"
→ RULE 5. By-season trend. UNION ALL across EVERY season in the inclusive range.
   ONE ROW PER SEASON. No SUM. No GROUP BY across seasons.
SELECT player_name, season_label, pts, gp, fg_pct, fg3_pct, ft_pct
FROM (
  SELECT player_name, '2015-16' AS season_label, pts, gp, fg_pct, fg3_pct, ft_pct
    FROM all_players_regular_2015_2016 WHERE player_name ILIKE '%Kevin Durant%'
  UNION ALL
  SELECT player_name, '2016-17', pts, gp, fg_pct, fg3_pct, ft_pct
    FROM all_players_regular_2016_2017 WHERE player_name ILIKE '%Kevin Durant%'
  UNION ALL
  SELECT player_name, '2017-18', pts, gp, fg_pct, fg3_pct, ft_pct
    FROM all_players_regular_2017_2018 WHERE player_name ILIKE '%Kevin Durant%'
  UNION ALL
  SELECT player_name, '2018-19', pts, gp, fg_pct, fg3_pct, ft_pct
    FROM all_players_regular_2018_2019 WHERE player_name ILIKE '%Kevin Durant%'
  UNION ALL
  SELECT player_name, '2019-20', pts, gp, fg_pct, fg3_pct, ft_pct
    FROM all_players_regular_2019_2020 WHERE player_name ILIKE '%Kevin Durant%'
  UNION ALL
  SELECT player_name, '2020-21', pts, gp, fg_pct, fg3_pct, ft_pct
    FROM all_players_regular_2020_2021 WHERE player_name ILIKE '%Kevin Durant%'
  UNION ALL
  SELECT player_name, '2021-22', pts, gp, fg_pct, fg3_pct, ft_pct
    FROM all_players_regular_2021_2022 WHERE player_name ILIKE '%Kevin Durant%'
  UNION ALL
  SELECT player_name, '2022-23', pts, gp, fg_pct, fg3_pct, ft_pct
    FROM all_players_regular_2022_2023 WHERE player_name ILIKE '%Kevin Durant%'
  UNION ALL
  SELECT player_name, '2023-24', pts, gp, fg_pct, fg3_pct, ft_pct
    FROM all_players_regular_2023_2024 WHERE player_name ILIKE '%Kevin Durant%'
) AS trend
ORDER BY season_label;

Q: "Compare Luka Doncic and Trae Young assists from 2020 to 2024"
→ RULE 6 + RULE 5. Two players, multi-season range. UNION ALL each (player, season).
   ONE ROW PER (player, season). No cross-season aggregation.
SELECT player_name, season_label, ast, gp
FROM (
  SELECT player_name, '2020-21' AS season_label, ast, gp FROM all_players_regular_2020_2021
    WHERE player_name ILIKE '%Luka Doncic%' OR player_name ILIKE '%Trae Young%'
  UNION ALL
  SELECT player_name, '2021-22', ast, gp FROM all_players_regular_2021_2022
    WHERE player_name ILIKE '%Luka Doncic%' OR player_name ILIKE '%Trae Young%'
  UNION ALL
  SELECT player_name, '2022-23', ast, gp FROM all_players_regular_2022_2023
    WHERE player_name ILIKE '%Luka Doncic%' OR player_name ILIKE '%Trae Young%'
  UNION ALL
  SELECT player_name, '2023-24', ast, gp FROM all_players_regular_2023_2024
    WHERE player_name ILIKE '%Luka Doncic%' OR player_name ILIKE '%Trae Young%'
  UNION ALL
  SELECT player_name, '2024-25', ast, gp FROM all_players_regular_2024_2025
    WHERE player_name ILIKE '%Luka Doncic%' OR player_name ILIKE '%Trae Young%'
) AS combined
ORDER BY season_label, player_name;

Q: "Who had the most drives per game in 2023?"
→ RULE 0 + RULE 11. "drives" → tracking_pt_drives. "in 2023" → 2023_24 season.
   "DRIVES" is already per-game in this table — DO NOT divide by GP.
   These tables store numbers as TEXT, so cast with NULLIF(col, '')::numeric.
SELECT "PLAYER_NAME", "TEAM_ABBREVIATION", "GP", "DRIVES",
       "DRIVE_PTS", "DRIVE_FG_PCT", "DRIVE_AST"
FROM nba_player_tracking_pt_drives_season_2023_24_season_type_re
WHERE NULLIF("GP", '')::numeric >= 20
ORDER BY NULLIF("DRIVES", '')::numeric DESC NULLS LAST
LIMIT 10;

Q: "Who leads in deflections this season?"
→ RULE 0 + RULE 11. "deflections" → hustle. "this season" → 2025_26.
   These tables store numbers as TEXT, so cast with NULLIF(col, '')::numeric.
SELECT "PLAYER_NAME", "TEAM_ABBREVIATION", "G", "DEFLECTIONS", "CONTESTED_SHOTS", "CHARGES_DRAWN"
FROM nba_hustle_season_2025_26_season_type_regular_season_per_mo
WHERE NULLIF("G", '')::numeric >= 20
ORDER BY NULLIF("DEFLECTIONS", '')::numeric DESC NULLS LAST
LIMIT 10;

Q: "Who had the highest PER in 2023?"
→ RULE 15. PER is not stored — closest equivalent is "PIE" in nba_advanced_*.
   Numbers in advanced tables are TEXT, so cast with NULLIF(col, '')::numeric.
SELECT "PLAYER_NAME", "TEAM_ABBREVIATION", "GP", "PIE"
FROM nba_advanced_season_2023_24_season_type_regular_season_per
WHERE NULLIF("GP", '')::numeric >= 20
ORDER BY NULLIF("PIE", '')::numeric DESC NULLS LAST
LIMIT 10;

Q: "How does Giannis perform on post-ups?"
→ RULE 0 + RULE 11. "post-ups" → tracking_pt_posttouch. No year ⇒ current ⇒ 2025_26.
SELECT "PLAYER_NAME", "TEAM_ABBREVIATION", "GP", "POST_TOUCHES",
       "POST_TOUCH_FG_PCT", "POINTS", "PTS_PER_TOUCH"
FROM nba_player_tracking_pt_posttouch_season_2025_26_season_type
WHERE "PLAYER_NAME" ILIKE '%Giannis%';

Q: "What were LeBron's stats vs the Celtics in 2024?"
→ RULE 13. Use player_game_logs. Calendar year 2024 ⇒ filter by game_date range.
SELECT player_name, game_date, matchup, wl, pts, reb, ast, stl, blk, tov,
  fgm, fga, fg3m, fg3a, ftm, fta, min,
  (CAST(fgm AS DOUBLE PRECISION) / NULLIF(fga, 0)) AS fg_pct
FROM player_game_logs
WHERE player_name ILIKE '%LeBron James%'
  AND matchup ILIKE '%BOS%'
  AND game_date >= '2024-01-01' AND game_date <= '2024-12-31'
ORDER BY game_date DESC;

Q: "How many points did Wembanyama score in his last 5 games?"
→ RULE 3. "last N games" — do NOT pin to a specific season; let game_date order across season boundary.
SELECT player_name, game_date, matchup, wl, pts, reb, ast, fgm, fga, fg3m, fg3a, ftm, fta, min
FROM player_game_logs
WHERE player_name ILIKE '%Victor Wembanyama%'
  OR player_name ILIKE '%Wembanyama%'
ORDER BY game_date DESC
LIMIT 5;

════════════════════════════════════════════════════════════════════════
SECTION 6: COMMON MISTAKES — NEVER DO THESE
════════════════════════════════════════════════════════════════════════

❌ SELECT season_id FROM all_players_playoffs_2024_2025    -- does not exist in summary tables
❌ SELECT game_date FROM all_players_regular_2024_2025     -- does not exist in summary tables
❌ SELECT fg_pct FROM player_game_logs                     -- does not exist, must calculate
❌ SELECT oreb FROM player_game_logs                       -- does not exist in game logs
❌ WHERE season_type = 'Playoffs' on a summary table       -- column does not exist there
❌ WHERE season_id = '22025' on a summary table            -- column does not exist there
❌ player_name ILIKE 'Jordan%'                             -- matches wrong players
❌ player_name ILIKE 'Harris'                              -- missing wildcards
❌ fgm / fga                                               -- unsafe, use NULLIF
❌ SELECT * FROM any table                                  -- always name columns explicitly
❌ Using all_players_playoffs_2018_2019 when no year given  -- always default to 2024_2025
❌ Using player_game_logs for "analyze playoff performance" -- use season summary tables
❌ AVG(fg_pct) from season summary tables                  -- use SUM(fgm)/SUM(fga) instead
❌ ORDER BY game_date on a season summary table            -- game_date does not exist there
❌ WHERE season_id = '22024' for current 2025-26 games     -- current season is '22025'
❌ Using all_players_regular_2024_2025 for "last X games"  -- no game_date column there
❌ Assuming player_game_logs is outdated                   -- it has data through Feb 2026
❌ SELECT gp FROM player_game_logs                         -- use COUNT(*) AS games_played

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
            model="gpt-5.4-mini",
            messages=[
                {"role": "system", "content": "You are a SQL query generator. Return ONLY valid SQL queries."},
                {"role": "user", "content": prompt}
            ],
            temperature=0,
            max_completion_tokens=1500
        )

        sql_query = response.choices[0].message.content.strip()
        sql_query = sql_query.replace("```sql", "").replace("```", "").strip()
        logger.info("Generated SQL from model:\n%s", sql_query)

    except Exception as e:
        logger.error("OpenAI API error: %s", e)
        return None

    sql_query = _enforce_start_year_table_mapping(sql_query, user_input_param)
    sql_query = _enforce_nth_season_table_mapping(sql_query, user_input_param, conn)
    sql_query = _rewrite_nth_season_comparison_sql(sql_query, user_input_param, conn)
    sql_query = _rewrite_implicit_head_to_head_to_career_sql(sql_query, user_input_param, conn)
    sql_query = _enforce_advanced_table_mapping(sql_query, user_input_param, schema_description)
    sql_query = _rewrite_career_aggregate_to_by_season(sql_query, user_input_param, conn)
    sql_query = _ensure_rebounding_leaderboard_columns(sql_query, user_input_param)
    sql_query = _ensure_assist_leaderboard_columns(sql_query, user_input_param)
    sql_query = _ensure_all_players_broad_columns(sql_query, user_input_param)
    sql_query = _expand_player_name_filters_for_encoding(sql_query)
    sql_query = _ensure_profile_columns_in_sql(sql_query, user_input_param)
    sql_query = _ensure_season_columns_in_sql(sql_query)
    # Skip raw-data policy for advanced metrics by user-intent keywords. The
    # enforcer itself ALSO bypasses scrubbing when the SQL touches a text-storage
    # family (nba_advanced_*, nba_clutch_*, nba_hustle_*, nba_player_tracking_pt_*,
    # nba_lineups_*, team_advanced_*, nba_standings_*) — those queries need ORDER BY
    # and NULLIF/::numeric casts to function. So this gate is just for keyword-based
    # routing; the function is the safety net for everything else.
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
            sql_query = _enforce_start_year_table_mapping(sql_query, user_input_param)
            sql_query = _enforce_nth_season_table_mapping(sql_query, user_input_param, conn)
            sql_query = _rewrite_nth_season_comparison_sql(sql_query, user_input_param, conn)
            sql_query = _rewrite_implicit_head_to_head_to_career_sql(sql_query, user_input_param, conn)
            sql_query = _enforce_advanced_table_mapping(sql_query, user_input_param, schema_description)
            sql_query = _expand_player_name_filters_for_encoding(sql_query)
            sql_query = _ensure_profile_columns_in_sql(sql_query, user_input_param)
            sql_query = _ensure_season_columns_in_sql(sql_query)
            sql_query = _enforce_raw_data_only_sql(sql_query)
    sql_query = _rewrite_nth_season_comparison_sql(sql_query, user_input_param, conn)
    sql_query = _rewrite_implicit_head_to_head_to_career_sql(sql_query, user_input_param, conn)
    sql_query = limit_rows(sql_query)

    try:
        sql_query = validate_and_normalize_sql(sql_query)
    except ValueError as e:
        logger.error("Validation error: %s", e)
        return None

    max_attempts = 3

    for attempt in range(max_attempts):
        try:
            logger.debug("Attempt %d executing query...", attempt + 1)
            logger.info("Final SQL being executed:\n%s", sql_query)
            return execute_query(conn, sql_query)

        except Exception as e:
            error_message = str(e)
            logger.error("SQL execution error: %s", error_message)

            if any(keyword in error_message.lower()
                   for keyword in ["does not exist", "column", "relation"]):

                logger.debug("Attempting schema self-repair...")

                sql_query = repair_sql_error(
                    original_sql=sql_query,
                    error_message=error_message,
                    schema_description=schema_description,
                    user_input=user_input_param
                )

                sql_query = _enforce_start_year_table_mapping(sql_query, user_input_param)
                sql_query = _enforce_nth_season_table_mapping(sql_query, user_input_param, conn)
                sql_query = _rewrite_nth_season_comparison_sql(sql_query, user_input_param, conn)
                sql_query = _rewrite_implicit_head_to_head_to_career_sql(sql_query, user_input_param, conn)
                sql_query = _enforce_advanced_table_mapping(sql_query, user_input_param, schema_description)
                sql_query = _rewrite_career_aggregate_to_by_season(sql_query, user_input_param, conn)
                sql_query = _ensure_rebounding_leaderboard_columns(sql_query, user_input_param)
                sql_query = _ensure_assist_leaderboard_columns(sql_query, user_input_param)
                sql_query = _ensure_all_players_broad_columns(sql_query, user_input_param)
                sql_query = _expand_player_name_filters_for_encoding(sql_query)
                sql_query = _ensure_profile_columns_in_sql(sql_query, user_input_param)
                sql_query = _ensure_season_columns_in_sql(sql_query)
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
        cursor.execute(
            "SELECT current_database(), current_user, current_schema(), current_setting('search_path'), inet_server_addr(), inet_server_port();"
        )
        db_identity = cursor.fetchone()

        sql_after_year = _enforce_start_year_table_mapping(model_sql or "", user_input or "")
        sql_after_nth = _enforce_nth_season_table_mapping(sql_after_year, user_input or "", conn)
        sql_after_advanced = _enforce_advanced_table_mapping(sql_after_nth, user_input or "")
        sql_after_name = _expand_player_name_filters_for_encoding(sql_after_advanced)
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
              AND table_name ILIKE 'nba%%advanced%%'
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