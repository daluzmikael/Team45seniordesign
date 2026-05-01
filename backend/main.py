import logging
import os
import json

# Align root log level with env before importing Interpreter/Executor (uvicorn may configure logging first).
logging.getLogger().setLevel(
    getattr(logging, (os.getenv("LOG_LEVEL") or "INFO").upper(), logging.INFO)
)

from fastapi import FastAPI, HTTPException, Header
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, List, Dict, Any, Set
from DashboardBackend.dashboardInterpreter import interpret_question
from Analyzer.query_analyzer import analyze_question_with_data
from auth import (
    sign_up,
    log_in,
    verify_token,
    save_history_message,
    get_conversation_messages,
    list_conversations,
)
from Interpreter.interpreter import run_query, debug_query_routing
from openai import OpenAI
import numpy as np
import pandas as pd
import re

app = FastAPI()

context_client = (
    OpenAI(api_key=os.getenv("OPENAI_API_KEY"), base_url="https://us.api.openai.com/v1")
    if os.getenv("OPENAI_API_KEY")
    else None
)

@app.get("/")
async def root():
    return {"status": "ok", "message": "API is running"}

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

class QueryRequest(BaseModel):
    question: str
    conversationId: Optional[str] = None
    history: Optional[List[Dict[str, Any]]] = None

class AuthRequest(BaseModel):
    email: str
    password: str

class DebugRoutingRequest(BaseModel):
    question: str
    model_sql: Optional[str] = None


class HistoryMessageRequest(BaseModel):
    conversationId: str
    role: str
    content: str


def get_uid_from_authorization(authorization: Optional[str]) -> str:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing or invalid Authorization header")

    id_token = authorization.replace("Bearer ", "", 1).strip()
    if not id_token:
        raise HTTPException(status_code=401, detail="Missing token")

    verified = verify_token(id_token)
    if not verified.get("success"):
        raise HTTPException(status_code=401, detail="Invalid token")

    uid = verified.get("uid")
    if not uid:
        raise HTTPException(status_code=401, detail="Invalid token payload")
    return uid


def _build_contextual_question(
    current_question: str, history_messages: List[Dict[str, Any]], max_messages: int = 8
) -> str:
    """
    Build a compact context wrapper so follow-up questions can resolve references
    (e.g., "him", "that season", "those two players") using recent chat turns.
    """
    if not history_messages:
        return current_question

    recent = history_messages[-max_messages:]
    context_lines: List[str] = []
    for msg in recent:
        role = str(msg.get("role", "")).strip().lower()
        if role not in {"user", "assistant"}:
            continue
        content = str(msg.get("content", "")).strip()
        if not content:
            continue
        content = content.replace("\n", " ").strip()
        if len(content) > 300:
            content = content[:300] + "..."
        speaker = "User" if role == "user" else "Assistant"
        context_lines.append(f"{speaker}: {content}")

    if not context_lines:
        return current_question

    context_block = "\n".join(context_lines)
    return (
        "Use the recent conversation context below only to resolve references in the current question.\n"
        "Do not answer from chat text; still query the database as needed.\n\n"
        f"Conversation context:\n{context_block}\n\n"
        f"Current question: {current_question}"
    )


def _compact_history_for_context_ai(
    history_messages: List[Dict[str, Any]], max_messages: int = 8, max_chars: int = 450
) -> str:
    recent = history_messages[-max_messages:]
    context_lines: List[str] = []
    for msg in recent:
        role = str(msg.get("role", "")).strip().lower()
        if role not in {"user", "assistant"}:
            continue
        content = str(msg.get("content", "")).strip()
        if not content:
            continue
        content = re.sub(r"\s+", " ", content)
        if len(content) > max_chars:
            content = content[:max_chars] + "..."
        speaker = "User" if role == "user" else "Assistant"
        context_lines.append(f"{speaker}: {content}")
    return "\n".join(context_lines)


def _json_object_from_text(raw: str) -> Optional[Dict[str, Any]]:
    text = (raw or "").strip()
    if not text:
        return None
    try:
        parsed = json.loads(text)
        return parsed if isinstance(parsed, dict) else None
    except Exception:
        pass

    match = re.search(r"\{.*\}", text, flags=re.DOTALL)
    if not match:
        return None
    try:
        parsed = json.loads(match.group(0))
        return parsed if isinstance(parsed, dict) else None
    except Exception:
        return None


def _resolve_followup_with_ai(
    current_question: str, history_messages: List[Dict[str, Any]]
) -> Optional[Dict[str, str]]:
    """
    Ask a small model to convert a sequential chat turn into a standalone NBA
    analytics question before SQL generation. This keeps pronoun/entity/time
    resolution flexible without putting raw assistant prose into the SQL prompt.
    """
    if context_client is None or not history_messages:
        return None

    history_block = _compact_history_for_context_ai(history_messages)
    if not history_block:
        return None

    system_prompt = (
        "You rewrite NBA analytics chat follow-ups into standalone database questions.\n"
        "Return ONLY a JSON object with these string/boolean fields:\n"
        "standalone_question, analysis_question, needs_history, reason.\n\n"
        "Rules:\n"
        "- Use history only to resolve references like he, him, they, them, that season, those teams, or same stat.\n"
        "- Do not answer the question and do not write SQL.\n"
        "- Preserve the user's latest requested metric, season, season type, opponent, and entity type.\n"
        "- If the latest user asks 'what about X' or 'how about X', carry the relevant prior metric/timeframe and replace the subject with X.\n"
        "- If pronouns refer to teams/franchises, write team names as teams, not players.\n"
        "- If pronouns refer to players, write player names as players.\n"
        "- If the user says yes/sure/ok/do it, infer the specific follow-up from the last assistant offer and prior user question.\n"
        "- If the current question is already standalone, set needs_history to false and repeat it exactly.\n"
        "- analysis_question should be the same as standalone_question unless a shorter natural wording is clearer for the final explanation.\n"
        "- Never invent a player, team, or season that is not present in the current question or recent history.\n"
    )
    user_prompt = (
        f"Recent conversation:\n{history_block}\n\n"
        f"Current user question:\n{current_question}\n\n"
        "JSON only:"
    )

    try:
        response = context_client.chat.completions.create(
            model=os.getenv("CONTEXT_RESOLVER_MODEL", "gpt-5.4-mini"),
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0,
            max_completion_tokens=500,
        )
        parsed = _json_object_from_text(response.choices[0].message.content or "")
        if not parsed:
            return None

        standalone = str(parsed.get("standalone_question", "")).strip()
        analysis_question = str(parsed.get("analysis_question", "")).strip() or standalone
        if not standalone:
            return None

        needs_raw = parsed.get("needs_history", True)
        if isinstance(needs_raw, bool):
            needs_history = needs_raw
        else:
            needs_history = str(needs_raw).strip().lower() not in {"false", "0", "no"}
        if not needs_history and standalone.lower() == (current_question or "").strip().lower():
            return {
                "effective_question": current_question,
                "analysis_question": current_question,
                "reason": "already_standalone",
            }

        return {
            "effective_question": standalone,
            "analysis_question": analysis_question,
            "reason": str(parsed.get("reason", "ai_context_rewrite")).strip() or "ai_context_rewrite",
        }
    except Exception as context_error:
        print(f"AI context rewrite skipped: {context_error}")
        return None


def _is_affirmative_followup(question: str) -> bool:
    q = re.sub(r"[^\w\s]", "", (question or "").strip().lower())
    if not q:
        return False
    tokens = q.split()
    if not tokens:
        return False
    if tokens[0] in {"yes", "yeah", "yep", "yup", "yah", "ya", "sure", "ok", "okay", "k", "kk"}:
        return True
    return q in {"do it", "go ahead", "go for it", "lets go", "lets do it", "please do"}


_COMPARISON_FOLLOWUP_MARKERS = (
    "better", "worse", "compare", "vs ", " vs.",
    "between them", "of the two", "of those two",
    "who was better", "who is better", "who's better",
    " or ", "as well as",
    # Comparative phrasing requires "than" — bare "more" / "less" matches
    # too aggressively (e.g. "tell me more about X" is a drill-down, not
    # a comparison).
    "more than", "less than", "greater than",
)

def _looks_like_comparison_followup(question: str) -> bool:
    raw = (question or "").strip().lower()
    # Strip a leading discourse "or " — when "Or what about KD?" comes after a
    # prior turn, that's a drill-down on KD, not a comparison.
    if raw.startswith("or "):
        raw = raw[3:].strip()
    q = " " + raw + " "
    return any(m in q for m in _COMPARISON_FOLLOWUP_MARKERS)


# Phrases that signal the user wants a deeper look at a SPECIFIC entity from
# the prior turn. When combined with a named player in the question, this lets
# us scope the response to that one player.
_DRILLDOWN_MARKERS = (
    "more in-depth", "more in depth", "more detail", "more details",
    "deeper", "deep dive", "deep-dive", "drill down", "drill-down",
    "tell me more", "tell me about",
    "breakdown of", "break down",
    "more on", "more about", "expand on", "elaborate on",
    "what about", "how about",
    "focus on", "just ",
)

def _looks_like_single_player_drilldown(question: str) -> bool:
    """True when the question has a 'tell me more / drill down' shape.
    Used to scope a follow-up to one named player (not a comparison)."""
    raw = (question or "").strip().lower()
    if raw.startswith("or "):
        raw = raw[3:].strip()
    q = " " + raw + " "
    if any(m in q for m in _COMPARISON_FOLLOWUP_MARKERS):
        return False
    return any(m in q for m in _DRILLDOWN_MARKERS)


# Match 1-3 word capitalized names. Allows internal uppercase (LeBron, McGee).
# Trailing 's (possessive) is stripped before matching. Sentence-start filter
# keeps common question words from leaking through.
_QUESTION_NAME_RE = re.compile(r"\b([A-Z][a-zA-Z]{2,}(?:\s+[A-Z][a-zA-Z\.\-']*){0,2})\b")

def _extract_named_players_from_question(question: str) -> List[str]:
    """Pull capitalized 1-3 word names from the user's question.
    Conservative — drops obvious section-header / stat-phrase / common-word
    false positives via _NAME_STOPWORDS and a sentence-start filter."""
    if not question:
        return []
    found: List[str] = []
    seen: Set[str] = set()
    # Strip possessive 's so "Curry's" → "Curry"
    cleaned = re.sub(r"['\u2019]s\b", "", question)
    # Skip common sentence-start capitalized words that look like names
    sentence_start_blocklist = {
        "what", "who", "where", "when", "why", "how", "show", "give",
        "tell", "compare", "find", "list", "rank", "between", "statistically",
        "analyze", "break", "more", "yes", "yeah", "ok", "sure",
        "deeper", "deep", "expand", "elaborate", "focus", "just",
        "describe", "explain", "summarize", "look", "looking",
    }
    for cand in _QUESTION_NAME_RE.findall(cleaned):
        key = cand.lower()
        first_word = key.split()[0] if key else ""
        if first_word in sentence_start_blocklist:
            continue
        if any(stop in key for stop in _NAME_STOPWORDS):
            continue
        if key in seen:
            continue
        seen.add(key)
        found.append(cand)
    return found


# Stop words to filter out of name candidates pulled from prior assistant text.
_NAME_STOPWORDS = {
    "regular season", "per game", "playoffs", "playoff", "field goal",
    "free throw", "double double", "triple double", "double-double",
    "triple-double", "plus minus", "plus-minus", "western conference",
    "eastern conference", "all star", "all-star",
}

_NAME_CANDIDATE_RE = re.compile(r"\b([A-Z][a-z]+(?:\s+[A-Z][a-z\.\-']+){1,2})\b")

def _extract_player_names_from_history(
    history_messages: List[Dict[str, Any]], limit: int = 4
) -> List[str]:
    names: List[str] = []
    seen = set()
    for msg in reversed(history_messages or []):
        content = str(msg.get("content", "")).strip()
        if not content:
            continue
        for cand in _NAME_CANDIDATE_RE.findall(content):
            key = cand.lower()
            if any(stop in key for stop in _NAME_STOPWORDS):
                continue
            if key in seen:
                continue
            seen.add(key)
            names.append(cand)
            if len(names) >= limit:
                return names
    return names


def _extract_latest_player_and_season_from_history(
    history_messages: List[Dict[str, Any]]
) -> tuple[Optional[str], Optional[str]]:
    player: Optional[str] = None
    season: Optional[str] = None
    season_pattern = re.compile(r"\b((?:19|20)\d{2}-\d{2})\s+season\b", re.IGNORECASE)
    requested_pattern = re.compile(r"([A-Za-z][A-Za-z\.\- ]+?)'s requested season was", re.IGNORECASE)
    heading_pattern = re.compile(r"\*\*([A-Za-z][A-Za-z\.\- ]+)\*\*")

    for msg in reversed(history_messages or []):
        content = str(msg.get("content", "")).strip()
        if not content:
            continue

        if season is None:
            season_match = season_pattern.search(content)
            if season_match:
                season = season_match.group(1)

        if player is None:
            requested_match = requested_pattern.search(content)
            if requested_match:
                player = requested_match.group(1).strip()
            else:
                heading_match = heading_pattern.search(content)
                if heading_match:
                    player = heading_match.group(1).strip()

        if player and season:
            break

    return player, season


def _should_apply_history_context(question: str) -> bool:
    q = (question or "").strip().lower()
    if not q:
        return False

    # Only inject history for likely follow-up/ellipsis prompts.
    followup_markers = [
        "what about",
        "how about",
        "and what",
        "and how",
        "and him",
        "and her",
        "and them",
        "and those",
        "and that",
        "and defensively",
        "and offensively",
        "also",
        "same ",
        "that ",
        "those ",
        "them",
        "they",
        "him",
        "his ",
        "her ",
        "their ",
        " it ",
        "its ",
        "that team",
        "those teams",
        "same team",
        "same stat",
        "same question",
        "offensively",
        "defensively",
        "break that down",
        "which one",
        "who is better",
        "who's better",
        "who was better",
        "yes",
        "yeah",
        "yep",
        "yup",
        "ok",
        "okay",
        "sure",
        # Drill-down phrasings — user is asking for more detail on something
        # established earlier in the conversation.
        "more in-depth",
        "more in depth",
        "more detail",
        "tell me more",
        "tell me about",
        "breakdown of",
        "break down",
        "more on",
        "more about",
        "expand on",
        "elaborate",
        "deeper",
        "deep dive",
        "drill down",
        "focus on",
        # Single-pronoun follow-ups — implicitly reference the prior subject.
        "how was he",
        "how is he",
        "how was she",
        "how is she",
        "was he",
        "was she",
        "is he",
        "is she",
        "did he",
        "did she",
        "does he",
        "does she",
        " he ",
        " she ",
    ]
    if any(marker in q for marker in followup_markers):
        return True

    # Very short prompts are often dependent on prior context.
    token_count = len(re.findall(r"\w+", q))
    return token_count <= 4


def _analysis_debug_enabled() -> bool:
    # Dev-focused: enabled by default; disable by setting ANALYSIS_DEBUG=0
    return os.getenv("ANALYSIS_DEBUG", "1").strip() not in {"0", "false", "False"}


def _sanitize_history_messages(history: Optional[List[Dict[str, Any]]]) -> List[Dict[str, str]]:
    if not isinstance(history, list):
        return []

    sanitized: List[Dict[str, str]] = []
    for msg in history:
        if not isinstance(msg, dict):
            continue
        role = str(msg.get("role", "")).strip().lower()
        content = str(msg.get("content", "")).strip()
        if role not in {"user", "assistant"} or not content:
            continue
        sanitized.append({"role": role, "content": content})
    return sanitized


def _extract_explicit_season_start(question: str) -> tuple[Optional[int], bool]:
    q = (question or "").lower()
    is_playoffs = "playoff" in q or "postseason" in q

    season_match = re.search(r"\b(19\d{2}|20\d{2})\s*[-/]\s*(\d{2}|19\d{2}|20\d{2})\b", q)
    if season_match:
        return int(season_match.group(1)), is_playoffs

    year_match = re.search(r"\b(19\d{2}|20\d{2})\b", q)
    if not year_match:
        return None, is_playoffs

    year = int(year_match.group(1))
    if is_playoffs:
        return year - 1, True
    return year, False


def _unsupported_specialty_message(question: str) -> Optional[str]:
    q = (question or "").lower()
    hustle_terms = [
        "hustle",
        "deflection",
        "deflections",
        "contested shot",
        "contested shots",
        "charge",
        "charges",
        "screen assist",
        "screen assists",
        "box out",
        "box outs",
        "loose ball",
        "loose balls",
    ]
    if not any(term in q for term in hustle_terms):
        return None

    season_start, is_playoffs = _extract_explicit_season_start(question)
    if season_start is None:
        return None

    if is_playoffs:
        available_playoff_starts = {1998, 2004, *range(2015, 2025)}
        if season_start not in available_playoff_starts:
            season_label = f"{season_start}-{str(season_start + 1)[-2:]}"
            return (
                f"Hustle/deflections playoff data is not available for {season_label} in this database. "
                "Available playoff hustle seasons are 1998-99, 2004-05, and 2015-16 through 2024-25."
            )
        return None

    if season_start < 2015:
        season_label = f"{season_start}-{str(season_start + 1)[-2:]}"
        return (
            f"Hustle/deflections regular-season data is not available for {season_label} in this database. "
            "Regular-season hustle data starts at 2015-16 and runs through 2025-26."
        )
    return None


def _build_effective_question_from_history(
    current_question: str, history_messages: List[Dict[str, Any]]
) -> tuple[str, str, str]:
    """
    Return (sql_question, analyzer_question, strategy). Prefer an AI rewrite
    into a standalone question; fall back to the old context wrapper plus
    targeted deterministic constraints when the rewrite is unavailable.
    """
    ai_resolution = _resolve_followup_with_ai(current_question, history_messages)
    if ai_resolution:
        return (
            ai_resolution["effective_question"],
            ai_resolution["analysis_question"],
            f"ai_standalone_rewrite: {ai_resolution['reason']}",
        )

    effective_question = _build_contextual_question(current_question, history_messages)
    analysis_question = current_question

    if _is_affirmative_followup(current_question):
        player_name, season_label = _extract_latest_player_and_season_from_history(history_messages)
        if player_name and season_label:
            effective_question += (
                "\n\nFollow-up constraint: keep the SAME player and SAME season as the last answer. "
                f"Use player_name ILIKE '%{player_name}%' and season_label '{season_label}' context "
                "(do not advance to a different season)."
            )

    # Comparison continuity. If the follow-up is comparison-shaped but the user
    # dropped one or both names, pull the missing entities from history.
    if _looks_like_comparison_followup(current_question):
        prior_names = _extract_player_names_from_history(history_messages)
        current_lower = current_question.lower()
        missing = [n for n in prior_names if n.lower() not in current_lower]
        if missing:
            names_clause = " and ".join(missing[:2])
            effective_question += (
                f"\n\nFollow-up constraint: this is a CONTINUATION of an earlier comparison. "
                f"Include {names_clause} alongside any players named in the current question. "
                f"Treat as a multi-player comparison and return one row per player."
            )
            analysis_question = (
                f"{analysis_question} (continuing comparison with {names_clause})"
            )

    # Single-player drill-down. If the follow-up explicitly names one player and
    # is not comparison-shaped, scope the SQL and narrative to that one player.
    elif _looks_like_single_player_drilldown(current_question):
        named = _extract_named_players_from_question(current_question)
        if len(named) == 1:
            only_player = named[0]
            effective_question += (
                f"\n\nFollow-up constraint: this drill-down is about {only_player} ONLY. "
                f"Filter SQL to player_name ILIKE '%{only_player}%' and do NOT include "
                f"any other players from the prior conversation. The narrative must focus "
                f"exclusively on {only_player}."
            )
            analysis_question = (
                f"{analysis_question} (single-player drill-down: {only_player})"
            )

    return effective_question, analysis_question, "deterministic_context_wrapper"

@app.post("/api/dashboards")
async def dashboard_endpoint(request: QueryRequest):
    result = interpret_question(request.question)
    if result.get("success"):
        return result
    else:
        print("Error details:", result.get("error"), result.get("details"))
        raise HTTPException(status_code=400, detail=result.get("error", "Unknown error"))

@app.post("/api/analysis")
async def analysis_endpoint(
    request: QueryRequest,
    authorization: Optional[str] = Header(default=None),
):
    try:
        print("----HIT----- /api/analysis")
        print(f"Question: {request.question}")

        effective_question = request.question
        analysis_question = request.question
        history_context_applied = False
        history_context_reason = "no_history_available"
        history_messages: List[Dict[str, Any]] = _sanitize_history_messages(request.history)

        # Prefer history passed by the frontend (works for both guest and auth chats).
        if history_messages and _should_apply_history_context(request.question):
            effective_question, analysis_question, context_strategy = _build_effective_question_from_history(
                request.question, history_messages
            )
            history_context_applied = True
            history_context_reason = f"request_history_used/{context_strategy}"
        # Fallback for older clients: load persisted history for authenticated users.
        elif request.conversationId and authorization and _should_apply_history_context(request.question):
            try:
                uid = get_uid_from_authorization(authorization)
                history_result = get_conversation_messages(uid, request.conversationId.strip())
                if history_result.get("success"):
                    fetched_history = history_result.get("messages", [])
                    if isinstance(fetched_history, list) and fetched_history:
                        effective_question, analysis_question, context_strategy = _build_effective_question_from_history(
                            request.question, _sanitize_history_messages(fetched_history)
                        )
                        history_context_applied = True
                        history_context_reason = f"stored_history_used/{context_strategy}"
                    else:
                        history_context_reason = "stored_history_empty"
                else:
                    history_context_reason = "history_lookup_failed"
            except Exception as history_error:
                # Keep analysis available even if history lookup fails.
                print(f"History context skipped: {history_error}")
                history_context_reason = "history_lookup_exception"
        elif history_messages:
            history_context_reason = "history_not_needed_for_standalone_question"
        elif request.conversationId and authorization:
            history_context_reason = "stored_history_not_needed_for_standalone_question"
        elif request.conversationId and not authorization:
            history_context_reason = "guest_without_request_history"

        unsupported_message = _unsupported_specialty_message(effective_question)
        if unsupported_message:
            payload = {
                "success": True,
                "analysis": unsupported_message,
                "data": [],
                "question": analysis_question,
            }
            if _analysis_debug_enabled():
                payload["debug"] = {
                    "historyContextApplied": history_context_applied,
                    "historyContextReason": history_context_reason,
                    "conversationId": request.conversationId,
                    "originalQuestion": request.question,
                    "effectiveQuestion": effective_question,
                    "analysisQuestion": analysis_question,
                    "unsupportedReason": "specialty_table_unavailable",
                }
            return payload

        # Run the query once here; query_analyzer should only interpret the returned dataframe.
        query_result = run_query(effective_question)

        # Handle empty or failed queries with a helpful message instead of crashing
        if query_result is None or query_result.empty:
            payload = {
                "success": True,
                "analysis": (
                    "No data was found for this query. This could mean:\n"
                    "- The player or team did not appear in the requested season/playoffs.\n"
                    "- The player or team name may be misspelled or not recognized.\n"
                    "- Try specifying a season year, e.g. 'Giannis 2023 playoff performance'."
                ),
                "data": [],
                "question": analysis_question
            }
            if _analysis_debug_enabled():
                payload["debug"] = {
                    "historyContextApplied": history_context_applied,
                    "historyContextReason": history_context_reason,
                    "conversationId": request.conversationId,
                    "originalQuestion": request.question,
                    "effectiveQuestion": effective_question,
                    "analysisQuestion": analysis_question,
                }
            return payload

        # Clean NaN before JSON serialization
        clean_data = query_result.replace({np.nan: None}).to_dict(orient="records")

        # Pass the already-fetched dataframe directly to the analyzer
        # so it does NOT run a second query internally
        analysis_result = analyze_question_with_data(analysis_question, query_result)

        payload = {
            "success": True,
            "analysis": analysis_result,
            "data": clean_data,
            "question": analysis_question
        }
        if _analysis_debug_enabled():
            payload["debug"] = {
                "historyContextApplied": history_context_applied,
                "historyContextReason": history_context_reason,
                "conversationId": request.conversationId,
                "originalQuestion": request.question,
                "effectiveQuestion": effective_question,
                "analysisQuestion": analysis_question,
            }
        return payload

    except Exception as e:
        print(f"Analysis error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Analysis failed: {str(e)}")

@app.post("/api/signup")
async def signup_endpoint(request: AuthRequest):
    result = sign_up(request.email, request.password)
    if result["success"]:
        return result
    raise HTTPException(status_code=400, detail=result["error"])

@app.post("/api/login")
async def login_endpoint(request: AuthRequest):
    result = log_in(request.email, request.password)
    if result["success"]:
        return result
    raise HTTPException(status_code=400, detail=result["error"])


@app.post("/api/history/message")
async def save_history_message_endpoint(
    request: HistoryMessageRequest,
    authorization: Optional[str] = Header(default=None),
):
    uid = get_uid_from_authorization(authorization)

    if request.role not in {"user", "assistant"}:
        raise HTTPException(status_code=400, detail="role must be 'user' or 'assistant'")

    if not request.conversationId.strip():
        raise HTTPException(status_code=400, detail="conversationId is required")
    if not request.content.strip():
        raise HTTPException(status_code=400, detail="content is required")

    result = save_history_message(
        uid=uid,
        conversation_id=request.conversationId.strip(),
        role=request.role,
        content=request.content.strip(),
    )
    if result.get("success"):
        return {"success": True}
    raise HTTPException(status_code=500, detail=result.get("error", "Failed to save message"))


@app.get("/api/history")
async def list_history_endpoint(authorization: Optional[str] = Header(default=None)):
    uid = get_uid_from_authorization(authorization)
    result = list_conversations(uid)
    if result.get("success"):
        return result
    raise HTTPException(status_code=500, detail=result.get("error", "Failed to load history list"))


@app.get("/api/history/{conversation_id}")
async def get_history_endpoint(
    conversation_id: str,
    authorization: Optional[str] = Header(default=None),
):
    uid = get_uid_from_authorization(authorization)
    result = get_conversation_messages(uid, conversation_id)
    if result.get("success"):
        return result
    raise HTTPException(status_code=500, detail=result.get("error", "Failed to load history"))


@app.post("/api/debug/routing")
async def debug_routing_endpoint(request: DebugRoutingRequest):
    try:
        if not request.model_sql:
            raise HTTPException(
                status_code=400,
                detail="model_sql is required so routing can be validated against a real generated query."
            )
        return {
            "success": True,
            "debug": debug_query_routing(request.question, request.model_sql)
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Debug routing failed: {str(e)}")
