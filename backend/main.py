import logging
import os

# Align root log level with env before importing Interpreter/Executor (uvicorn may configure logging first).
logging.getLogger().setLevel(
    getattr(logging, (os.getenv("LOG_LEVEL") or "INFO").upper(), logging.INFO)
)

from fastapi import FastAPI, HTTPException, Header
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
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
from Interpreter.interpreter import run_query, debug_query_routing, get_last_tables_used
import numpy as np
import pandas as pd
import re
import time
from threading import Lock

app = FastAPI()

_RESPONSE_CACHE_LOCK = Lock()
_RESPONSE_CACHE: dict[str, tuple[float, Dict[str, Any]]] = {}


def _cache_ttl_seconds() -> int:
    return int(os.getenv("RESPONSE_CACHE_TTL_SECONDS", "45"))


def _cache_max_items() -> int:
    return int(os.getenv("RESPONSE_CACHE_MAX_ITEMS", "128"))


def _make_cache_key(question: str, effective_question: str, history: List[Dict[str, Any]]) -> str:
    # Include effective question and compact history footprint so follow-ups stay correct.
    return f"{question.strip()}||{effective_question.strip()}||{repr(history[-4:])}"


def _get_cached_response(cache_key: str) -> Optional[Dict[str, Any]]:
    ttl = _cache_ttl_seconds()
    now = time.time()
    with _RESPONSE_CACHE_LOCK:
        row = _RESPONSE_CACHE.get(cache_key)
        if not row:
            return None
        ts, payload = row
        if (now - ts) > ttl:
            _RESPONSE_CACHE.pop(cache_key, None)
            return None
        return dict(payload)


def _set_cached_response(cache_key: str, payload: Dict[str, Any]) -> None:
    with _RESPONSE_CACHE_LOCK:
        if len(_RESPONSE_CACHE) >= _cache_max_items():
            # Remove oldest item (insertion-order dict in modern Python).
            oldest_key = next(iter(_RESPONSE_CACHE))
            _RESPONSE_CACHE.pop(oldest_key, None)
        _RESPONSE_CACHE[cache_key] = (time.time(), dict(payload))

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
    current_question: str, history_messages: List[Dict[str, Any]], max_messages: int = 4
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
        if len(content) > 150:
            content = content[:150] + "..."
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


def _is_affirmative_followup(question: str) -> bool:
    q = (question or "").strip().lower()
    return q in {"yes", "yeah", "yep", "yup", "sure", "ok", "okay", "do it", "go ahead"}


def _has_explicit_season_reference(question: str) -> bool:
    q = (question or "").lower()
    return (
        re.search(r"\b(19\d{2}|20\d{2})\b", q) is not None
        or re.search(r"\b(19\d{2}|20\d{2})\s*[-/_]\s*(\d{2}|19\d{2}|20\d{2})\b", q) is not None
        or "this season" in q
        or "current season" in q
        or "last season" in q
        or "this playoff" in q
        or "current playoff" in q
        or "last playoff" in q
        or "this postseason" in q
        or "current postseason" in q
        or "last postseason" in q
    )


def _should_force_previous_context(question: str) -> bool:
    q = (question or "").strip().lower()
    if not q or _has_explicit_season_reference(q):
        return False

    vague_markers = [
        "compare",
        "vs",
        "versus",
        "between",
        "them",
        "those two",
        "both",
        "who is better",
        "which one",
        "what about",
        "how about",
        "and ",
    ]
    if any(marker in q for marker in vague_markers):
        return True

    token_count = len(re.findall(r"\w+", q))
    return token_count <= 6


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
        "and ",
        "also",
        "same ",
        "that ",
        "those ",
        "them",
        "him",
        "his ",
        "her ",
        "their ",
        "break that down",
        "which one",
        "who is better",
        "who's better",
        "yes",
        "yeah",
        "yep",
        "yup",
        "ok",
        "okay",
        "sure",
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

        effective_question = request.question
        history_context_applied = False
        history_context_reason = "no_history_available"
        history_messages: List[Dict[str, Any]] = _sanitize_history_messages(request.history)

        # Prefer history passed by the frontend (works for both guest and auth chats).
        # Always apply available history so sequential questions consistently keep context.
        if history_messages:
            effective_question = _build_contextual_question(request.question, history_messages)
            player_name, season_label = _extract_latest_player_and_season_from_history(history_messages)
            if season_label and (_is_affirmative_followup(request.question) or _should_force_previous_context(request.question)):
                effective_question += (
                    "\n\nFollow-up constraint: preserve previously established context unless user explicitly changes it. "
                    f"Use the same season context: {season_label}. "
                    "(Do not change to a different season unless the user asks for one.)"
                )
                if player_name:
                    effective_question += (
                        f" Keep player context anchored to {player_name} when resolving pronouns/references."
                    )
            history_context_applied = True
            history_context_reason = "request_history_used"
        # Fallback for older clients: load persisted history for authenticated users.
        elif request.conversationId and authorization:
            try:
                uid = get_uid_from_authorization(authorization)
                history_result = get_conversation_messages(uid, request.conversationId.strip())
                if history_result.get("success"):
                    fetched_history = history_result.get("messages", [])
                    if isinstance(fetched_history, list) and fetched_history:
                        effective_question = _build_contextual_question(
                            request.question, fetched_history
                        )
                        player_name, season_label = _extract_latest_player_and_season_from_history(fetched_history)
                        if season_label and (_is_affirmative_followup(request.question) or _should_force_previous_context(request.question)):
                            effective_question += (
                                "\n\nFollow-up constraint: preserve previously established context unless user explicitly changes it. "
                                f"Use the same season context: {season_label}. "
                                "(Do not change to a different season unless the user asks for one.)"
                            )
                            if player_name:
                                effective_question += (
                                    f" Keep player context anchored to {player_name} when resolving pronouns/references."
                                )
                        history_context_applied = True
                        history_context_reason = "stored_history_used"
                    else:
                        history_context_reason = "stored_history_empty"
                else:
                    history_context_reason = "history_lookup_failed"
            except Exception as history_error:
                # Keep analysis available even if history lookup fails.
                print(f"History context skipped: {history_error}")
                history_context_reason = "history_lookup_exception"
        elif history_messages:
            history_context_reason = "request_history_present_but_unused"
        elif request.conversationId and authorization:
            history_context_reason = "stored_history_unavailable"
        elif request.conversationId and not authorization:
            history_context_reason = "guest_without_request_history"

        # Force current-season interpretation for undated regular-season performance asks.
        q_lower = (effective_question or "").lower()
        has_explicit_year = re.search(r"\b(19\d{2}|20\d{2})\b", q_lower) is not None
        has_regular_perf_intent = ("regular season" in q_lower) and any(
            k in q_lower for k in ["performance", "season stats", "season stat", "season averages", "stats"]
        )
        if has_regular_perf_intent and not has_explicit_year:
            effective_question = f"{effective_question.strip()} in 2025-26 season"

        cache_key = _make_cache_key(request.question, effective_question, history_messages)
        cached_payload = _get_cached_response(cache_key)
        if cached_payload is not None:
            if _analysis_debug_enabled():
                cached_payload.setdefault("debug", {})
                cached_payload["debug"]["cacheHit"] = True
            return cached_payload

        # Run the query ONCE here — do not let query_analyzer run it again
        query_result = run_query(effective_question)

        tables_used = get_last_tables_used()
        if tables_used:
            print("---- TABLES USED ----")
            print(", ".join(tables_used))

        # Handle empty or failed queries with a helpful message instead of crashing
        if query_result is None or query_result.empty:
            q_lower = (request.question or "").lower()
            if "playoff" in q_lower or "postseason" in q_lower:
                empty_message = (
                    "I don't currently have enough playoff data to answer this question confidently.\n"
                    "No playoff data was found for this query in the available playoff tables.\n"
                    "- This player may not have records in the currently selected playoff seasons.\n"
                    "- Try a specific playoff year (example: 'Giannis 2021 playoff performance').\n"
                    "- If you want, ask for regular-season stats instead."
                )
            else:
                empty_message = (
                    "I don't currently have enough data in the database to answer this question confidently.\n"
                    "No data matched this query.\n"
                    "- The player name may be misspelled or formatted differently in the database.\n"
                    "- The requested season/split may not exist in the currently available tables.\n"
                    "- Try a specific season, e.g. 'Giannis 2020 season' or 'Giannis 2023 playoff performance'."
                )
            payload = {
                "success": True,
                "analysis": empty_message,
                "data": [],
                "question": request.question,
                "tablesUsed": tables_used,
            }
            if _analysis_debug_enabled():
                payload["debug"] = {
                    "historyContextApplied": history_context_applied,
                    "historyContextReason": history_context_reason,
                    "conversationId": request.conversationId,
                }
            _set_cached_response(cache_key, payload)
            return payload

        # Clean NaN before JSON serialization
        clean_data = query_result.replace({np.nan: None}).to_dict(orient="records")

        # Pass the already-fetched dataframe directly to the analyzer
        # so it does NOT run a second query internally
        analysis_result = analyze_question_with_data(request.question, query_result)
        q_lower = (request.question or "").lower()
        if "clutch" in q_lower:
            low_analysis = (analysis_result or "").lower()
            if ("last 5 minutes" not in low_analysis) or ("totals" not in low_analysis):
                analysis_result = (
                    (analysis_result or "").rstrip()
                    + "\n\nThis clutch response uses last 5 minutes context totals from the clutch table."
                )

        payload = {
            "success": True,
            "analysis": analysis_result,
            "data": clean_data,
            "question": request.question,
            "tablesUsed": tables_used,
        }
        if _analysis_debug_enabled():
            payload["debug"] = {
                "historyContextApplied": history_context_applied,
                "historyContextReason": history_context_reason,
                "conversationId": request.conversationId,
            }
        _set_cached_response(cache_key, payload)
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