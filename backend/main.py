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
from Interpreter.interpreter import run_query, debug_query_routing
import numpy as np
import pandas as pd
import re

app = FastAPI()

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
    "better", "worse", "more", "less", "compare", "vs ", " vs.",
    "between them", "of the two", "of those two",
    "who was", "who is", "who's", " or ", "as well as",
)

def _looks_like_comparison_followup(question: str) -> bool:
    q = " " + (question or "").strip().lower() + " "
    return any(m in q for m in _COMPARISON_FOLLOWUP_MARKERS)


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
        if history_messages and _should_apply_history_context(request.question):
            effective_question = _build_contextual_question(request.question, history_messages)

            if _is_affirmative_followup(request.question):
                player_name, season_label = _extract_latest_player_and_season_from_history(history_messages)
                if player_name and season_label:
                    effective_question += (
                        "\n\nFollow-up constraint: keep the SAME player and SAME season as the last answer. "
                        f"Use player_name ILIKE '%{player_name}%' and season_label '{season_label}' context "
                        "(do not advance to a different season)."
                    )

            # NEW: comparison continuity. If the follow-up is comparison-shaped but
            # the user dropped one or both player names, pull them from history.
            if _looks_like_comparison_followup(request.question):
                prior_names = _extract_player_names_from_history(history_messages)
                current_lower = request.question.lower()
                missing = [n for n in prior_names if n.lower() not in current_lower]
                if missing:
                    names_clause = " and ".join(missing[:2])
                    effective_question += (
                        f"\n\nFollow-up constraint: this is a CONTINUATION of an earlier comparison. "
                        f"Include {names_clause} alongside any players named in the current question. "
                        f"Treat as a multi-player comparison and return one row per player."
                    )
                    # Also rewrite the question that the analyzer sees so the
                    # narrative covers all parties, not just the named one.
                    request.question = (
                        f"{request.question} (continuing comparison with {names_clause})"
                    )

            history_context_applied = True
            history_context_reason = "request_history_used"
        # Fallback for older clients: load persisted history for authenticated users.
        elif request.conversationId and authorization and _should_apply_history_context(request.question):
            try:
                uid = get_uid_from_authorization(authorization)
                history_result = get_conversation_messages(uid, request.conversationId.strip())
                if history_result.get("success"):
                    fetched_history = history_result.get("messages", [])
                    if isinstance(fetched_history, list) and fetched_history:
                        effective_question = _build_contextual_question(
                            request.question, fetched_history
                        )
                        if _is_affirmative_followup(request.question):
                            player_name, season_label = _extract_latest_player_and_season_from_history(fetched_history)
                            if player_name and season_label:
                                effective_question += (
                                    "\n\nFollow-up constraint: keep the SAME player and SAME season as the last answer. "
                                    f"Use player_name ILIKE '%{player_name}%' and season_label '{season_label}' context "
                                    "(do not advance to a different season)."
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
            history_context_reason = "history_not_needed_for_standalone_question"
        elif request.conversationId and authorization:
            history_context_reason = "stored_history_not_needed_for_standalone_question"
        elif request.conversationId and not authorization:
            history_context_reason = "guest_without_request_history"

        # Run the query ONCE here — do not let query_analyzer run it again
        query_result = run_query(effective_question)

        # Handle empty or failed queries with a helpful message instead of crashing
        if query_result is None or query_result.empty:
            payload = {
                "success": True,
                "analysis": (
                    "No data was found for this query. This could mean:\n"
                    "- The player did not participate in the most recent playoffs or season.\n"
                    "- The player name may be misspelled or not recognized.\n"
                    "- Try specifying a season year, e.g. 'Giannis 2023 playoff performance'."
                ),
                "data": [],
                "question": request.question
            }
            if _analysis_debug_enabled():
                payload["debug"] = {
                    "historyContextApplied": history_context_applied,
                    "historyContextReason": history_context_reason,
                    "conversationId": request.conversationId,
                }
            return payload

        # Clean NaN before JSON serialization
        clean_data = query_result.replace({np.nan: None}).to_dict(orient="records")

        # Pass the already-fetched dataframe directly to the analyzer
        # so it does NOT run a second query internally
        analysis_result = analyze_question_with_data(request.question, query_result)

        payload = {
            "success": True,
            "analysis": analysis_result,
            "data": clean_data,
            "question": request.question
        }
        if _analysis_debug_enabled():
            payload["debug"] = {
                "historyContextApplied": history_context_applied,
                "historyContextReason": history_context_reason,
                "conversationId": request.conversationId,
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