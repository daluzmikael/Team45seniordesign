from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from DashboardBackend.dashboardInterpreter import interpret_question
from Analyzer.query_analyzer import analyze_question_with_data
from auth import sign_up, log_in
from Interpreter.interpreter import run_query
import numpy as np
import pandas as pd

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

class AuthRequest(BaseModel):
    email: str
    password: str

@app.post("/api/dashboards")
async def dashboard_endpoint(request: QueryRequest):
    result = interpret_question(request.question)
    if result.get("success"):
        return result
    else:
        print("Error details:", result.get("error"), result.get("details"))
        raise HTTPException(status_code=400, detail=result.get("error", "Unknown error"))

@app.post("/api/analysis")
async def analysis_endpoint(request: QueryRequest):
    try:
        print("----HIT----- /api/analysis")

        # Run the query ONCE here — do not let query_analyzer run it again
        query_result = run_query(request.question)

        # Handle empty or failed queries with a helpful message instead of crashing
        if query_result is None or query_result.empty:
            return {
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

        # Clean NaN before JSON serialization
        clean_data = query_result.replace({np.nan: None}).to_dict(orient="records")

        # Pass the already-fetched dataframe directly to the analyzer
        # so it does NOT run a second query internally
        analysis_result = analyze_question_with_data(request.question, query_result)

        return {
            "success": True,
            "analysis": analysis_result,
            "data": clean_data,
            "question": request.question
        }

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