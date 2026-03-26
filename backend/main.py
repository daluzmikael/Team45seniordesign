from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from DashboardBackend.dashboardInterpreter import interpret_question
from Analyzer.query_analyzer import analyze_question
from auth import sign_up, log_in

# Initialize the app with FastAPI
app = FastAPI()

# Root route so Render health checks don't hit 404 on "/"
@app.get("/")
async def root():
    return {"status": "ok", "message": "API is running"}

# Allows both local frontend and deployed Vercel frontend to access the backend
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

# Specifically for /api/dashboards endpoint, not the one for the written analysis
# This connects to the page.tsx under frontend/ai_analyst/app/dashboards
@app.post("/api/dashboards")
async def dashboard_endpoint(request: QueryRequest):
    result = interpret_question(request.question)

    if result.get("success"):
        return result
    else:
        print("Error details:", result.get("error"), result.get("details"))
        raise HTTPException(status_code=400, detail=result.get("error", "Unknown error"))

# This connects to the page.tsx under frontend/ai_analyst/app
@app.post("/api/analysis")
async def analysis_endpoint(request: QueryRequest):
    try:
        # Call the analyze_question function from query_analyzer
        analysis_result = analyze_question(request.question)

        # Check if there was an error in the analysis
        if isinstance(analysis_result, str) and analysis_result.startswith("Error:"):
            raise HTTPException(status_code=500, detail=analysis_result)

        return {
            "success": True,
            "analysis": analysis_result,
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

# To run locally:
# uvicorn main:app --reload --port 8000
