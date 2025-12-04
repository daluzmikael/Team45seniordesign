from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from DashboardBackend.dashboardInterpreter import interpret_question

# Initialize the app with FastAPI
app = FastAPI()

# Allows your Next.js app on port 3000 to talk to this
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
class QueryRequest(BaseModel):
    question: str

#Specifically for /api/dashboards endpoint, not the one for the written analysis
#This connects to the page.tsx under frontend/ai_analyst/app/dashboards
#For now this stays untouched
@app.post("/api/dashboards")
async def dashboard_endpoint(request: QueryRequest):
    result = interpret_question(request.question)
    
    if result.get("success"):
        return result
    else:
        # ADD THIS LINE TO PRINT THE ERROR TO YOUR TERMINAL:
        print("Error details:", result.get("error"), result.get("details")) 
        
        raise HTTPException(status_code=400, detail=result.get("error", "Unknown error"))

#This will be for the analysis, still needs to be complete
#This connects to the page.tsx under frontend/ai_analyst/app
'''
@app.post("/api/analysis")
async def analysis(request: QueryRequest):
    result = interpret_question(request.question)
        raise HTTPException(status_code=400, detail=result.get("error", "Unknown error"))
'''
# To run this:
# uvicorn main:app --reload --port 8000