Team45seniordesign
Senior Design project  
Access site here: https://team45seniordesign.vercel.app/

## Table of Contents
- [Project Overview](#project-overview)
- [Repo/Proj Structure](#repoproj-structure)
- [Backend Architecture](#backend-architecture)
- [Frontend Architecture](#frontend-architecture)
- [Local Setup and Prerequisites](#local-setup-and-prerequisites)
- [Example Usage](#example-usage)
- [Database Info](#database-info)
- [Current Features](#current-features)
- [Current Work in Progress Issues](#current-work-in-progress-issues)
- [Planned Future Features](#planned-future-features)
- [Current Project Status](#current-project-status)
- [Authors](#authors)

## Project Overview
Team45seniordesign is an NBA analytics assistant that turns natural language questions into charts and written analysis. It combines a Next.js frontend with a FastAPI backend that generates SQL, queries a PostgreSQL database, and summarizes results with LLM-assisted analysis.

## Repo/Proj Structure
```
Team45seniordesign/
  backend/                 # FastAPI API, query + analysis pipeline
    main.py
    DashboardBackend/
    Analyzer/
    Executer/
  frontend/
    ai_analyst/             # Next.js app (App Router)
  scrap2/                   # scratch work
  README.md
```

## Backend Architecture
- `FastAPI` app in `backend/main.py` exposes:
  - `POST /api/dashboards` for chart-oriented queries.
  - `POST /api/analysis` for long-form analysis output.
- `DashboardBackend/dashboardInterpreter.py`:
  - Builds a system prompt, uses OpenAI to generate SQL and a chart config.
  - Runs the SQL against PostgreSQL and formats results for the frontend.
- `Executer/query_bot.py`:
  - Converts natural language to SQL using OpenAI.
  - Applies safety checks and row limits before running queries.
- `Analyzer/query_analyzer.py`:
  - Scores and summarizes results, then requests LLM analysis output.

## Frontend Architecture
- Next.js app (App Router) in `frontend/ai_analyst`.
- `app/page.tsx` hosts the analysis chat interface.
- `app/dashboards/page.tsx` renders dashboard charts based on backend responses.
- `components/` contains UI and chart components used across pages.

## Local Setup and Prerequisites
Prerequisites:
- Python 3.10+
- Node.js 18+ and npm
- An OpenAI API key
- Access to the PostgreSQL data source (see Database Info)

Backend:
1. `cd /Users/mikaeldaluz/Documents/sendes/Team45seniordesign/backend`
2. Create `.env` from `envSample.txt` and set `OPENAI_API_KEY`.
3. `pip install -r requirements.txt`
4. `python -m uvicorn main:app --reload --port 8000`

Frontend:
1. `cd /Users/mikaeldaluz/Documents/sendes/Team45seniordesign/frontend/ai_analyst`
2. `npm install`
3. `npm run dev` (serves on `http://localhost:3000`)

Database (local notes):
- The backend currently connects to an AWS RDS PostgreSQL instance.
- Connection details are hardcoded in `backend/Executer/query_bot.py` and
  `backend/DashboardBackend/dashboardInterpreter.py`.
- To run against a local DB, update those files to read env vars and point to
  your local database, then load the required NBA tables.

## Example Usage
Dashboard examples (charts):
- "Who are the top 5 players with the most total minutes played in the 2024 season?"
- "Show me the trend for Shai Gilgeous-Alexander's attempted free throws per year in his career"
- "Show me a comparison of Lebron James and Kevin Durant points per game in the 2024 season"
- "Show me the skill profile of Anthony Edwards and Shai Gilgeous-Alexander in 2024."

Analysis examples (written output):
- "top 5 scorers 2023"
- "top 5 defenders 2023"
- "what are jaylen brown's stats"

## Database Info
- PostgreSQL (AWS RDS).
- Key tables:
  - `all_players_regular_YYYY_YYYY`: season summaries (per-game averages).
  - `player_game_logs`: per-game totals with `game_date`, `matchup`,
    and `season_type` (Regular Season/Playoffs).
- The dashboard prompt uses these tables to decide between season trends and
  game-log queries.

## Current Features
- Natural language to SQL generation for NBA stats.
- Dashboard charts: leaderboard, comparison, trend, and skill profile.
- Written analysis with domain-specific scoring (defense, shooting, playmaking, scoring).
- SQL safety checks (SELECT-only) and row limiting.

## Current Work in Progress Issues
- Pulling all historical data into the database.
- Security hardening (secrets in env, auth, stricter SQL validation).
- Frontend polish (UX, error handling, loading states, and layout refinement).

## Planned Future Features
- Additional sports and leagues beyond NBA.
- Expanded chart types and cross-season comparisons.
- User accounts and saved analyses.

## Current Project Status
Active development / prototype stage. Core flows work end-to-end, but the system
is not production-hardened.

## Authors
- Konrad Koc
- Mikael Daluz
- Lawrence Mensah
- Shah Arian
- Von Lindenthal
