# Team45 Senior Design

Team45 Senior Design is an NBA analytics assistant that lets users ask basketball questions in plain English and receive stat-backed answers. The project combines a web chat interface, dashboard visualizations, SQL generation, PostgreSQL data access, and GPT-powered analysis.

The goal is to make NBA data easier for regular users to explore without needing to know SQL, database table names, or advanced analytics tooling. A user can open the site, ask a question such as "What was LeBron's best season from 2003 to 2022?" or "Show me the top 10 scorers this season," and the system will generate the database query, fetch the data, and return either a written analyst-style answer or a chart.

## How A User Uses It

The app has two main modes:

- `Analyst`: a chat-style experience for asking NBA stat questions and receiving written explanations.
- `Dashboards`: a visualization-focused page for generating charts such as leaderboards, comparisons, trends, skill profiles, and shot charts.

Typical usage:

1. Open the frontend in a browser.
2. Choose `Analyst` or `Dashboards`.
3. Type a basketball question in natural language.
4. Review the returned table, summary, or visualization.
5. Continue the conversation or start a new chat.

## Tech Stack

Frontend:

- `Next.js` 15
- `React` 19
- `TypeScript`
- `Tailwind CSS`
- `Radix UI`
- `Recharts`
- `Firebase` client SDK for auth/history features

Backend:

- `Python`
- `FastAPI`
- `Uvicorn`
- `Pandas` / `NumPy`
- `Psycopg2`
- `SQLGlot`
- `OpenAI` API
- `Firebase Admin`
- `Pyrebase`

Database and services:

- AWS RDS PostgreSQL for NBA stats data
- OpenAI/GPT for SQL generation and natural-language analysis
- Firebase for authentication and saved chat history

## Project Structure

```text
Team45seniordesign/
  backend/
    main.py                    # FastAPI entrypoint
    Analyzer/                  # Converts query results into user-facing analysis
    DashboardBackend/          # Dashboard query + chart generation flow
    Executer/                  # Database connection, SQL validation, query execution
    Interpreter/               # Natural-language to SQL flow and SQL safeguards
    requirements.txt           # Python backend dependencies
  frontend/
    ai_analyst/
      app/                     # Next.js App Router pages
      components/              # UI and chart components
      lib/                     # Frontend helpers
      package.json             # Frontend dependencies and scripts
  README.md
```

## Requirements

You need these installed locally or in venv:

- Python 3.13 recommended
- Node.js 20 recommended
- npm
- Git
- Access to the required `.env` values

Backend virtual environment requirements:

- The backend must run inside a Python virtual environment named `.venv` inside `backend/`.
- Install backend packages from `backend/requirements.txt`.
- Current backend Python dependencies are:
  - `flask`
  - `flask-cors`
  - `requests`
  - `psycopg2-binary`
  - `python-dotenv`
  - `openai`
  - `pydantic`
  - `uvicorn`
  - `fastapi`
  - `pandas`
  - `numpy`
  - `pyrebase4`
  - `firebase-admin`
  - `sqlglot`

Frontend requirements:

- Install frontend packages from `frontend/ai_analyst/package.json`.
- Run the frontend from `frontend/ai_analyst/`, not from the root folder.

Environment requirements:

- `backend/.env` must contain the project secrets and service credentials.
- At minimum, the backend needs OpenAI/GPT credentials and database credentials.
- Firebase credentials are required for authentication/history features.
- The AWS RDS PostgreSQL database must be available and reachable.

## Running Locally

Open two terminals: one for the backend and one for the frontend.

### 1. Clone Or Pull The Project

```bash
git clone https://github.com/daluzmikael/Team45seniordesign.git
cd Team45seniordesign
```

If you already have the project:

```bash
cd /Users/mikaeldaluz/Documents/sendes/Team45seniordesign
git pull
```

### 2. Set Up Backend Environment

From the project root:

```bash
cd backend
rm -rf .venv
python3.13 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

Make sure `backend/.env` exists before starting the backend. If you do not have the required `.env` values, ask a project member for them.

### 3. Start The Backend

From `backend/` with the venv activated:

```bash
python -m uvicorn main:app --reload --host 127.0.0.1 --port 8000
```

The backend should be available at:

```text
http://127.0.0.1:8000
```

You can check the FastAPI docs at:

```text
http://127.0.0.1:8000/docs
```

### 4. Install Frontend Dependencies

In a second terminal:

```bash
cd /Users/mikaeldaluz/Documents/sendes/Team45seniordesign/frontend/ai_analyst
npm install
```

### 5. Start The Frontend

From `frontend/ai_analyst/`:

```bash
npm run dev -- --hostname 127.0.0.1 --port 3000
```

The frontend should be available at:

```text
http://127.0.0.1:3000
```

## Restarting After A Pull

If you pulled new code and need to restart both servers:

Backend:

```bash
cd /Users/mikaeldaluz/Documents/sendes/Team45seniordesign/backend
source .venv/bin/activate
python -m pip install -r requirements.txt
python -m uvicorn main:app --reload --host 127.0.0.1 --port 8000
```

Frontend:

```bash
cd /Users/mikaeldaluz/Documents/sendes/Team45seniordesign/frontend/ai_analyst
npm install
npm run dev -- --hostname 127.0.0.1 --port 3000
```

If a port is already in use:

```bash
lsof -ti tcp:8000 | xargs kill
lsof -ti tcp:3000 | xargs kill
```

## Common Issues

If the frontend shows a missing `.next` module or stale chunk error, clear the Next.js build cache:

```bash
cd /Users/mikaeldaluz/Documents/sendes/Team45seniordesign/frontend/ai_analyst
rm -rf .next
npm run dev -- --hostname 127.0.0.1 --port 3000
```

If the backend venv points to an old folder after renaming or moving the project, recreate it:

```bash
cd /Users/mikaeldaluz/Documents/sendes/Team45seniordesign/backend
rm -rf .venv
python3.13 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

## Authors

- Konrad Koc
- Mikael Daluz
- Lawrence Mensah
- Shah Arian
- Von Lindenthal

## Important Warning

This project will not work without the required GPT/OpenAI key and Firebase credentials. If you do not have those keys, contact the project team.

The backend also depends on the AWS RDS PostgreSQL database. The RDS instance may not always be running or reachable during testing. If the app cannot connect to the database, or if you have any questions about credentials or access, reach out to the project team.
