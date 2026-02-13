"""
To be deleted. Call executor.py and interpreter.py functions directly instead of this layer.
"""

import psycopg2
import re
import pandas as pd
from openai import OpenAI
import os

# 1. OpenAI client
from dotenv import load_dotenv

load_dotenv()
# Setting up OpenAI client
print("API Key Loaded:", os.getenv("OPENAI_API_KEY"))
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# 2. Connect to PostgreSQL (AWS RDS)
def get_connection():
    """Get a fresh database connection"""
    return psycopg2.connect(
        host="nba-sdp-project.cs1c0smw8vqa.us-east-1.rds.amazonaws.com",
        port=5432,
        dbname="NBA-STATS",
        user="VonLindenthal",
        password="Vlindenthal1!",
        sslmode="require"
    )

conn = get_connection()
cursor = conn.cursor()

# Analyzer bridge globals
df_output = None
user_input = None


# 3. Read DB schema (for GPT prompt)
def get_db_schema():
    global conn, cursor
    try:
        cursor.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public';
        """)
    except Exception:
        # Reconnect if connection is dead
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public';
        """)

    tables = cursor.fetchall()
    schema_description = ""

    for table in tables:
        table_name = table[0]

        cursor.execute(f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = '{table_name}'
        """)

        columns = cursor.fetchall()
        column_list = ", ".join([col[0] for col in columns])
        schema_description += f"{table_name}({column_list})\n"

    return schema_description


# 4. SQL safety checker
def is_safe_sql(sql_query):
    sql_lower = sql_query.strip().lower()
    sql_lower = re.sub(r"--.*?\n", "", sql_lower).strip()

    if "select" not in sql_lower:
        return False

    dangerous = ["insert", "update", "delete", "drop",
                 "alter", "create", "replace", "truncate"]

    for keyword in dangerous:
        if re.search(rf"\b{keyword}\b", sql_lower):
            return False

    return True


# 5. Add LIMIT automatically
def limit_rows(sql_query, limit=50):
    """Add LIMIT to query if not present, handling UNION queries properly"""
    sql_lower = sql_query.lower()
    
    # Check if LIMIT already exists
    if "limit" in sql_lower:
        return sql_query
    
    # For UNION queries, wrap in subquery with limit
    if "union" in sql_lower:
        sql_query = sql_query.rstrip(";")
        return f"SELECT * FROM ({sql_query}) AS combined_results LIMIT {limit};"
    
    # For simple queries, just append LIMIT
    sql_query = sql_query.rstrip(";")
    return f"{sql_query} LIMIT {limit};"

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

    Fix the SQL to match the schema exactly.
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

    return fixed_sql



# 6. Convert natural language → SQL
def natural_language_to_sql(user_input_param: str):
    global df_output, conn, cursor, user_input

    user_input = user_input_param
    schema_description = get_db_schema()

    prompt = f"""
You are a senior SQL data engineer.
Your task is to convert a natural language request into a VALID PostgreSQL SELECT query for the NBA stats database.
RULES:
- Use ONLY tables and columns that exist in the schema below.
- Do NOT invent columns.
- Do NOT guess column names.
- If unsure, choose the closest matching column from the schema.
- Use explicit table aliases when joining.
- Fully qualify ambiguous columns (table.column).
- ONLY generate SELECT queries.
- Do NOT include explanations.
- Do NOT include markdown.
- Output SQL only.

PERFORMANCE RULES:
- Use GROUP BY only when aggregation is required.
- Avoid SELECT * unless explicitly requested.
- Use the most efficient query structure.

DATABASE SCHEMA:
{schema_description}

USER REQUEST:
{user_input_param}

Generate the SQL"""

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

    except Exception as e:
        print(f"[ERROR] OpenAI API error: {e}")
        return None

    sql_query = limit_rows(sql_query)

    if not is_safe_sql(sql_query):
        print(f"[ERROR] Unsafe SQL generated: {sql_query}")
        return None

    # Execution with self query repair loop
    max_attempts = 3

    for attempt in range(max_attempts):
        try:
            print(f"[DEBUG] Attempt {attempt+1} executing query...")
            cursor.execute(sql_query)

            rows = cursor.fetchall()
            colnames = [desc[0] for desc in cursor.description]

            df = pd.DataFrame(rows, columns=colnames)
            print(f"[DEBUG] Query returned {len(df)} rows")

            df_output = df
            #conn.commit() not needed here -kon
            return df

        except Exception as e:
            error_message = str(e)
            print(f"[ERROR] SQL execution error: {error_message}")

            # Only repair schema-related errors
            if any(keyword in error_message.lower()
                   for keyword in ["does not exist", "column", "relation"]):

                print("[DEBUG] Attempting schema self-repair...")

                sql_query = repair_sql_error(
                    original_sql=sql_query,
                    error_message=error_message,
                    schema_description=schema_description,
                    user_input=user_input_param
                )

                sql_query = limit_rows(sql_query)

                if not is_safe_sql(sql_query):
                    print("[ERROR] Repaired SQL is unsafe.")
                    return None

                continue

            else:
                print("[ERROR] Non-repairable error.")
                conn.rollback()
                return None

    print("[ERROR] Max repair attempts reached.")
    conn.rollback()
    return None


# 7. Interactive query loop
if __name__ == "__main__":
    print("Welcome to HoopQuery! Type 'quit' to exit.")
    while True:
        user_inp = input("\nAsk a question about NBA data: ")
        if user_inp.lower() in ["quit", "exit"]:
            break
        natural_language_to_sql(user_inp)


def run_query(question: str):
    return natural_language_to_sql(question)