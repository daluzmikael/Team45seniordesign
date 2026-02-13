import psycopg2
import re
import pandas as pd
from openai import OpenAI
import os

# 1. OpenAI client
from dotenv import load_dotenv

load_dotenv()
# Setting up OpenAI client
#print("API Key Loaded:", os.getenv("OPENAI_API_KEY"))
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


# ------------------------------
# 3. Read DB schema (for GPT prompt)
# ------------------------------
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


# ------------------------------
# 4. SQL safety checker
# ------------------------------
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


# ------------------------------
# 5. Add LIMIT automatically
# ------------------------------
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


# ------------------------------
# 6. Convert natural language → SQL
# ------------------------------
def natural_language_to_sql(user_input_param: str):
    global df_output, conn, cursor
    global user_input
    user_input = user_input_param

    schema_description = get_db_schema()

    prompt = f"""
You convert natural language into SQL queries.
Use only the database schema below:

{schema_description}

User request: "{user_input_param}"

Return ONLY a safe SQL SELECT query.
Do NOT include markdown or code fences.
"""

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are a SQL query generator. Return ONLY valid SQL queries without any markdown formatting or explanations. Keep queries concise and efficient."},
                {"role": "user", "content": prompt}
            ],
            temperature=0,
            max_tokens=1500  # Increased from 500 to handle longer queries
        )

        sql_query = response.choices[0].message.content.strip()
        print(f"\n[DEBUG] Generated SQL: {sql_query[:200]}...")  # Only show first 200 chars
    except Exception as e:
        print(f"[ERROR] OpenAI API error: {e}")
        return None

    # strip accidental markdown formatting
    sql_query = sql_query.replace("```sql", "").replace("```", "").strip()

    sql_query = limit_rows(sql_query)

    if not is_safe_sql(sql_query):
        print(f"\n[ERROR] Unsafe SQL generated: {sql_query}")
        return None

    print(f"[DEBUG] Executing SQL query...")

    # ------------------------------
    # Execute the query and return DataFrame
    # ------------------------------
    try:
        cursor.execute(sql_query)
        rows = cursor.fetchall()
        colnames = [desc[0] for desc in cursor.description]

        df = pd.DataFrame(rows, columns=colnames)
        print(f"[DEBUG] Query returned {len(df)} rows, {len(df.columns)} columns")

        df_output = df
        conn.commit()  # Commit the transaction
        return df

    except Exception as e:
        print(f"[ERROR] SQL execution error: {e}")
        print(f"[ERROR] Failed query was: {sql_query}")
        conn.rollback()  # Rollback failed transaction
        # Try to reconnect for next query
        try:
            conn = get_connection()
            cursor = conn.cursor()
        except Exception:
            pass
        return None


# ------------------------------
# 7. Interactive query loop
# ------------------------------
if __name__ == "__main__":
    print("Welcome to QUERY BOT (Pandas Edition)! Type 'quit' to exit.")
    while True:
        user_inp = input("\nAsk a question about NBA data: ")
        if user_inp.lower() in ["quit", "exit"]:
            break
        natural_language_to_sql(user_inp)


def run_query(question: str):
    return natural_language_to_sql(question)
