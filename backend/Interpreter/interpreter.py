import os
from dotenv import load_dotenv
from openai import OpenAI

from executor import (
    get_connection,
    get_db_schema,
    is_safe_sql,
    limit_rows,
    execute_query
)

load_dotenv()
print("API Key Loaded:", os.getenv("OPENAI_API_KEY"))
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))


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

    conn = get_connection()
    schema_description = get_db_schema(conn)

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

DATABASE SCHEMA:
{schema_description}

USER REQUEST:
{user_input_param}

Generate the SQL
"""

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

    max_attempts = 3

    for attempt in range(max_attempts):
        try:
            print(f"[DEBUG] Attempt {attempt+1} executing query...")
            return execute_query(conn, sql_query)

        except Exception as e:
            error_message = str(e)
            print(f"[ERROR] SQL execution error: {error_message}")

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
                return None

    print("[ERROR] Max repair attempts reached.")
    return None


def run_query(question: str):
    return natural_language_to_sql(question)
