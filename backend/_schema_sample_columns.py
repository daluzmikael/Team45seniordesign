"""One-off: print column names for sample NBA-STATS tables."""
from __future__ import annotations

import os
from pathlib import Path

import psycopg2
from dotenv import load_dotenv


def main() -> None:
    load_dotenv(Path(__file__).resolve().parent / ".env")
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "nba-sdp-project.cs1c0smw8vqa.us-east-1.rds.amazonaws.com"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "NBA-STATS"),
        user=os.getenv("POSTGRES_USER", "VonLindenthal"),
        password=os.environ["POSTGRES_PASSWORD"],
        sslmode=os.getenv("POSTGRES_SSLMODE", "require"),
    )
    cur = conn.cursor()
    tables = [
        "player_pergame_regularseason_2023_2024",
        "player_totals_regularseason_2023_2024",
        "player_totals_playoffseason_2023_2024",
        "advance_totals_regularseason_2023_2024",
    ]
    for t in tables:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s
            ORDER BY ordinal_position;
            """,
            (t,),
        )
        cols = [r[0] for r in cur.fetchall()]
        print("===", t, "count", len(cols))
        print(",".join(cols))
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
