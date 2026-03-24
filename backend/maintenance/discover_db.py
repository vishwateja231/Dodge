import os

import psycopg

DB = os.getenv("DATABASE_URL", "")


def main():
    if not DB:
        raise RuntimeError("DATABASE_URL is required")
    with psycopg.connect(DB, prepare_threshold=None) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema='public'
                ORDER BY table_name
                """
            )
            tables = [r[0] for r in cur.fetchall()]
            for t in tables:
                print(f"\n[{t}]")
                cur.execute(
                    """
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_schema='public' AND table_name=%s
                    ORDER BY ordinal_position
                    """,
                    [t],
                )
                for c, dt in cur.fetchall():
                    print(f"  {c}: {dt}")


if __name__ == "__main__":
    main()
