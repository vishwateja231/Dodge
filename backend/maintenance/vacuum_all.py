import os

import psycopg

DB = os.getenv("DATABASE_URL", "")


def main():
    if not DB:
        raise RuntimeError("DATABASE_URL is required")
    with psycopg.connect(DB, prepare_threshold=None) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema='public'
                  AND table_type='BASE TABLE'
                ORDER BY table_name
                """
            )
            tables = [r[0] for r in cur.fetchall()]
            for t in tables:
                cur.execute(f"VACUUM ANALYZE {t}")
                print(f"VACUUM ANALYZE OK: {t}")


if __name__ == "__main__":
    main()
