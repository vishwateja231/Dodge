import os

import psycopg

DB = os.getenv("DATABASE_URL", "")
DATASET_ROOT = r"C:/Users/vishw/Downloads/Dodge Ai Assignment/data/sap-o2c-data"


def main():
    if not DB:
        raise RuntimeError("DATABASE_URL is required")

    dataset_folders = {
        d
        for d in os.listdir(DATASET_ROOT)
        if os.path.isdir(os.path.join(DATASET_ROOT, d))
    }

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

            fk_map = {t: {"as_child": 0, "as_parent": 0} for t in tables}
            cur.execute(
                """
                SELECT
                  tc.table_name AS child_table,
                  ccu.table_name AS parent_table
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                  ON tc.constraint_name = kcu.constraint_name
                 AND tc.table_schema = kcu.table_schema
                JOIN information_schema.constraint_column_usage ccu
                  ON ccu.constraint_name = tc.constraint_name
                 AND ccu.table_schema = tc.table_schema
                WHERE tc.table_schema='public'
                  AND tc.constraint_type='FOREIGN KEY'
                """
            )
            for child, parent in cur.fetchall():
                if child in fk_map:
                    fk_map[child]["as_child"] += 1
                if parent in fk_map:
                    fk_map[parent]["as_parent"] += 1

            print("TABLE|ROW_COUNT|MATCHES_DATASET_FOLDER|FK_AS_CHILD|FK_AS_PARENT")
            for t in tables:
                cur.execute(f"SELECT COUNT(*) FROM {t}")
                count = cur.fetchone()[0]
                matches = "yes" if t in dataset_folders else "no"
                print(
                    f"{t}|{count}|{matches}|{fk_map[t]['as_child']}|{fk_map[t]['as_parent']}"
                )


if __name__ == "__main__":
    main()
