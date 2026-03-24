"""
Production NDJSON -> PostgreSQL loader with dynamic schema verification.

Usage:
  set DATABASE_URL=postgresql://...
  python migrate_postgres.py
"""
from __future__ import annotations

import glob
import json
import os
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import psycopg
from psycopg import sql
from psycopg.types.json import Json


BASE_DIR = Path(__file__).resolve().parent.parent / "data" / "sap-o2c-data"
DATABASE_URL = os.getenv("DATABASE_URL", "")

# Parent-first order for stronger FK consistency; unknown folders are appended.
PREFERRED_LOAD_ORDER = [
    "business_partners",
    "plants",
    "products",
    "product_descriptions",
    "product_plants",
    "product_storage_locations",
    "business_partner_addresses",
    "customer_company_assignments",
    "customer_sales_area_assignments",
    "sales_order_headers",
    "sales_order_items",
    "sales_order_schedule_lines",
    "outbound_delivery_headers",
    "outbound_delivery_items",
    "billing_document_headers",
    "billing_document_items",
    "billing_document_cancellations",
    "journal_entry_items_accounts_receivable",
    "payments_accounts_receivable",
]


def to_snake(name: str) -> str:
    out = []
    for i, ch in enumerate(name):
        if ch.isupper() and i > 0 and (not name[i - 1].isupper()):
            out.append("_")
        out.append(ch.lower())
    return "".join(out)


def parse_time_dict(v):
    if not isinstance(v, dict):
        return None
    try:
        return f"{int(v.get('hours', 0)):02d}:{int(v.get('minutes', 0)):02d}:{int(v.get('seconds', 0)):02d}"
    except Exception:
        return None


def parse_date(v):
    if v in (None, ""):
        return None
    if isinstance(v, str):
        for fmt in ("%Y-%m-%d", "%Y%m%d"):
            try:
                return datetime.strptime(v[:10], fmt).date()
            except Exception:
                continue
    return None


def parse_ts(v):
    if v in (None, ""):
        return None
    if isinstance(v, str):
        txt = v.replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(txt)
        except Exception:
            return None
    return None


def parse_numeric(v):
    if v in (None, ""):
        return None
    try:
        return Decimal(str(v))
    except Exception:
        return None


def infer_pg_type(values: list):
    non_null = [v for v in values if v is not None]
    if not non_null:
        return "TEXT"
    if any(isinstance(v, (dict, list)) for v in non_null):
        return "JSONB"
    if all(isinstance(v, bool) for v in non_null):
        return "BOOLEAN"
    if all(isinstance(v, (int, float, Decimal)) for v in non_null):
        return "NUMERIC"
    str_vals = [v for v in non_null if isinstance(v, str)]
    if len(str_vals) == len(non_null):
        if all(parse_ts(v) is not None for v in str_vals):
            return "TIMESTAMP"
        if all(parse_date(v) is not None for v in str_vals):
            return "DATE"
        if all(parse_time_dict(v) is not None for v in non_null if isinstance(v, dict)):
            return "TIME"
    return "TEXT"


def discover_folder_tables():
    folders = [p.name for p in BASE_DIR.iterdir() if p.is_dir()]
    ordered = [f for f in PREFERRED_LOAD_ORDER if f in folders]
    ordered.extend(sorted([f for f in folders if f not in set(ordered)]))
    return ordered


def load_jsonl(folder: str):
    paths = sorted(glob.glob(str(BASE_DIR / folder / "*.jsonl")))
    for path in paths:
        with open(path, "r", encoding="utf-8") as f:
            ln = 0
            for line in f:
                ln += 1
                line = line.strip()
                if not line:
                    continue
                try:
                    yield path, ln, json.loads(line)
                except Exception as ex:
                    yield path, ln, {"__corrupt__": str(ex), "__raw__": line}


def table_exists(conn, table: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT EXISTS (
              SELECT 1
              FROM information_schema.tables
              WHERE table_schema='public' AND table_name=%s
            )
            """,
            [table],
        )
        return bool(cur.fetchone()[0])


def pg_columns(conn, table_name: str):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name, data_type, udt_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = %s
            ORDER BY ordinal_position
            """,
            [table_name],
        )
        rows = cur.fetchall()
    return {r[0]: {"data_type": r[1], "udt_name": r[2]} for r in rows}


def pk_columns(conn, table_name: str):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema = kcu.table_schema
            WHERE tc.table_schema='public'
              AND tc.table_name=%s
              AND tc.constraint_type='PRIMARY KEY'
            ORDER BY kcu.ordinal_position
            """,
            [table_name],
        )
        return [r[0] for r in cur.fetchall()]


def ensure_columns(conn, table: str, samples: list[dict]):
    existing = pg_columns(conn, table)
    by_col = {}
    for obj in samples:
        for k, v in obj.items():
            if k.startswith("__"):
                continue
            sk = to_snake(k)
            by_col.setdefault(sk, []).append(v)
    to_add = []
    for col, vals in by_col.items():
        if col not in existing:
            to_add.append((col, infer_pg_type(vals)))
    if not to_add:
        return []
    with conn.cursor() as cur:
        for col, typ in to_add:
            cur.execute(
                sql.SQL("ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} {}").format(
                    sql.Identifier(table),
                    sql.Identifier(col),
                    sql.SQL(typ),
                )
            )
    conn.commit()
    return to_add


def convert_value(col_meta: dict, raw_val):
    if raw_val is None:
        return None
    data_type = col_meta["data_type"].lower()
    udt = col_meta["udt_name"].lower()

    if data_type in ("json", "jsonb"):
        return Json(raw_val)
    if data_type == "boolean":
        if isinstance(raw_val, bool):
            return raw_val
        if isinstance(raw_val, str):
            return raw_val.lower() == "true"
        return bool(raw_val)
    if data_type in ("integer", "bigint", "smallint"):
        try:
            return int(raw_val)
        except Exception:
            return None
    if data_type in ("numeric", "double precision", "real", "decimal"):
        return parse_numeric(raw_val)
    if data_type == "date":
        return parse_date(raw_val)
    if "timestamp" in data_type:
        return parse_ts(raw_val)
    if data_type == "time without time zone":
        if isinstance(raw_val, dict):
            return parse_time_dict(raw_val)
        return raw_val
    if data_type == "ARRAY" or udt.startswith("_"):
        if isinstance(raw_val, list):
            return raw_val
        return None
    return raw_val


def build_row(table_cols: dict, obj: dict):
    snake_obj = {to_snake(k): v for k, v in obj.items() if not k.startswith("__")}
    row = {}
    for col, meta in table_cols.items():
        row[col] = convert_value(meta, snake_obj.get(col))
    return row


def upsert_rows(conn, table: str, pk_cols: list[str], rows: list[dict]):
    if not rows:
        return 0
    cols = list(rows[0].keys())
    per_row_vals = sql.SQL("({})").format(sql.SQL(", ").join(sql.Placeholder() for _ in cols))
    values_sql = sql.SQL(", ").join([per_row_vals] * len(rows))
    insert = sql.SQL("INSERT INTO {table} ({cols}) VALUES {vals}").format(
        table=sql.Identifier(table),
        cols=sql.SQL(", ").join(sql.Identifier(c) for c in cols),
        vals=values_sql,
    )
    if pk_cols:
        non_pk = [c for c in cols if c not in set(pk_cols)]
        if non_pk:
            insert += sql.SQL(" ON CONFLICT ({pk}) DO UPDATE SET {updates}").format(
                pk=sql.SQL(", ").join(sql.Identifier(c) for c in pk_cols),
                updates=sql.SQL(", ").join(
                    sql.SQL("{}=EXCLUDED.{}").format(sql.Identifier(c), sql.Identifier(c))
                    for c in non_pk
                ),
            )
        else:
            insert += sql.SQL(" ON CONFLICT ({pk}) DO NOTHING").format(
                pk=sql.SQL(", ").join(sql.Identifier(c) for c in pk_cols)
            )
    flat_params = []
    for r in rows:
        flat_params.extend([r[c] for c in cols])
    with conn.cursor() as cur:
        cur.execute(insert, flat_params)
    return len(rows)


def run_validations(conn, loaded_counts: dict, source_counts: dict):
    print("\nValidation:")
    for table, src_count in source_counts.items():
        with conn.cursor() as cur:
            cur.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(table)))
            db_count = cur.fetchone()[0]
        print(f"  {table:<45} source={src_count:>7} db={db_count:>7}")

    checks = [
        (
            "orphan_sales_order_items",
            """
            SELECT COUNT(*)
            FROM sales_order_items i
            LEFT JOIN sales_order_headers h ON h.sales_order = i.sales_order
            WHERE h.sales_order IS NULL
            """,
        ),
        (
            "orphan_billing_document_items",
            """
            SELECT COUNT(*)
            FROM billing_document_items i
            LEFT JOIN billing_document_headers h ON h.billing_document = i.billing_document
            WHERE h.billing_document IS NULL
            """,
        ),
    ]
    with conn.cursor() as cur:
        for label, q in checks:
            cur.execute(q)
            print(f"  {label:<45} {cur.fetchone()[0]}")


def main():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is required")
    tables = discover_folder_tables()
    failed_records = []
    loaded_counts = {}
    source_counts = {}

    with psycopg.connect(DATABASE_URL, prepare_threshold=None) as conn:
        conn.autocommit = False
        print("Discovered folder->table mapping:")
        for t in tables:
            print(f"  {t} -> {t}")

        for table in tables:
            if not table_exists(conn, table):
                print(f"SKIP table missing in DB: {table}")
                continue

            # Read once to discover dynamic columns and count source records.
            records = list(load_jsonl(table))
            src_objs = [r[2] for r in records if "__corrupt__" not in r[2]]
            source_counts[table] = len(src_objs)
            corrupt = [r for r in records if "__corrupt__" in r[2]]
            failed_records.extend([(table, p, ln, o["__corrupt__"]) for p, ln, o in corrupt])

            added = ensure_columns(conn, table, src_objs[:5000])
            if added:
                print(f"  Added columns in {table}: {added}")

            table_cols = pg_columns(conn, table)
            pk_cols = pk_columns(conn, table)
            batch = []
            loaded = 0
            for path, ln, obj in records:
                if "__corrupt__" in obj:
                    continue
                batch.append(build_row(table_cols, obj))
                if len(batch) >= 1000:
                    try:
                        loaded += upsert_rows(conn, table, pk_cols, batch)
                        conn.commit()
                    except Exception as ex:
                        conn.rollback()
                        for single in batch:
                            try:
                                upsert_rows(conn, table, pk_cols, [single])
                                conn.commit()
                                loaded += 1
                            except Exception as rex:
                                conn.rollback()
                                failed_records.append((table, path, ln, str(rex)))
                    batch.clear()
            if batch:
                try:
                    loaded += upsert_rows(conn, table, pk_cols, batch)
                    conn.commit()
                except Exception as ex:
                    conn.rollback()
                    for single in batch:
                        try:
                            upsert_rows(conn, table, pk_cols, [single])
                            conn.commit()
                            loaded += 1
                        except Exception as rex:
                            conn.rollback()
                            failed_records.append((table, "", -1, str(rex)))
            loaded_counts[table] = loaded
            print(f"  LOADED {table:<43} {loaded:>7} rows")

        run_validations(conn, loaded_counts, source_counts)

    if failed_records:
        log_path = Path(__file__).resolve().parent / "migration_errors.log"
        with open(log_path, "w", encoding="utf-8") as f:
            for t, p, ln, e in failed_records:
                f.write(f"{t}\t{p}\t{ln}\t{e}\n")
        print(f"\nCompleted with {len(failed_records)} failed records. See: {log_path}")
    else:
        print("\nCompleted with zero failed records.")


if __name__ == "__main__":
    main()
