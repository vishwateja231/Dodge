import glob
import json
import os
from datetime import datetime
from decimal import Decimal

import psycopg
from psycopg import sql
from psycopg.types.json import Json

DB = os.getenv("DATABASE_URL", "")
ROOT = r"C:/Users/vishw/Downloads/Dodge Ai Assignment/data/sap-o2c-data"
TABLES = [
    "outbound_delivery_items",
    "billing_document_headers",
    "billing_document_items",
    "billing_document_cancellations",
]


def to_snake(name: str) -> str:
    out = []
    for i, ch in enumerate(name):
        if ch.isupper() and i > 0 and (not name[i - 1].isupper()):
            out.append("_")
        out.append(ch.lower())
    return "".join(out)


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
        try:
            return datetime.fromisoformat(v.replace("Z", "+00:00"))
        except Exception:
            return None
    return None


def parse_num(v):
    if v in (None, ""):
        return None
    try:
        return Decimal(str(v))
    except Exception:
        return None


def parse_time(v):
    if isinstance(v, dict):
        try:
            return f"{int(v.get('hours', 0)):02d}:{int(v.get('minutes', 0)):02d}:{int(v.get('seconds', 0)):02d}"
        except Exception:
            return None
    return v


def main():
    if not DB:
        raise RuntimeError("DATABASE_URL is required")
    with psycopg.connect(DB, prepare_threshold=None) as conn:
        for table in TABLES:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT column_name, data_type, udt_name
                    FROM information_schema.columns
                    WHERE table_schema='public' AND table_name=%s
                    ORDER BY ordinal_position
                    """,
                    [table],
                )
                cols = cur.fetchall()
                meta = {c: {"dt": dt, "udt": udt} for c, dt, udt in cols}
                cur.execute(
                    """
                    SELECT kcu.column_name
                    FROM information_schema.table_constraints tc
                    JOIN information_schema.key_column_usage kcu
                      ON tc.constraint_name=kcu.constraint_name
                     AND tc.table_schema=kcu.table_schema
                    WHERE tc.table_schema='public'
                      AND tc.table_name=%s
                      AND tc.constraint_type='PRIMARY KEY'
                    ORDER BY kcu.ordinal_position
                    """,
                    [table],
                )
                pk = [r[0] for r in cur.fetchall()]

            loaded = 0
            for path in sorted(glob.glob(os.path.join(ROOT, table, "*.jsonl"))):
                with open(path, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        obj = json.loads(line)
                        sobj = {to_snake(k): v for k, v in obj.items()}
                        col_names = list(meta.keys())
                        row = []
                        for c in col_names:
                            m = meta[c]
                            v = sobj.get(c)
                            if m["dt"] in ("json", "jsonb"):
                                v = Json(v) if v is not None else None
                            elif m["dt"] == "boolean":
                                if v is None:
                                    v = None
                                elif isinstance(v, bool):
                                    pass
                                else:
                                    v = str(v).lower() == "true"
                            elif m["dt"] in ("numeric", "double precision", "real", "decimal"):
                                v = parse_num(v)
                            elif m["dt"] == "date":
                                v = parse_date(v)
                            elif "timestamp" in m["dt"]:
                                v = parse_ts(v)
                            elif m["dt"] == "time without time zone":
                                v = parse_time(v)
                            row.append(v)

                        q = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                            sql.Identifier(table),
                            sql.SQL(",").join(sql.Identifier(c) for c in col_names),
                            sql.SQL(",").join(sql.Placeholder() for _ in col_names),
                        )
                        if pk:
                            non_pk = [c for c in col_names if c not in set(pk)]
                            if non_pk:
                                q += sql.SQL(" ON CONFLICT ({}) DO UPDATE SET {}").format(
                                    sql.SQL(",").join(sql.Identifier(c) for c in pk),
                                    sql.SQL(",").join(
                                        sql.SQL("{}=EXCLUDED.{}").format(sql.Identifier(c), sql.Identifier(c))
                                        for c in non_pk
                                    ),
                                )
                            else:
                                q += sql.SQL(" ON CONFLICT ({}) DO NOTHING").format(
                                    sql.SQL(",").join(sql.Identifier(c) for c in pk)
                                )

                        with conn.cursor() as cur:
                            cur.execute(q, row)
                        loaded += 1
            conn.commit()
            print(table, loaded)


if __name__ == "__main__":
    main()
