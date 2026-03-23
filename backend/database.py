"""
database.py — SQLite connection helper
"""
import sqlite3
from config import DB_PATH


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row   # rows behave like dicts
    return conn


def query(sql: str, params: list = None) -> list[dict]:
    conn = get_conn()
    try:
        rows = conn.execute(sql, params or []).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()
