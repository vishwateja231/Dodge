import sqlite3
from config import DB_PATH

DDL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS customers (
    customer_id   TEXT PRIMARY KEY,
    name          TEXT,
    grouping      TEXT,
    is_blocked    INTEGER DEFAULT 0,
    is_archived   INTEGER DEFAULT 0,
    created_date  TEXT
);

CREATE TABLE IF NOT EXISTS products (
    product_id    TEXT PRIMARY KEY,
    product_name  TEXT,
    product_type  TEXT,
    product_group TEXT,
    old_sku       TEXT,
    weight_kg     REAL,
    base_unit     TEXT,
    division      TEXT,
    is_deleted    INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS orders (
    order_id                  TEXT PRIMARY KEY,
    customer_id               TEXT,
    order_type                TEXT,
    sales_org                 TEXT,
    order_date                TEXT,
    requested_delivery_date   TEXT,
    total_amount              REAL,
    currency                  TEXT DEFAULT 'INR',
    delivery_status           TEXT,
    process_status            TEXT
);

CREATE TABLE IF NOT EXISTS order_items (
    order_id        TEXT NOT NULL,
    line_no         TEXT NOT NULL,
    product_id      TEXT,
    quantity        REAL,
    unit            TEXT,
    net_amount      REAL,
    plant           TEXT,
    item_category   TEXT,
    delivery_status TEXT,
    PRIMARY KEY (order_id, line_no)
);

CREATE TABLE IF NOT EXISTS deliveries (
    delivery_id     TEXT PRIMARY KEY,
    order_id        TEXT,
    created_date    TEXT,
    ship_date       TEXT,
    picking_status  TEXT,
    goods_status    TEXT,
    shipping_point  TEXT,
    delivery_block  TEXT
);

CREATE TABLE IF NOT EXISTS invoices (
    invoice_id      TEXT PRIMARY KEY,
    order_id        TEXT,
    customer_id     TEXT,
    invoice_type    TEXT,
    invoice_date    TEXT,
    total_amount    REAL,
    currency        TEXT DEFAULT 'INR',
    accounting_doc  TEXT,
    is_cancelled    INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS payments (
    payment_id      TEXT,
    payment_item    TEXT,
    customer_id     TEXT,
    clearing_date   TEXT,
    posting_date    TEXT,
    amount          REAL,
    currency        TEXT DEFAULT 'INR',
    clearing_doc    TEXT,
    gl_account      TEXT,
    is_incoming     INTEGER,
    PRIMARY KEY (payment_id, payment_item)
);
"""


def setup_db():
    conn = sqlite3.connect(DB_PATH)
    conn.executescript(DDL)
    conn.commit()
    conn.close()
    print(f"✓ Schema created: {DB_PATH}")


if __name__ == "__main__":
    setup_db()
