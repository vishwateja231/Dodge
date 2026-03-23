"""
load_data.py
────────────
Main ETL pipeline: reads JSONL files → transforms → loads into SQLite.
Run: python load_data.py
"""
import sqlite3
import pandas as pd
from config import DB_PATH
from db_setup import setup_db
from transform import (
    load_jsonl_folder,
    build_customers,
    build_products,
    build_orders,
    build_order_items,
    build_deliveries,
    build_invoices,
    build_payments,
)


def write_table(df: pd.DataFrame, table: str, conn: sqlite3.Connection) -> None:
    if df.empty:
        print(f"  ⚠ Skipped {table}: empty DataFrame")
        return
    df.to_sql(table, conn, if_exists="replace", index=False)
    print(f"  ✓ {table:<20} {len(df):>6} rows")


def main():
    print("=" * 50)
    print("SAP O2C → SQLite Pipeline")
    print("=" * 50)

    # 1. Create schema
    setup_db()
    conn = sqlite3.connect(DB_PATH)

    print("\nLoading tables...")

    # 2. Customers
    write_table(
        build_customers(load_jsonl_folder("business_partners")),
        "customers", conn
    )

    # 3. Products (needs descriptions joined)
    write_table(
        build_products(
            load_jsonl_folder("products"),
            load_jsonl_folder("product_descriptions"),
        ),
        "products", conn
    )

    # 4. Orders
    write_table(
        build_orders(load_jsonl_folder("sales_order_headers")),
        "orders", conn
    )

    # 5. Order Items
    write_table(
        build_order_items(load_jsonl_folder("sales_order_items")),
        "order_items", conn
    )

    # 6. Deliveries (needs items for order_id bridge)
    write_table(
        build_deliveries(
            load_jsonl_folder("outbound_delivery_headers"),
            load_jsonl_folder("outbound_delivery_items"),
        ),
        "deliveries", conn
    )

    # 7. Invoices (needs items for order_id bridge)
    write_table(
        build_invoices(
            load_jsonl_folder("billing_document_headers"),
            load_jsonl_folder("billing_document_items"),
        ),
        "invoices", conn
    )

    # 8. Payments
    write_table(
        build_payments(load_jsonl_folder("payments_accounts_receivable")),
        "payments", conn
    )

    conn.close()

    print("\n" + "=" * 50)
    print(f"✓ Done! Database: {DB_PATH}")
    print("=" * 50)

    # Quick sanity check
    print("\nRow counts:")
    conn = sqlite3.connect(DB_PATH)
    for table in ["customers", "products", "orders", "order_items",
                  "deliveries", "invoices", "payments"]:
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"  {table:<20} {count:>6}")
    conn.close()


if __name__ == "__main__":
    main()
