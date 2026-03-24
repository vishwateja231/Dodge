import glob
import os

import psycopg

DB = os.getenv("DATABASE_URL", "")
ROOT = r"C:/Users/vishw/Downloads/Dodge Ai Assignment/data/sap-o2c-data"


def main():
    if not DB:
        raise RuntimeError("DATABASE_URL is required")
    with psycopg.connect(DB, prepare_threshold=None) as conn:
        with conn.cursor() as cur:
            print("Row count validation:")
            for folder in sorted([x for x in os.listdir(ROOT) if os.path.isdir(os.path.join(ROOT, x))]):
                src = 0
                for fp in glob.glob(os.path.join(ROOT, folder, "*.jsonl")):
                    with open(fp, "r", encoding="utf-8") as f:
                        src += sum(1 for ln in f if ln.strip())
                cur.execute(f"SELECT COUNT(*) FROM {folder}")
                db = cur.fetchone()[0]
                status = "OK" if src == db else "MISMATCH"
                print(f"{folder}: source={src} db={db} status={status}")

            print("\nRelationship checks:")
            checks = [
                (
                    "orphan_sales_order_items",
                    "SELECT COUNT(*) FROM sales_order_items i LEFT JOIN sales_order_headers h ON h.sales_order=i.sales_order WHERE h.sales_order IS NULL",
                ),
                (
                    "orphan_schedule_lines",
                    "SELECT COUNT(*) FROM sales_order_schedule_lines s LEFT JOIN sales_order_items i ON i.sales_order=s.sales_order AND i.sales_order_item=s.sales_order_item WHERE i.sales_order IS NULL",
                ),
                (
                    "orphan_delivery_items",
                    "SELECT COUNT(*) FROM outbound_delivery_items i LEFT JOIN outbound_delivery_headers h ON h.delivery_document=i.delivery_document WHERE h.delivery_document IS NULL",
                ),
                (
                    "orphan_billing_items",
                    "SELECT COUNT(*) FROM billing_document_items i LEFT JOIN billing_document_headers h ON h.billing_document=i.billing_document WHERE h.billing_document IS NULL",
                ),
            ]
            for name, query in checks:
                cur.execute(query)
                print(f"{name}: {cur.fetchone()[0]}")

            print("\nSample join rows:")
            cur.execute(
                """
                SELECT so.sales_order, so.sold_to_party, odi.delivery_document, bdi.billing_document
                FROM sales_order_headers so
                LEFT JOIN outbound_delivery_items odi ON odi.reference_sd_document=so.sales_order
                LEFT JOIN billing_document_items bdi ON bdi.reference_sd_document=so.sales_order
                LIMIT 5
                """
            )
            for row in cur.fetchall():
                print(row)


if __name__ == "__main__":
    main()
