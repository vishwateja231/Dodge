import os
import time
from textwrap import indent

import psycopg

DB = os.getenv("DATABASE_URL", "")


BASELINE_QUERIES = {
    "q_orders_by_customer_date": """
        EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
        SELECT so.sales_order, so.creation_date, so.total_net_amount
        FROM sales_order_headers so
        WHERE so.sold_to_party = '310000108'
        ORDER BY so.creation_date DESC
        LIMIT 50
    """,
    "q_delivery_by_order": """
        EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
        SELECT odh.delivery_document, odh.creation_date, odh.overall_goods_movement_status
        FROM outbound_delivery_headers odh
        JOIN outbound_delivery_items odi
          ON odi.delivery_document = odh.delivery_document
        WHERE odi.reference_sd_document = '740506'
        ORDER BY odh.creation_date DESC
        LIMIT 50
    """,
    "q_billing_by_order": """
        EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
        SELECT bdh.billing_document, bdh.billing_document_date, bdh.total_net_amount
        FROM billing_document_headers bdh
        JOIN billing_document_items bdi
          ON bdi.billing_document = bdh.billing_document
        WHERE bdi.reference_sd_document = '740506'
        ORDER BY bdh.billing_document_date DESC
        LIMIT 50
    """,
    "q_payments_by_customer_date": """
        EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
        SELECT accounting_document, posting_date, amount_in_transaction_currency
        FROM payments_accounts_receivable
        WHERE customer = '310000108'
        ORDER BY posting_date DESC
        LIMIT 50
    """,
}


OPT_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_soh_sold_to_party_creation_date ON sales_order_headers (sold_to_party, creation_date DESC)",
    "CREATE INDEX IF NOT EXISTS idx_soh_creation_date ON sales_order_headers (creation_date)",
    "CREATE INDEX IF NOT EXISTS idx_soh_overall_delivery_status ON sales_order_headers (overall_delivery_status)",
    "CREATE INDEX IF NOT EXISTS idx_soi_sales_order ON sales_order_items (sales_order)",
    "CREATE INDEX IF NOT EXISTS idx_soi_material ON sales_order_items (material)",
    "CREATE INDEX IF NOT EXISTS idx_odi_reference_sd_document ON outbound_delivery_items (reference_sd_document)",
    "CREATE INDEX IF NOT EXISTS idx_odi_delivery_document ON outbound_delivery_items (delivery_document)",
    "CREATE INDEX IF NOT EXISTS idx_odh_creation_date ON outbound_delivery_headers (creation_date)",
    "CREATE INDEX IF NOT EXISTS idx_odh_overall_goods_movement_status ON outbound_delivery_headers (overall_goods_movement_status)",
    "CREATE INDEX IF NOT EXISTS idx_bdi_reference_sd_document ON billing_document_items (reference_sd_document)",
    "CREATE INDEX IF NOT EXISTS idx_bdi_billing_document ON billing_document_items (billing_document)",
    "CREATE INDEX IF NOT EXISTS idx_bdh_billing_document_date ON billing_document_headers (billing_document_date)",
    "CREATE INDEX IF NOT EXISTS idx_bdh_sold_to_party ON billing_document_headers (sold_to_party)",
    "CREATE INDEX IF NOT EXISTS idx_par_customer_posting_date ON payments_accounts_receivable (customer, posting_date DESC)",
    "CREATE INDEX IF NOT EXISTS idx_par_invoice_reference ON payments_accounts_receivable (invoice_reference)",
    "CREATE INDEX IF NOT EXISTS idx_jeiar_customer_posting_date ON journal_entry_items_accounts_receivable (customer, posting_date DESC)",
    "CREATE INDEX IF NOT EXISTS idx_pp_product_plant ON product_plants (product, plant)",
    "CREATE INDEX IF NOT EXISTS idx_psl_product_plant_storage ON product_storage_locations (product, plant, storage_location)",
]


def run_explain(cur, label, sql):
    t0 = time.perf_counter()
    cur.execute(sql)
    rows = [r[0] for r in cur.fetchall()]
    elapsed_ms = (time.perf_counter() - t0) * 1000
    return label, elapsed_ms, rows


def get_fk_candidates(cur):
    cur.execute(
        """
        SELECT
          tc.table_name,
          kcu.column_name,
          ccu.table_name AS foreign_table_name,
          ccu.column_name AS foreign_column_name
        FROM information_schema.table_constraints AS tc
        JOIN information_schema.key_column_usage AS kcu
          ON tc.constraint_name = kcu.constraint_name
         AND tc.table_schema = kcu.table_schema
        JOIN information_schema.constraint_column_usage AS ccu
          ON ccu.constraint_name = tc.constraint_name
         AND ccu.table_schema = tc.table_schema
        WHERE tc.constraint_type = 'FOREIGN KEY'
          AND tc.table_schema='public'
        ORDER BY tc.table_name, kcu.column_name
        """
    )
    return cur.fetchall()


def main():
    if not DB:
        raise RuntimeError("DATABASE_URL is required")

    with psycopg.connect(DB, prepare_threshold=None) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            print("=== Existing Foreign Keys ===")
            for r in get_fk_candidates(cur):
                print(f"{r[0]}.{r[1]} -> {r[2]}.{r[3]}")

            print("\n=== Baseline EXPLAIN ANALYZE ===")
            baseline = {}
            for label, q in BASELINE_QUERIES.items():
                label, ms, plan = run_explain(cur, label, q)
                baseline[label] = {"elapsed_ms": ms, "plan": plan}
                print(f"\n[{label}] capture_ms={ms:.2f}")
                print(indent("\n".join(plan[:12]), "  "))

            print("\n=== Creating Performance Indexes ===")
            for stmt in OPT_INDEXES:
                cur.execute(stmt)
                print("OK:", stmt)

            print("\n=== Running ANALYZE on key tables ===")
            for t in [
                "sales_order_headers",
                "sales_order_items",
                "outbound_delivery_headers",
                "outbound_delivery_items",
                "billing_document_headers",
                "billing_document_items",
                "payments_accounts_receivable",
                "journal_entry_items_accounts_receivable",
                "product_plants",
                "product_storage_locations",
            ]:
                cur.execute(f"ANALYZE {t}")
                print("ANALYZE OK:", t)

            print("\n=== Post-Index EXPLAIN ANALYZE ===")
            for label, q in BASELINE_QUERIES.items():
                label2, ms2, plan2 = run_explain(cur, label, q)
                ms1 = baseline[label]["elapsed_ms"]
                delta = ms1 - ms2
                pct = (delta / ms1 * 100) if ms1 else 0.0
                print(f"\n[{label2}] before_ms={ms1:.2f} after_ms={ms2:.2f} improvement_ms={delta:.2f} improvement_pct={pct:.2f}")
                print(indent("\n".join(plan2[:12]), "  "))

            print("\n=== Partitioning Recommendation ===")
            print("Recommend RANGE partitioning by creation_date / posting_date if tables grow >10M rows:")
            print("- sales_order_headers by creation_date")
            print("- outbound_delivery_headers by creation_date")
            print("- billing_document_headers by billing_document_date")
            print("- payments_accounts_receivable by posting_date")


if __name__ == "__main__":
    main()
