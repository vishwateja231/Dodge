import os

import psycopg

DB = os.getenv("DATABASE_URL", "")

SQL = [
    "ALTER TABLE outbound_delivery_items DROP CONSTRAINT IF EXISTS outbound_delivery_items_reference_sd_document_reference_sd_fkey",
    "ALTER TABLE billing_document_items DROP CONSTRAINT IF EXISTS billing_document_items_reference_sd_document_reference_sd_docum_fkey",
    "ALTER TABLE payments_accounts_receivable DROP CONSTRAINT IF EXISTS payments_accounts_receivable_sales_document_sales_document_item_fkey",
    "ALTER TABLE payments_accounts_receivable DROP CONSTRAINT IF EXISTS payments_accounts_receivable_invoice_reference_fkey",
    "ALTER TABLE billing_document_headers DROP CONSTRAINT IF EXISTS billing_document_headers_cancelled_billing_document_fkey",
    """
    DO $$
    DECLARE c RECORD;
    BEGIN
      FOR c IN
        SELECT conname
        FROM pg_constraint
        WHERE conrelid = 'billing_document_items'::regclass
          AND contype = 'f'
      LOOP
        EXECUTE format('ALTER TABLE billing_document_items DROP CONSTRAINT IF EXISTS %I', c.conname);
      END LOOP;
    END $$;
    """,
    "ALTER TABLE billing_document_cancellations DROP CONSTRAINT IF EXISTS billing_document_cancellations_cancelled_billing_document_fkey",
]


def main():
    if not DB:
        raise RuntimeError("DATABASE_URL is required")
    with psycopg.connect(DB, prepare_threshold=None) as conn:
        with conn.cursor() as cur:
            for q in SQL:
                cur.execute(q)
        conn.commit()
    print("Dropped problematic FK constraints (if present).")


if __name__ == "__main__":
    main()
