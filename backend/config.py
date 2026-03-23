import os

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR = os.path.join(
    os.path.dirname(__file__), "..", "data", "sap-o2c-data"
)
DB_PATH = os.path.join(os.path.dirname(__file__), "sap_o2c.db")

# ── JSONL folder names ────────────────────────────────────────────────────────
FOLDERS = {
    "business_partners":                "business_partners",
    "products":                         "products",
    "product_descriptions":             "product_descriptions",
    "sales_order_headers":              "sales_order_headers",
    "sales_order_items":                "sales_order_items",
    "outbound_delivery_headers":        "outbound_delivery_headers",
    "outbound_delivery_items":          "outbound_delivery_items",
    "billing_document_headers":         "billing_document_headers",
    "billing_document_items":           "billing_document_items",
    "payments_accounts_receivable":     "payments_accounts_receivable",
}
