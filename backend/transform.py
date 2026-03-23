"""
transform.py
────────────
Loads raw JSONL data from the sap-o2c-data folders and transforms
each dataset into a clean pandas DataFrame ready for SQLite insertion.
"""
import os
import glob
import json
import pandas as pd
from config import BASE_DIR


# ── Utilities ─────────────────────────────────────────────────────────────────

def load_jsonl_folder(folder_name: str) -> pd.DataFrame:
    """Load all *.jsonl files in a folder into one DataFrame."""
    pattern = os.path.join(BASE_DIR, folder_name, "*.jsonl")
    files = glob.glob(pattern)
    if not files:
        print(f"  ⚠ No JSONL files found in: {folder_name}")
        return pd.DataFrame()

    frames = []
    for f in files:
        with open(f, encoding="utf-8") as fh:
            lines = [json.loads(line) for line in fh if line.strip()]
        if lines:
            frames.append(pd.DataFrame(lines))
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _flatten_time_col(df: pd.DataFrame, col: str) -> pd.DataFrame:
    """Convert nested time dicts {'hours':6,'minutes':49,'seconds':13} → 'HH:MM:SS'."""
    if col in df.columns:
        df[col] = df[col].apply(
            lambda x: (
                f"{x['hours']:02d}:{x['minutes']:02d}:{x['seconds']:02d}"
                if isinstance(x, dict) else None
            )
        )
    return df


def _safe_bool(series: pd.Series) -> pd.Series:
    """Convert True/False/None/string booleans → int 0/1."""
    return series.map(lambda v: 1 if v is True or v == "true" else 0)


def _keep(df: pd.DataFrame, cols: list) -> pd.DataFrame:
    """Return only the columns that actually exist in df."""
    return df[[c for c in cols if c in df.columns]]


# ── Table Builders ────────────────────────────────────────────────────────────

def build_customers(raw: pd.DataFrame) -> pd.DataFrame:
    df = raw.rename(columns={
        "businessPartner":          "customer_id",
        "businessPartnerFullName":  "name",
        "businessPartnerGrouping":  "grouping",
        "businessPartnerIsBlocked": "is_blocked",
        "isMarkedForArchiving":     "is_archived",
        "creationDate":             "created_date",
    })
    df["is_blocked"]  = _safe_bool(df.get("is_blocked",  pd.Series(dtype=object)))
    df["is_archived"] = _safe_bool(df.get("is_archived", pd.Series(dtype=object)))
    return _keep(df, ["customer_id", "name", "grouping",
                       "is_blocked", "is_archived", "created_date"])


def build_products(raw_products: pd.DataFrame,
                   raw_descriptions: pd.DataFrame) -> pd.DataFrame:
    # Join English descriptions
    if not raw_descriptions.empty and "language" in raw_descriptions.columns:
        desc = (raw_descriptions[raw_descriptions["language"] == "EN"]
                .rename(columns={"product": "product_id",
                                 "productDescription": "product_name"})
                [["product_id", "product_name"]])
    else:
        desc = pd.DataFrame(columns=["product_id", "product_name"])

    df = raw_products.rename(columns={
        "product":             "product_id",
        "productType":         "product_type",
        "productGroup":        "product_group",
        "productOldId":        "old_sku",
        "netWeight":           "weight_kg",
        "baseUnit":            "base_unit",
        "division":            "division",
        "isMarkedForDeletion": "is_deleted",
    })
    df["is_deleted"] = _safe_bool(df.get("is_deleted", pd.Series(dtype=object)))
    df = df.merge(desc, on="product_id", how="left")
    return _keep(df, ["product_id", "product_name", "product_type",
                       "product_group", "old_sku", "weight_kg",
                       "base_unit", "division", "is_deleted"])


def build_orders(raw: pd.DataFrame) -> pd.DataFrame:
    df = raw.rename(columns={
        "salesOrder":             "order_id",
        "soldToParty":            "customer_id",
        "salesOrderType":         "order_type",
        "salesOrganization":      "sales_org",
        "salesOrderDate":         "order_date",
        "creationDate":           "order_date" if "salesOrderDate" not in raw.columns else "creation_date_raw",
        "requestedDeliveryDate":  "requested_delivery_date",
        "totalNetOrderAmount":    "total_amount",
        "transactionCurrency":    "currency",
        "overallDeliveryStatus":  "delivery_status",
        "overallSDProcessStatus": "process_status",
    })
    # If salesOrderDate was missing/all-null, fall back to creationDate
    if "order_date" not in df.columns or df["order_date"].isna().all():
        if "creation_date_raw" in df.columns:
            df["order_date"] = df["creation_date_raw"]
        elif "creationDate" in raw.columns:
            df["order_date"] = raw["creationDate"]
    df["total_amount"] = pd.to_numeric(df.get("total_amount"), errors="coerce")
    cols = ["order_id", "customer_id", "order_type", "sales_org", "order_date",
            "requested_delivery_date", "total_amount", "currency",
            "delivery_status", "process_status"]
    return _keep(df, cols)


def build_order_items(raw: pd.DataFrame) -> pd.DataFrame:
    df = raw.rename(columns={
        "salesOrder":             "order_id",
        "salesOrderItem":         "line_no",
        "material":               "product_id",
        "requestedQuantity":      "quantity",
        "requestedQuantityUnit":  "unit",
        "netAmount":              "net_amount",
        "plant":                  "plant",
        "salesOrderItemCategory": "item_category",
        "itemDeliveryStatus":     "delivery_status",
    })
    df["quantity"]   = pd.to_numeric(df.get("quantity"),   errors="coerce")
    df["net_amount"] = pd.to_numeric(df.get("net_amount"), errors="coerce")
    return _keep(df, ["order_id", "line_no", "product_id", "quantity",
                       "unit", "net_amount", "plant",
                       "item_category", "delivery_status"])


def build_deliveries(raw_headers: pd.DataFrame,
                     raw_items: pd.DataFrame) -> pd.DataFrame:
    # Derive order_id from delivery items
    if not raw_items.empty and "referenceSdDocument" in raw_items.columns:
        bridge = (raw_items[["deliveryDocument", "referenceSdDocument"]]
                  .drop_duplicates(subset=["deliveryDocument"])
                  .rename(columns={"deliveryDocument":    "delivery_id",
                                   "referenceSdDocument": "order_id"}))
    else:
        bridge = pd.DataFrame(columns=["delivery_id", "order_id"])

    df = _flatten_time_col(raw_headers.copy(), "actualGoodsMovementTime")
    df = _flatten_time_col(df, "creationTime")
    df = df.rename(columns={
        "deliveryDocument":           "delivery_id",
        "creationDate":               "created_date",
        "actualGoodsMovementDate":    "ship_date",
        "overallPickingStatus":       "picking_status",
        "overallGoodsMovementStatus": "goods_status",
        "shippingPoint":              "shipping_point",
        "deliveryBlockReason":        "delivery_block",
    })
    df = df.merge(bridge, on="delivery_id", how="left")
    return _keep(df, ["delivery_id", "order_id", "created_date", "ship_date",
                       "picking_status", "goods_status",
                       "shipping_point", "delivery_block"])


def build_invoices(raw_headers: pd.DataFrame,
                   raw_items: pd.DataFrame) -> pd.DataFrame:
    # One order_id per billing document (first item's reference)
    if not raw_items.empty and "referenceSdDocument" in raw_items.columns:
        bridge = (raw_items[["billingDocument", "referenceSdDocument"]]
                  .drop_duplicates(subset=["billingDocument"])
                  .rename(columns={"billingDocument":    "invoice_id",
                                   "referenceSdDocument": "order_id"}))
    else:
        bridge = pd.DataFrame(columns=["invoice_id", "order_id"])

    df = raw_headers.rename(columns={
        "billingDocument":           "invoice_id",
        "billingDocumentType":       "invoice_type",
        "soldToParty":               "customer_id",
        "billingDocumentDate":       "invoice_date",
        "netAmount":                 "total_amount",
        "transactionCurrency":       "currency",
        "accountingDocument":        "accounting_doc",
        "billingDocumentIsCancelled": "is_cancelled",
    })
    df["total_amount"] = pd.to_numeric(df.get("total_amount"), errors="coerce")
    df["is_cancelled"] = _safe_bool(df.get("is_cancelled", pd.Series(dtype=object)))
    df = df.merge(bridge, on="invoice_id", how="left")
    return _keep(df, ["invoice_id", "order_id", "customer_id", "invoice_type",
                       "invoice_date", "total_amount", "currency",
                       "accounting_doc", "is_cancelled"])


def build_payments(raw: pd.DataFrame) -> pd.DataFrame:
    df = raw.rename(columns={
        "accountingDocument":          "payment_id",
        "accountingDocumentItem":      "payment_item",
        "customer":                    "customer_id",
        "clearingDate":                "clearing_date",
        "postingDate":                 "posting_date",
        "amountInTransactionCurrency": "amount",
        "transactionCurrency":         "currency",
        "clearingAccountingDocument":  "clearing_doc",
        "glAccount":                   "gl_account",
    })
    df["amount"]      = pd.to_numeric(df.get("amount"), errors="coerce")
    df["is_incoming"] = (df["amount"] > 0).astype(int)
    return _keep(df, ["payment_id", "payment_item", "customer_id",
                       "clearing_date", "posting_date", "amount", "currency",
                       "clearing_doc", "gl_account", "is_incoming"])
