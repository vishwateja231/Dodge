from fastapi import APIRouter, Query
from database import query as db_query

router = APIRouter()


@router.get("/top-products")
def top_products(limit: int = Query(10, le=50)):
    """Top N products by billed revenue (excluding cancelled invoices)."""
    sql = """
    SELECT
        oi.product_id,
        pr.product_name,
        pr.product_type,
        pr.old_sku,
        COUNT(DISTINCT oi.order_id)  AS total_orders,
        SUM(oi.net_amount)           AS total_revenue,
        SUM(oi.quantity)             AS total_units_sold
    FROM order_items oi
    JOIN products pr ON pr.product_id = oi.product_id
    JOIN invoices  i  ON i.order_id   = oi.order_id AND i.is_cancelled = 0
    GROUP BY oi.product_id, pr.product_name, pr.product_type, pr.old_sku
    ORDER BY total_revenue DESC
    LIMIT ?
    """
    return db_query(sql, [limit])


@router.get("/customer-summary")
def all_customer_summary():
    """Order, invoiced, and paid amounts for every customer."""
    sql = """
    SELECT
        c.customer_id,
        c.name,
        c.is_blocked,
        COUNT(DISTINCT o.order_id)                                    AS total_orders,
        COALESCE(SUM(i.total_amount), 0)                              AS total_invoiced,
        COALESCE(SUM(CASE WHEN p.is_incoming=1 THEN p.amount END), 0) AS total_paid,
        COALESCE(SUM(i.total_amount), 0) -
        COALESCE(SUM(CASE WHEN p.is_incoming=1 THEN p.amount END), 0) AS outstanding
    FROM customers c
    LEFT JOIN orders   o ON o.customer_id = c.customer_id
    LEFT JOIN invoices i ON i.customer_id = c.customer_id AND i.is_cancelled = 0
    LEFT JOIN payments p ON p.customer_id = c.customer_id
    GROUP BY c.customer_id, c.name, c.is_blocked
    ORDER BY outstanding DESC
    """
    return db_query(sql)


@router.get("/delivery-performance")
def delivery_performance():
    """Average, min, max days from order date to goods issue."""
    sql = """
    SELECT
        COUNT(*)                                              AS shipped_orders,
        ROUND(AVG(JULIANDAY(d.ship_date) - JULIANDAY(o.order_date)), 1) AS avg_days,
        MIN(JULIANDAY(d.ship_date) - JULIANDAY(o.order_date))           AS min_days,
        MAX(JULIANDAY(d.ship_date) - JULIANDAY(o.order_date))           AS max_days
    FROM orders o
    JOIN deliveries d ON d.order_id = o.order_id AND d.goods_status = 'C'
    WHERE d.ship_date IS NOT NULL AND o.order_date IS NOT NULL
    """
    rows = db_query(sql)
    return rows[0] if rows else {}


@router.get("/revenue-leakage")
def revenue_leakage():
    """Orders that are delivered but have no invoice (unrecovered revenue)."""
    sql = """
    SELECT
        o.order_id,
        c.name AS customer_name,
        d.ship_date,
        o.total_amount AS order_amount
    FROM orders o
    JOIN  customers  c ON c.customer_id = o.customer_id
    JOIN  deliveries d ON d.order_id    = o.order_id AND d.goods_status = 'C'
    LEFT JOIN invoices i ON i.order_id  = o.order_id
    WHERE i.invoice_id IS NULL
    """
    return db_query(sql)


@router.get("/overdue-ar")
def overdue_ar():
    """Invoices that are not cancelled but have no incoming payment."""
    sql = """
    SELECT
        i.invoice_id,
        c.name   AS customer_name,
        c.is_blocked,
        i.invoice_date,
        i.total_amount
    FROM invoices i
    JOIN  customers c ON c.customer_id = i.customer_id
    LEFT JOIN payments p ON p.customer_id = i.customer_id
                        AND p.is_incoming = 1
    WHERE p.payment_id IS NULL AND i.is_cancelled = 0
    ORDER BY i.invoice_date
    """
    return db_query(sql)


@router.get("/pipeline-summary")
def pipeline_summary():
    """High-level counts for the entire O2C pipeline."""
    sql_counts = {
        "total_customers":         "SELECT COUNT(*) FROM customers",
        "blocked_customers":       "SELECT COUNT(*) FROM customers WHERE is_blocked = 1",
        "total_orders":            "SELECT COUNT(*) FROM orders",
        "orders_delivered":        "SELECT COUNT(DISTINCT order_id) FROM deliveries WHERE goods_status = 'C'",
        "orders_billed":           "SELECT COUNT(DISTINCT order_id) FROM invoices WHERE is_cancelled = 0",
        "total_invoices":          "SELECT COUNT(*) FROM invoices",
        "cancelled_invoices":      "SELECT COUNT(*) FROM invoices WHERE is_cancelled = 1",
        "total_payments_received": "SELECT COUNT(*) FROM payments WHERE is_incoming = 1",
        "total_products":          "SELECT COUNT(*) FROM products WHERE is_deleted = 0",
    }
    import sqlite3
    from config import DB_PATH
    conn = sqlite3.connect(DB_PATH)
    result = {}
    for key, sql in sql_counts.items():
        result[key] = conn.execute(sql).fetchone()[0]
    conn.close()
    return result
