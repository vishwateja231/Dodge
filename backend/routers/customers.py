from fastapi import APIRouter, Query
from database import query as db_query

router = APIRouter()


@router.get("/")
def list_customers(
    blocked: bool = Query(None, description="Filter by blocked status"),
    limit: int = Query(50),
    offset: int = Query(0),
):
    if blocked is None:
        sql = "SELECT * FROM customers LIMIT ? OFFSET ?"
        return db_query(sql, [limit, offset])
    sql = "SELECT * FROM customers WHERE is_blocked = ? LIMIT ? OFFSET ?"
    return db_query(sql, [int(blocked), limit, offset])


@router.get("/{customer_id}")
def get_customer(customer_id: str):
    sql = "SELECT * FROM customers WHERE customer_id = ?"
    rows = db_query(sql, [customer_id])
    return rows[0] if rows else {"error": "Not found"}


@router.get("/{customer_id}/orders")
def customer_orders(customer_id: str):
    sql = """
    SELECT o.*, d.delivery_id, d.goods_status, d.ship_date,
           i.invoice_id, i.total_amount AS invoice_amount, i.is_cancelled
    FROM orders o
    LEFT JOIN deliveries d ON d.order_id   = o.order_id
    LEFT JOIN invoices   i ON i.order_id   = o.order_id
    WHERE o.customer_id = ?
    ORDER BY o.order_date DESC
    """
    return db_query(sql, [customer_id])


@router.get("/{customer_id}/summary")
def customer_summary(customer_id: str):
    sql = """
    SELECT
        c.customer_id, c.name, c.is_blocked,
        COUNT(DISTINCT o.order_id)                                   AS total_orders,
        COALESCE(SUM(i.total_amount), 0)                             AS total_invoiced,
        COALESCE(SUM(CASE WHEN p.is_incoming=1 THEN p.amount END),0) AS total_paid
    FROM customers c
    LEFT JOIN orders   o ON o.customer_id = c.customer_id
    LEFT JOIN invoices i ON i.customer_id = c.customer_id AND i.is_cancelled = 0
    LEFT JOIN payments p ON p.customer_id = c.customer_id
    WHERE c.customer_id = ?
    GROUP BY c.customer_id, c.name, c.is_blocked
    """
    rows = db_query(sql, [customer_id])
    return rows[0] if rows else {"error": "Not found"}
