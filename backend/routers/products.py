from fastapi import APIRouter, Query
from database import query as db_query

router = APIRouter()


@router.get("/")
def list_products(
    product_type: str = Query(None),
    limit: int = Query(50),
    offset: int = Query(0),
):
    if product_type:
        sql = "SELECT * FROM products WHERE product_type = ? AND is_deleted = 0 LIMIT ? OFFSET ?"
        return db_query(sql, [product_type, limit, offset])
    sql = "SELECT * FROM products WHERE is_deleted = 0 LIMIT ? OFFSET ?"
    return db_query(sql, [limit, offset])


@router.get("/{product_id}")
def get_product(product_id: str):
    rows = db_query("SELECT * FROM products WHERE product_id = ?", [product_id])
    return rows[0] if rows else {"error": "Not found"}
