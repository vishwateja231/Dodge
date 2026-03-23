"""
query.py — Production-Grade Two-Stage LLM Pipeline
    Stage 1: Groq (fast) → Intent parsing + query normalization
    Stage 2: Gemini Flash → SQL generation (deterministic)
    Stage 3: Python formatter → clean, hallucination-free response

POST /query/
GET  /query/{query_id}  (pagination)
"""
import os
import sqlite3
import json
import re
import uuid
import logging
from fastapi import APIRouter
from pydantic import BaseModel
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

router = APIRouter()

# ── SQL cache for pagination (query_id → raw SQL string) ──────────────────────
sql_cache: dict[str, str] = {}

# ── Known DB tables and their key columns ─────────────────────────────────────
ALLOWED_TABLES = {
    "customers", "products", "orders", "order_items",
    "deliveries", "invoices", "payments"
}

DB_SCHEMA = """
TABLE customers    (customer_id, name, grouping, is_blocked, is_archived, created_date)
TABLE products     (product_id, product_name, product_type, product_group, old_sku, weight_kg, base_unit, division, is_deleted)
TABLE orders       (order_id, customer_id, order_type, sales_org, order_date, requested_delivery_date, total_amount, currency, delivery_status)
TABLE order_items  (order_id, line_no, product_id, quantity, unit, net_amount, plant, item_category)
TABLE deliveries   (delivery_id, order_id, created_date, ship_date, picking_status, goods_status, shipping_point, delivery_block)
TABLE invoices     (invoice_id, order_id, customer_id, invoice_type, invoice_date, total_amount, currency, accounting_doc, is_cancelled)
TABLE payments     (payment_id, payment_item, customer_id, clearing_date, posting_date, amount, currency, clearing_doc, gl_account, is_incoming)

CRITICAL JOIN RULES:
1. customers → orders:       orders.customer_id = customers.customer_id
2. orders → order_items:     order_items.order_id = orders.order_id
3. order_items → products:   products.product_id = order_items.product_id
4. orders → deliveries:      deliveries.order_id = orders.order_id
5. orders → invoices:        invoices.order_id = orders.order_id
6. invoices → payments:      payments.payment_item = invoices.invoice_id AND payments.is_incoming = 1

UNPAID ORDER DEFINITION (MANDATORY):
An order is UNPAID if: invoice exists AND (no payment OR payment.amount < invoice.total_amount)
Use this exact pattern when detecting unpaid orders:
  SELECT DISTINCT o.order_id, c.name
  FROM orders o
  JOIN invoices i ON o.order_id = i.order_id
  LEFT JOIN payments p ON p.payment_item = i.invoice_id AND p.is_incoming = 1
  JOIN customers c ON o.customer_id = c.customer_id
  WHERE i.is_cancelled = 0
    AND (p.payment_id IS NULL OR p.amount < i.total_amount)

CRITICAL ANTI-HALLUCINATION RULES:
- NO process_status column exists anywhere.
- NO delivery_status on order_items.
- ONLY use the exact columns listed above.
- For unpaid/payment queries, ALWAYS join invoices first.
"""

STAGE1_PROMPT = """You are a fast intent classifier for a business data system (SAP Order-to-Cash).
Normalize the user query and extract structured intent.

Output STRICT JSON only. No explanation. No markdown.

Format:
{
  "intent": "list_unpaid_orders" | "trace_order" | "top_products" | "customer_summary" | "count_orders" | "list_invoices" | "list_deliveries" | "list_payments" | "unknown",
  "normalized_query": "clean version of the query",
  "entities": {
    "order_id": null or "string",
    "customer_id": null or "string",
    "product": null or "string"
  }
}

Intent mappings (use these EXACTLY):
- "unpaid orders", "orders not paid", "pending payments", "outstanding invoices" → intent: "list_unpaid_orders"
- "trace order X", "show order X", "order X status" → intent: "trace_order"
"""

STAGE2_PROMPT = f"""You are a precise SQLite SQL generator for a business database.

{DB_SCHEMA}

Generate a single valid SQLite SELECT query. Output STRICT JSON only. No explanation. No markdown. No thinking.

Format:
{{"sql": "SELECT ...", "confidence": "high" | "low"}}

Rules:
- ONLY SELECT statements
- ONLY use tables/columns listed in the schema
- For unpaid orders always use the exact join pattern provided
- Set confidence to "low" if the query cannot be reliably mapped to the schema
- NEVER return only an ID column — always select rich descriptive columns using `*` or explicit valid columns (e.g., `SELECT o.*, c.name FROM orders o...`).
- CRITICAL: Do NOT invent column names. Only use the exact columns listed in the schema. Pay close attention to which table a column belongs to.
"""


class QuestionRequest(BaseModel):
    question: str


# ── Stage 1: Groq intent parser ──────────────────────────────────────────────
def parse_intent(question: str) -> dict:
    try:
        from groq import Groq
        client = Groq(api_key=os.environ.get("GROQ_API_KEY"))
        resp = client.chat.completions.create(
            model="llama-3.1-8b-instant",
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": STAGE1_PROMPT},
                {"role": "user",   "content": question},
            ],
            temperature=0.0,
        )
        return json.loads(resp.choices[0].message.content)
    except Exception as e:
        logger.error(f"Stage 1 (intent parse) failed: {e}")
        return {
            "intent": "unknown",
            "normalized_query": question.strip().lower(),
            "entities": {}
        }


# ── Stage 2: Gemini SQL generator ─────────────────────────────────────────────
def generate_sql_gemini(normalized: str, intent: str) -> dict:
    try:
        import google.generativeai as genai
        genai.configure(api_key=os.environ.get("GEMINI_API_KEY"))
        model = genai.GenerativeModel(
            model_name="gemini-2.0-flash",
            generation_config=genai.types.GenerationConfig(
                temperature=0.0,
                response_mime_type="application/json",
            )
        )
        prompt = f"Intent: {intent}\nQuery: {normalized}\n\nGenerate the SQL. Output JSON only."
        result = model.generate_content([STAGE2_PROMPT, prompt])
        raw = result.text.strip()
        # Strip any accidental markdown fences
        raw = re.sub(r'^```(?:json)?\s*', '', raw, flags=re.MULTILINE)
        raw = re.sub(r'\s*```$', '', raw, flags=re.MULTILINE)
        return json.loads(raw)
    except Exception as e:
        logger.error(f"Stage 2 Gemini SQL failed: {e}")
        return {"sql": "", "confidence": "none"}


# ── Stage 2 fallback: Groq SQL generator ─────────────────────────────────────
def generate_sql_groq(normalized: str, intent: str) -> dict:
    try:
        from groq import Groq
        client = Groq(api_key=os.environ.get("GROQ_API_KEY"))
        resp = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": STAGE2_PROMPT},
                {"role": "user",   "content": f"Intent: {intent}\nQuery: {normalized}"},
            ],
            temperature=0.0,
        )
        return json.loads(resp.choices[0].message.content)
    except Exception as e:
        logger.error(f"Stage 2 Groq SQL fallback failed: {e}")
        return {"sql": "", "confidence": "none"}


def generate_sql(normalized: str, intent: str) -> dict:
    """Try Gemini first, fall back to Groq."""
    result = generate_sql_gemini(normalized, intent)
    if not result.get("sql") or result.get("confidence") == "none":
        logger.warning("Gemini SQL failed, falling back to Groq")
        result = generate_sql_groq(normalized, intent)
    return result


# ── SQL Validation ────────────────────────────────────────────────────────────
def validate_sql(sql: str) -> bool:
    """Strict safety check: only SELECT, only known tables, no destructive ops."""
    if not sql or not sql.strip():
        return False
    lower = sql.lower().strip()

    # Must start with SELECT or WITH (CTE)
    if not (lower.startswith("select") or lower.startswith("with")):
        return False

    # Block any DML/DDL keywords
    disallowed = ["drop", "delete", "update", "insert", "alter",
                  "create", "grant", "revoke", "replace", "truncate"]
    for w in disallowed:
        if re.search(r'\b' + w + r'\b', lower):
            return False

    # Ensure at least one valid table is referenced
    if not any(t in lower for t in ALLOWED_TABLES):
        return False

    return True


# ── Python-based response formatter (no LLM needed) ──────────────────────────
def format_preview(results: list, entity: str) -> list:
    preview = []
    id_field = f"{entity}_id" if entity != "unknown" else "order_id"
    fallback_keys = ["order_id", "customer_id", "delivery_id",
                     "invoice_id", "payment_id", "name"]
    for r in results:
        if id_field in r:
            preview.append(r[id_field])
        else:
            for k in fallback_keys:
                if k in r and r[k]:
                    preview.append(r[k])
                    break
            else:
                preview.append(list(r.values())[0])
    return preview[:5]


def python_format_summary(results: list, title: str, entity: str) -> str:
    """Build a clean bullet-point summary using Python only — no LLM, no hallucination."""
    count = len(results)
    id_field = f"{entity}_id" if entity not in ("unknown", "") else "order_id"
    fallback_keys = ["order_id", "customer_id", "delivery_id",
                     "invoice_id", "payment_id", "name"]

    bullets = []
    for r in results[:10]:
        label = None
        if id_field in r:
            label = str(r[id_field])
        else:
            for k in fallback_keys:
                if k in r and r[k]:
                    label = str(r[k])
                    break
        if label:
            bullets.append(f"• {entity.capitalize()} {label}")

    lines = [f"Here are the results:\n"]
    lines += bullets
    if count > 10:
        lines.append(f"  … and {count - 10} more")
    lines.append(f"\nTotal: {count} result{'s' if count != 1 else ''} found.")
    return "\n".join(lines)


def get_entity_from_intent(intent: str) -> str:
    mapping = {
        "list_unpaid_orders": "order",
        "trace_order": "order",
        "count_orders": "order",
        "top_products": "product",
        "customer_summary": "customer",
        "list_invoices": "invoice",
        "list_deliveries": "delivery",
        "list_payments": "payment",
    }
    return mapping.get(intent, "order")


def get_title_from_intent(intent: str, normalized: str) -> str:
    mapping = {
        "list_unpaid_orders": "Unpaid Orders",
        "trace_order": "Order Trace",
        "count_orders": "Order Count",
        "top_products": "Top Products",
        "customer_summary": "Customer Summary",
        "list_invoices": "Invoices",
        "list_deliveries": "Deliveries",
        "list_payments": "Payments",
    }
    return mapping.get(intent, normalized.title()[:40])


# ── Pagination endpoint ────────────────────────────────────────────────────────
@router.get("/{query_id}")
def load_query_page(query_id: str, page: int = 0, limit: int = 10):
    from config import DB_PATH
    if query_id not in sql_cache:
        return {"error": "Query expired or not found", "full_data": []}
    base_sql = sql_cache[query_id].rstrip(";")
    paginated = f"{base_sql} LIMIT {limit} OFFSET {page * limit}"
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(paginated).fetchall()
        return {"full_data": [dict(r) for r in rows]}
    except Exception as e:
        return {"error": str(e), "full_data": []}
    finally:
        conn.close()


# ── Main query endpoint ────────────────────────────────────────────────────────
@router.post("/")
def natural_language_query(body: QuestionRequest):
    from config import DB_PATH

    # ── Stage 1: Intent + normalization ──────────────────────────────────────
    stage1 = parse_intent(body.question)
    normalized = stage1.get("normalized_query", body.question)
    intent = stage1.get("intent", "unknown")
    entities = stage1.get("entities", {})
    logger.info(f"[Stage 1] Q='{body.question}' → intent='{intent}' normalized='{normalized}'")

    # ── Stage 2: SQL generation (Gemini first, Groq fallback) ────────────────
    stage2 = generate_sql(normalized, intent)
    sql = stage2.get("sql", "").strip()
    confidence = stage2.get("confidence", "none").lower()

    if confidence in ("none", "low") or not sql:
        logger.warning(f"[Stage 2] Low/no confidence for query: '{body.question}'")
        return {"type": "error", "answer": "I couldn't find reliable data for this query."}

    if not validate_sql(sql):
        logger.warning(f"[Stage 2] SQL validation failed: {sql[:120]}")
        return {"type": "error", "answer": "I couldn't find reliable data for this query."}

    logger.info(f"[Stage 2] SQL → {sql[:120]}")

    # ── Execute SQL ───────────────────────────────────────────────────────────
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(sql).fetchall()
        results = [dict(r) for r in rows]
    except Exception as e:
        logger.error(f"[SQL Exec] {e}")
        return {"type": "error", "answer": "An error occurred while querying the database."}
    finally:
        conn.close()

    if not results:
        logger.info("[SQL Exec] 0 rows returned")
        return {"type": "error", "answer": "No matching data found for your query."}

    logger.info(f"[SQL Exec] {len(results)} rows returned")

    # ── Format response (Python-only, no hallucination) ───────────────────────
    entity = get_entity_from_intent(intent)
    title  = get_title_from_intent(intent, normalized)
    summary = python_format_summary(results, title, entity)
    preview = format_preview(results, entity)

    # Cache SQL for pagination — keyed by new UUID every request (no stale cache)
    query_id = str(uuid.uuid4())
    sql_cache[query_id] = sql.rstrip(";")

    return {
        "type": "list",
        "title": title,
        "summary": summary,
        "total": len(results),
        "preview": preview,
        "full_data": results,
        "query_id": query_id,
        "entity": entity,
        "intent": intent,
        "entities": entities,
    }
