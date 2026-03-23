"""
main.py
───────
FastAPI application entry point.
Run: uvicorn main:app --reload --port 8000
"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from routers import orders, customers, products, analytics, query

app = FastAPI(
    title="SAP O2C API",
    description="Order-to-Cash analytics API backed by SQLite",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(customers.router, prefix="/customers", tags=["Customers"])
app.include_router(products.router,  prefix="/products",  tags=["Products"])
app.include_router(orders.router,    prefix="/orders",    tags=["Orders"])
app.include_router(analytics.router, prefix="/analytics", tags=["Analytics"])
app.include_router(query.router,     prefix="/query",     tags=["LLM Query"])


@app.get("/", tags=["Health"])
def root():
    return {"status": "ok", "message": "SAP O2C API is running"}
