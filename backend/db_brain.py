"""
db_brain.py — DB Brain layer for structured semantic SAP database understanding.
"""

from typing import Dict, Any, List

BUSINESS_FLOW = "Business Partner (Customer) -> Sales Order -> Outbound Delivery -> Billing Document (Invoice) -> Payment (Accounts Receivable)"

BUSINESS_CONTEXT: Dict[str, Any] = {
    "description": "This database represents an order-to-cash system in SAP.\n\nBusiness partners (customers) place sales orders.\nSales orders generate outbound deliveries.\nOutbound deliveries generate billing documents (invoices).\nBilling documents are settled by payments in accounts receivable.",
    "tables": {
        "business_partners": {
            "description": "Stores business partner details, representing customers.",
            "columns": {
                "business_partner": "Unique identifier for a business partner (customer)",
                "business_partner_name": "Name of the business partner",
                "customer": "Customer identifier"
            }
        },
        "sales_order_headers": {
            "description": "Stores high-level sales order information.",
            "columns": {
                "sales_order": "Unique identifier for a sales order",
                "sold_to_party": "Reference to the customer who placed the order",
                "creation_date": "Date when the order was placed",
                "total_net_amount": "Total value of the order",
                "overall_delivery_status": "Current fulfillment status of the order"
            }
        },
        "sales_order_items": {
            "description": "Individual line items within a sales order.",
            "columns": {
                "sales_order": "Reference to the parent sales order",
                "sales_order_item": "Line item number",
                "material": "Reference to the material/product ordered",
                "requested_quantity": "Number of units ordered",
                "net_amount": "Net value of this line item"
            }
        },
        "outbound_delivery_headers": {
            "description": "Records of physical deliveries for orders.",
            "columns": {
                "delivery_document": "Unique identifier for a delivery",
                "creation_date": "Date when the delivery was created",
                "overall_picking_status": "Status of warehouse picking"
            }
        },
        "outbound_delivery_items": {
            "description": "Line items for physical deliveries bridging orders to deliveries.",
            "columns": {
                "delivery_document": "Reference to the delivery document",
                "delivery_document_item": "Delivery item number",
                "reference_sd_document": "Reference to the original sales order",
                "reference_sd_document_item": "Reference to the sales order item"
            }
        },
        "billing_document_headers": {
            "description": "Billing documents (invoices) sent to customers.",
            "columns": {
                "billing_document": "Unique identifier for a billing document",
                "sold_to_party": "Reference to the customer being billed",
                "creation_date": "Date when the invoice was issued",
                "total_net_amount": "Total amount due"
            }
        },
        "billing_document_items": {
            "description": "Line items for billing documents bridging deliveries/orders to invoices.",
            "columns": {
                "billing_document": "Reference to the billing document",
                "billing_document_item": "Billing item number",
                "reference_sd_document": "Reference to the delivery or sales order",
                "reference_sd_document_item": "Reference to the original item number",
                "net_amount": "Net amount of the line item"
            }
        },
        "payments_accounts_receivable": {
            "description": "Financial payments received from customers.",
            "columns": {
                "accounting_document": "Unique identifier for the payment journal entry",
                "customer": "Reference to the customer who made the payment",
                "clearing_date": "Date when the payment was cleared",
                "amount_in_company_code_currency": "Amount paid",
                "sales_document": "Reference to the related sales order"
            }
        },
        "products": {
            "description": "Master list of products/materials available.",
            "columns": {
                "product": "Unique identifier for a product",
                "product_type": "Category or type of the product"
            }
        }
    }
}


def build_db_brain(schema: Dict[str, Any]) -> Dict[str, Any]:
    """Combines raw schema with business semantic context into a DB_BRAIN object."""
    db_brain: Dict[str, Any] = {}
    fks: List[Dict[str, Any]] = schema.get("foreign_keys", [])  # pyre-ignore[9]
    tables: Dict[str, List[str]] = schema.get("tables", {})  # pyre-ignore[9]
    
    for table_name, columns in tables.items():
        table_name_str = str(table_name)
        if table_name_str not in BUSINESS_CONTEXT["tables"]:
            # If there's an unknown table, just provide basic schema
            db_brain[table_name_str] = {
                "description": f"Table {table_name_str}",
                "columns": {str(col): f"Column {col}" for col in columns},
                "relations": {}
            }
            continue
            
        table_context = BUSINESS_CONTEXT["tables"][table_name_str]
        
        # Build column meaning mapping
        col_dict = {}
        for col in columns:
            col_str = str(col)
            columns_context = table_context.get("columns", {})
            col_dict[col_str] = columns_context.get(col_str, f"Column {col_str}")
            
        # Find relations
        relations = {}
        for fk in fks:
            from_t = str(fk.get("from_table", ""))
            if from_t == table_name_str:
                to_t = str(fk.get("to_table", ""))
                from_c = str(fk.get("from_col", ""))
                to_c = str(fk.get("to_col", ""))
                relations[to_t] = f"{from_t}.{from_c} -> {to_t}.{to_c}"
                
        db_brain[table_name_str] = {
            "description": table_context.get("description", ""),
            "columns": col_dict,
            "relations": relations
        }
        
    return db_brain


def get_dynamic_context(db_brain: Dict[str, Any], query: str) -> str:
    """Filters DB_BRAIN based on keywords in the query to provide relevant context."""
    q_lower = query.lower()
    
    relevant_tables = set()
    
    if "customer" in q_lower or "partner" in q_lower or "client" in q_lower:
        relevant_tables.update(["business_partners", "sales_order_headers"])
    if "delivery" in q_lower or "ship" in q_lower or "picking" in q_lower:
        relevant_tables.update(["outbound_delivery_headers", "outbound_delivery_items", "sales_order_headers"])
    if "payment" in q_lower or "pay" in q_lower or "clear" in q_lower:
        relevant_tables.update(["payments_accounts_receivable", "billing_document_headers", "business_partners"])
    if "invoice" in q_lower or "bill" in q_lower:
        relevant_tables.update(["billing_document_headers", "billing_document_items", "sales_order_headers", "business_partners"])
    if "order" in q_lower:
        relevant_tables.update(["sales_order_headers", "sales_order_items", "business_partners"])
    if "product" in q_lower or "item" in q_lower or "material" in q_lower:
        relevant_tables.update(["products", "product_descriptions", "sales_order_items"])
        
    if len(relevant_tables) == 0:
        # Fallback to all core tables if no specific keyword matches
        relevant_tables = set(db_brain.keys())
    
    # Always include orders as it's the central entity for join paths
    relevant_tables.add("sales_order_headers")
    
    # Filter valid tables only
    relevant_tables = {str(t) for t in relevant_tables if str(t) in db_brain}

    lines = []
    lines.append(str(BUSINESS_CONTEXT.get("description", "")))
    lines.append(f"Business Flow: {BUSINESS_FLOW}")
    lines.append("")
    
    lines.append("Tables:")
    for table in relevant_tables:
        t_data = db_brain[table]
        columns_keys = list(t_data.get("columns", {}).keys())
        lines.append(f"* {table}({', '.join(columns_keys)})")
        lines.append(f"  Description: {t_data.get('description', '')}")
        
    lines.append("")
    lines.append("Relationships:")
    
    printed_rels = set()
    for table in relevant_tables:
        relations = db_brain[table].get("relations", {})
        for other_table, rel_desc in relations.items():
            if other_table in relevant_tables:
                rel_str = str(rel_desc)
                if rel_str not in printed_rels:
                    lines.append(f"* {rel_str}")
                    printed_rels.add(rel_str)

    return "\n".join(lines)
