"""
graph_builder.py

Converts SQL result rows into a stable graph structure {nodes: [], edges: []}.
"""

def build_graph(rows: list[dict]) -> dict:
    graph = {"nodes": [], "edges": []}
    if not rows:
        return graph

    unique_nodes = {}
    unique_edges = {}

    def add_node(id_val, type_val, label, metadata):
        if not id_val: return
        node_id = f"{type_val.lower()}-{str(id_val)}"
        if node_id not in unique_nodes:
            unique_nodes[node_id] = {
                "id": node_id,
                "type": "default",
                "data": {"label": label},
                "metadata": {"type": type_val, **metadata}
            }

    def add_edge(source_type, source_id, target_type, target_id, label):
        if not source_id or not target_id: return
        source_real_id = f"{source_type.lower()}-{str(source_id)}"
        target_real_id = f"{target_type.lower()}-{str(target_id)}"
        
        # Create edge only if both nodes exist (user rule)
        if source_real_id in unique_nodes and target_real_id in unique_nodes:
            edge_id = f"e-{source_real_id}-{target_real_id}"
            if edge_id not in unique_edges:
                unique_edges[edge_id] = {
                    "id": edge_id,
                    "source": source_real_id,
                    "target": target_real_id,
                    "label": label
                }

    for row in rows:
        # Extract base IDs
        cust_id = row.get("customer_id")
        ord_id = row.get("order_id")
        del_id = row.get("delivery_id")
        inv_id = row.get("invoice_id")
        pay_id = row.get("payment_id")
        prod_id = row.get("product_id")

        # 1. Add Nodes
        if cust_id: add_node(cust_id, "Customer", f"Customer {cust_id}", {"id": cust_id})
        if ord_id: add_node(ord_id, "Order", f"Order {ord_id}", {"id": ord_id})
        if del_id: add_node(del_id, "Delivery", f"Delivery {del_id}", {"id": del_id})
        if inv_id: add_node(inv_id, "Invoice", f"Invoice {inv_id}", {"id": inv_id})
        if pay_id: add_node(pay_id, "Payment", f"Payment {pay_id}", {"id": pay_id})
        if prod_id: add_node(prod_id, "Product", f"Product {prod_id}", {"id": prod_id})
            
        # 2. Add Edges
        # Customer -> Order
        if cust_id and ord_id: add_edge("Customer", cust_id, "Order", ord_id, "placed")
        # Order -> Delivery
        if ord_id and del_id: add_edge("Order", ord_id, "Delivery", del_id, "fulfilled by")
        # Delivery -> Invoice or Order -> Invoice
        if del_id and inv_id: add_edge("Delivery", del_id, "Invoice", inv_id, "billed via")
        elif ord_id and inv_id: add_edge("Order", ord_id, "Invoice", inv_id, "billed via")
        # Invoice -> Payment
        if inv_id and pay_id: add_edge("Invoice", inv_id, "Payment", pay_id, "payment received")
        # Product -> Order Item
        if prod_id and ord_id: add_edge("Order", ord_id, "Product", prod_id, "contains")

    graph["nodes"] = list(unique_nodes.values())
    graph["edges"] = list(unique_edges.values())
    return graph
