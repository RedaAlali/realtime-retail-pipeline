import random
from datetime import datetime

# These product IDs must match dim_products in Postgres
PRODUCTS = [
    {"product_id": "LAPTOP-001", "category": "Electronics"},
    {"product_id": "PHONE-001", "category": "Electronics"},
    {"product_id": "HEADPHONES-001", "category": "Accessories"},
    {"product_id": "KEYBOARD-001", "category": "Accessories"},
    {"product_id": "MOUSE-001", "category": "Accessories"},
    {"product_id": "MONITOR-001", "category": "Electronics"},
]

STORES = ["STORE-001", "STORE-002", "STORE-003"]
CUSTOMER_ID_MAX = 1000

REFUND_REASONS = [
    "defective",
    "wrong_item",
    "not_as_described",
    "changed_mind",
    "damaged_shipping",
]

def build_transaction(transaction_id: int) -> dict:
    product = random.choice(PRODUCTS)
    store_id = random.choice(STORES)
    customer_id = f"CUST-{random.randint(1, CUSTOMER_ID_MAX):04d}"

    tx = {
        "transaction_id": transaction_id,
        "product_id": product["product_id"],
        "store_id": store_id,
        "customer_id": customer_id,
        "amount": round(random.uniform(20.0, 1500.0), 2),
        "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
    }
    return tx

def build_refund(refund_id: int, original_tx: dict) -> dict:
    """Build a refund event based on an original transaction."""
    # Refund can be partial (50-100% of original amount)
    refund_pct = random.uniform(0.5, 1.0)
    refund_amount = round(original_tx["amount"] * refund_pct, 2)
    
    return {
        "refund_id": refund_id,
        "original_transaction_id": original_tx["transaction_id"],
        "product_id": original_tx["product_id"],
        "store_id": original_tx["store_id"],
        "customer_id": original_tx["customer_id"],
        "refund_amount": refund_amount,
        "reason": random.choice(REFUND_REASONS),
        "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
    }
