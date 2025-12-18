import json
import os
import random
import time
from datetime import datetime
from collections import deque

from kafka import KafkaProducer


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC_TRANSACTIONS = os.getenv("KAFKA_TOPIC", "transactions")
KAFKA_TOPIC_REFUNDS = os.getenv("KAFKA_TOPIC_REFUNDS", "refunds")
SLEEP_SECONDS = float(os.getenv("PRODUCER_SLEEP_SECONDS", "1.0"))
REFUND_PROBABILITY = float(os.getenv("REFUND_PROBABILITY", "0.1"))  # 10% chance

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

# Keep track of recent transactions for refunds
recent_transactions = deque(maxlen=100)


def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


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


def main() -> None:
    random.seed()
    producer = create_producer()
    tx_id = 1
    refund_id = 1

    print(f"[producer] Starting multi-stream producer:")
    print(f"  - Transactions topic: '{KAFKA_TOPIC_TRANSACTIONS}'")
    print(f"  - Refunds topic: '{KAFKA_TOPIC_REFUNDS}'")
    print(f"  - Refund probability: {REFUND_PROBABILITY*100:.0f}%")
    print(f"  - Bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")

    while True:
        # Generate and send transaction
        tx = build_transaction(tx_id)
        producer.send(KAFKA_TOPIC_TRANSACTIONS, value=tx)
        recent_transactions.append(tx)
        print(f"[producer] Sent transaction: {tx}")
        tx_id += 1

        # Randomly generate refunds for past transactions
        if recent_transactions and random.random() < REFUND_PROBABILITY:
            original_tx = random.choice(list(recent_transactions))
            refund = build_refund(refund_id, original_tx)
            producer.send(KAFKA_TOPIC_REFUNDS, value=refund)
            print(f"[producer] Sent refund: {refund}")
            refund_id += 1

        producer.flush()
        time.sleep(SLEEP_SECONDS)


if __name__ == "__main__":
    main()

