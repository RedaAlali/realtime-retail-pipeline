"""
Transaction Producer

Establishes connection to Kafka broker and loops generating transactions/refunds.
"""

import json
import os
import random
import time
from collections import deque
from kafka import KafkaProducer

try:
    from src.generator import build_transaction, build_refund
except ImportError:
    from generator import build_transaction, build_refund

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC_TRANSACTIONS = os.getenv("KAFKA_TOPIC", "transactions")
KAFKA_TOPIC_REFUNDS = os.getenv("KAFKA_TOPIC_REFUNDS", "refunds")
SLEEP_SECONDS = float(os.getenv("PRODUCER_SLEEP_SECONDS", "1.0"))
REFUND_PROBABILITY = float(os.getenv("REFUND_PROBABILITY", "0.1"))  # 10% chance

# Keep track of recent transactions for refunds
recent_transactions = deque(maxlen=100)

def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

def main() -> None:
    random.seed()
    
    # Simple retry block to allow Kafka to fully boot up
    producer = None
    for attempt in range(5):
        try:
            producer = create_producer()
            break
        except Exception as e:
            print(f"[producer] Connection attempt {attempt + 1} failed: {e}. Retrying in 10s...")
            time.sleep(10)
            
    if not producer:
        print("[producer] ERROR: Could not connect to Kafka. Exiting.")
        return

    tx_id = 1
    refund_id = 1

    print(f"[producer] Starting multi-stream producer:")
    print(f"  - Transactions topic: '{KAFKA_TOPIC_TRANSACTIONS}'")
    print(f"  - Refunds topic: '{KAFKA_TOPIC_REFUNDS}'")
    print(f"  - Refund probability: {REFUND_PROBABILITY*100:.0f}%")
    print(f"  - Bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")

    while True:
        try:
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
        except Exception as e:
            print(f"[producer] Error sending event: {e}")
            
        time.sleep(SLEEP_SECONDS)

if __name__ == "__main__":
    main()
