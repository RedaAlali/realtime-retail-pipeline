# Producer Service (`app/producer/`)

A Python service that simulates Point-of-Sale (POS) retail transactions AND refunds, publishing to two Kafka topics.

## Files

| File | Description |
|------|-------------|
| `producer.py` | Main producer script that generates transactions and refunds |
| `Dockerfile` | Container configuration for the producer service |
| `requirements.txt` | Python dependencies (kafka-python) |

## What It Does

The producer simulates a retail store by generating two types of events:

### Transaction Events (`transactions` topic)

```json
{
  "transaction_id": 12345,
  "product_id": "LAPTOP-001",
  "store_id": "STORE-002",
  "customer_id": "CUST-0042",
  "amount": 1299.99,
  "timestamp": "2025-01-01 12:34:56"
}
```

### Refund Events (`refunds` topic)

```json
{
  "refund_id": 1001,
  "original_transaction_id": 12345,
  "product_id": "LAPTOP-001",
  "store_id": "STORE-002",
  "customer_id": "CUST-0042",
  "refund_amount": 649.99,
  "reason": "defective",
  "timestamp": "2025-01-01 14:00:00"
}
```

### Refund Reasons

- `defective` - Product not working
- `wrong_item` - Wrong product shipped
- `not_as_described` - Product didn't match description
- `changed_mind` - Customer changed their mind
- `damaged_shipping` - Damaged during shipping

### Products & Stores

**Products:**
- LAPTOP-001, PHONE-001, MONITOR-001 (Electronics)
- HEADPHONES-001, KEYBOARD-001, MOUSE-001 (Accessories)

**Stores:**
- STORE-001 (Downtown Mall, Riyadh)
- STORE-002 (Westside Plaza, Jeddah)
- STORE-003 (Tech Hub, Dammam)

## How It Works

1. Connects to Kafka broker
2. Enters infinite loop:
   - Generates a random transaction → sends to `transactions` topic
   - With 10% probability, generates a refund for a recent transaction → sends to `refunds` topic
   - Sleeps for configured interval (default: 1 second)

## Interactions with Other Components

```
Producer ──┬──> Kafka (transactions) ──┬──> Spark
           │                           │
           └──> Kafka (refunds) ───────┘
```

The producer is the **entry point** for all streaming data in the pipeline.

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | kafka:9092 | Kafka broker address |
| `KAFKA_TOPIC` | transactions | Primary transactions topic |
| `KAFKA_TOPIC_REFUNDS` | refunds | Refunds topic |
| `PRODUCER_SLEEP_SECONDS` | 1.0 | Delay between transactions |
| `REFUND_PROBABILITY` | 0.1 | Probability of refund (10%) |

## Running Locally

```bash
# Via Docker
docker compose up producer

# Watch the logs
docker compose logs -f producer
```

You should see output like:
```
[producer] Sent transaction: {'transaction_id': 1, 'product_id': 'LAPTOP-001', ...}
[producer] Sent transaction: {'transaction_id': 2, 'product_id': 'PHONE-001', ...}
[producer] Sent refund: {'refund_id': 1, 'original_transaction_id': 1, 'reason': 'defective', ...}
```

