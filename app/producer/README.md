# Producer (`app/producer/`)

Simulates POS transactions and refunds, publishing to Kafka.

## Files

| File | Description |
|------|-------------|
| `producer.py` | Transaction/refund generator |
| `Dockerfile` | Container configuration |
| `requirements.txt` | Python dependencies |

## Event Schemas

**Transaction** (`transactions` topic):
```json
{"transaction_id": 1, "product_id": "LAPTOP-001", "store_id": "STORE-001", "customer_id": "CUST-001", "amount": 1299.99, "timestamp": "..."}
```

**Refund** (`refunds` topic):
```json
{"refund_id": 1, "original_transaction_id": 1, "product_id": "LAPTOP-001", "refund_amount": 649.99, "reason": "defective", ...}
```

## Refund Reasons

`defective`, `wrong_item`, `not_as_described`, `changed_mind`, `damaged_shipping`

## Environment Variables

| Variable | Default |
|----------|---------|
| `KAFKA_BOOTSTRAP_SERVERS` | kafka:9092 |
| `PRODUCER_SLEEP_SECONDS` | 1.0 |
| `REFUND_PROBABILITY` | 0.1 |
