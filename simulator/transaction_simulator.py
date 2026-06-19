import json
import random
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from confluent_kafka import Producer

from pipeline.common.config import get_settings
from pipeline.common.logging_config import get_logger

log = get_logger("simulator")

CHANNELS = ("fednow", "ach")
CURRENCIES = ("USD", "EUR", "GBP")
MERCHANT_CATEGORIES = ("grocery", "electronics", "travel", "wire", "atm", "utilities")
COUNTRIES = ("US", "GB", "DE", "IN", "NG", "RU")
OP_WEIGHTS = (("insert", 0.80), ("update", 0.15), ("delete", 0.05))


def _build_producer(settings) -> Producer:
    return Producer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            # event hubs kafka endpoint requires sasl_ssl - plain is rejected
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "PLAIN",
            # the literal $ConnectionString is the mandatory username for event hubs sasl plain
            "sasl.username": "$ConnectionString",
            "sasl.password": settings.eventhub_connection_string,
            "client.id": "sentinel-simulator",
            # idempotent producer prevents a broker-side retry from creating duplicate offsets
            "enable.idempotence": True,
        }
    )


def _choose_op_type() -> str:
    roll = random.random()
    cumulative = 0.0
    for op_type, weight in OP_WEIGHTS:
        cumulative += weight
        if roll <= cumulative:
            return op_type
    return "insert"


def _make_event(customer_ids: list[str]) -> dict:
    now = datetime.now(timezone.utc)
    # change_ts trails txn_ts by a random lag so downstream has real out-of-order cases to resolve
    change_ts = now - timedelta(seconds=random.randint(0, 90))
    amount = Decimal(random.randint(100, 500_000)) / Decimal(100)
    return {
        "transaction_id": str(uuid.uuid4()),
        "customer_id": random.choice(customer_ids),
        "account_no": f"AC{random.randint(10_000_000, 99_999_999)}",
        "counterparty_account": f"CP{random.randint(10_000_000, 99_999_999)}",
        "amount": str(amount),
        "currency": random.choice(CURRENCIES),
        "channel": random.choice(CHANNELS),
        "merchant_category": random.choice(MERCHANT_CATEGORIES),
        "txn_ts": now.isoformat(),
        "geo_country": random.choice(COUNTRIES),
        "device_id": f"dev-{random.randint(1000, 9999)}",
        "is_new_payee": random.random() < 0.20,
        "op_type": _choose_op_type(),
        "change_ts": change_ts.isoformat(),
        "is_fraud": random.random() < 0.03,
    }


def run(event_count: int = 500, customer_pool: int = 50) -> int:
    settings = get_settings()
    producer = _build_producer(settings)
    customer_ids = [f"CU{idx:05d}" for idx in range(customer_pool)]

    produced = 0
    for _ in range(event_count):
        event = _make_event(customer_ids)
        # key on customer_id so one customer's events stay ordered within a single partition
        producer.produce(
            settings.eventhub_topic,
            key=event["customer_id"],
            value=json.dumps(event),
        )
        produced += 1
        # poll drains delivery callbacks so the local send queue does not back up under load
        producer.poll(0)

    producer.flush()
    log.info("simulator run complete", extra={"rows_in": produced})
    return produced


if __name__ == "__main__":
    run()
