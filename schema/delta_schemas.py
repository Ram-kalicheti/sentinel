from pyspark.sql.types import (
    BooleanType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# decimal(12,2) matches ledger precision - floats drift on money and break reconciliation
TRANSACTION_EVENT_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("account_no", StringType(), True),
        StructField("counterparty_account", StringType(), True),
        StructField("amount", DecimalType(12, 2), False),
        StructField("currency", StringType(), True),
        StructField("channel", StringType(), True),
        StructField("merchant_category", StringType(), True),
        StructField("txn_ts", TimestampType(), False),
        StructField("geo_country", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("is_new_payee", BooleanType(), True),
        StructField("op_type", StringType(), False),
        StructField("change_ts", TimestampType(), False),
        StructField("is_fraud", BooleanType(), True),
    ]
)

# the raw payload is retained so a future parse change can be replayed from the landing zone
BRONZE_METADATA_FIELDS = [
    StructField("_kafka_partition", IntegerType(), True),
    StructField("_kafka_offset", LongType(), True),
    StructField("_ingest_ts", TimestampType(), True),
    StructField("_raw_payload", StringType(), True),
]

BRONZE_TRANSACTIONS_SCHEMA = StructType(
    TRANSACTION_EVENT_SCHEMA.fields + BRONZE_METADATA_FIELDS
)
