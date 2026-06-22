import time

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json

from pipeline.common.config import get_settings
from pipeline.common.logging_config import get_logger
from schema.delta_schemas import TRANSACTION_EVENT_SCHEMA

log = get_logger("bronze")

BRONZE_TXN_APP_ID = "sentinel-bronze"


def _kafka_source(spark: SparkSession, settings) -> DataFrame:
    jaas = (
        "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required "
        f'username="$ConnectionString" password="{settings.eventhub_connection_string}";'
    )
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
        .option("subscribe", settings.eventhub_topic)
        # event hubs kafka endpoint requires sasl_ssl - plain is rejected
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option("kafka.sasl.jaas.config", jaas)
        .option("startingOffsets", "earliest")
        # exactly-once is owned by the delta checkpoint - kafka auto-commit is not the source of truth
        .option("kafka.enable.auto.commit", "false")
        .option("failOnDataLoss", "false")
        .load()
    )


def _to_bronze(raw: DataFrame) -> DataFrame:
    decoded = raw.select(
        col("partition").alias("_kafka_partition"),
        col("offset").cast("long").alias("_kafka_offset"),
        col("value").cast("string").alias("_raw_payload"),
        from_json(col("value").cast("string"), TRANSACTION_EVENT_SCHEMA).alias("event"),
    )
    return decoded.select(
        "event.*", "_kafka_partition", "_kafka_offset", "_raw_payload"
    ).withColumn("_ingest_ts", current_timestamp())


def _land_batch(batch_df: DataFrame, batch_id: int, settings) -> int:
    rows = batch_df.count()
    bronze = _to_bronze(batch_df)
    (
        bronze.write.format("delta")
        .mode("append")
        # delta dedupes a re-driven micro-batch by (appId, batchId) so the append stays idempotent
        .option("txnAppId", BRONZE_TXN_APP_ID)
        .option("txnVersion", batch_id)
        .saveAsTable(settings.bronze_table)
    )
    return rows


def _land_batch_with_retry(settings):
    def handler(batch_df: DataFrame, batch_id: int) -> None:
        attempt = 0
        started = time.monotonic()
        while True:
            try:
                rows = _land_batch(batch_df, batch_id, settings)
                log.info(
                    "bronze batch landed",
                    extra={
                        "batch_id": batch_id,
                        "rows_in": rows,
                        "duration_ms": int((time.monotonic() - started) * 1000),
                    },
                )
                return
            except Exception:
                attempt += 1
                if attempt > settings.stream_max_retries:
                    log.error(
                        "bronze batch failed", extra={"batch_id": batch_id, "attempt": attempt}
                    )
                    raise
                # exponential backoff rides out a transient broker or storage blip without dropping the batch
                delay = settings.stream_backoff_base_seconds ** attempt
                log.warning(
                    "bronze batch retry", extra={"batch_id": batch_id, "attempt": attempt}
                )
                time.sleep(delay)

    return handler


def run(spark: SparkSession) -> None:
    settings = get_settings()
    source = _kafka_source(spark, settings)
    query = (
        source.writeStream.foreachBatch(_land_batch_with_retry(settings))
        # the checkpoint is what lets a restart resume instead of reprocess - never shared across streams
        .option("checkpointLocation", settings.bronze_checkpoint_path)
        # availableNow drains the current backlog then stops - cheaper than an always-on stream at dev scale
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    run(SparkSession.builder.getOrCreate())
