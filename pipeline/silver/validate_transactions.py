from datetime import datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pipeline.common.logging_config import get_logger
from pipeline.silver import checks

log = get_logger("silver.validate")

SILVER_PATH = "abfss://silver@sentinelstgrk1.dfs.core.windows.net/silver_transactions"
DEADLETTER_PATH = "abfss://silver@sentinelstgrk1.dfs.core.windows.net/silver_deadletter"

BUSINESS_FIELDS = [
    "transaction_id", "customer_id", "account_no", "counterparty_account",
    "amount", "currency", "channel", "merchant_category", "txn_ts",
    "geo_country", "device_id", "is_new_payee", "op_type", "change_ts",
    "is_fraud",
]


def split_validated(bronze_df: DataFrame):
    tagged = bronze_df.withColumn("_error_type", checks.error_type_column())
    passed = tagged.filter(F.col("_error_type").isNull())
    failed = tagged.filter(F.col("_error_type").isNotNull())
    return passed, failed


def route_deadletter(failed_df: DataFrame) -> int:
    # validation runs before any merge, so a malformed event is captured here
    # and the job continues - it can never crash the silver write
    dlq = (
        failed_df
        .withColumn("_error_detail", checks.error_detail_column())
        .withColumn("_failed_ts", F.lit(datetime.now(timezone.utc)))
        .select(
            "transaction_id",
            "_raw_payload",
            "_error_type",
            "_error_detail",
            "_failed_ts",
            F.col("_kafka_offset"),
        )
    )
    count = dlq.count()
    if count:
        dlq.write.format("delta").mode("append").save(DEADLETTER_PATH)
    return count


def merge_silver(spark: SparkSession, passed_df: DataFrame) -> None:
    prepared = (
        passed_df
        .select(*BUSINESS_FIELDS)
        .withColumn("_dq_passed", F.lit(True))
        .withColumn("_processed_ts", F.lit(datetime.now(timezone.utc)))
        # a replayed micro-batch can carry the same (transaction_id, change_ts)
        # twice - collapse here so the merge source itself is unique
        .dropDuplicates(["transaction_id", "change_ts"])
    )

    # initialise the table on first run - subsequent runs find it and merge
    if not DeltaTable.isDeltaTable(spark, SILVER_PATH):
        prepared.write.format("delta").save(SILVER_PATH)
        return

    target = DeltaTable.forPath(spark, SILVER_PATH)

    # match on natural key plus the cdc sequence key - a row that is already
    # present is left untouched, so re-running yesterday's batch is a no-op and
    # cannot duplicate or drop facts
    (
        target.alias("t")
        .merge(
            prepared.alias("s"),
            "t.transaction_id = s.transaction_id AND t.change_ts = s.change_ts",
        )
        .whenNotMatchedInsertAll()
        .execute()
    )


def run(spark: SparkSession, bronze_df: DataFrame) -> dict:
    rows_in = bronze_df.count()
    passed, failed = split_validated(bronze_df)
    passed = passed.cache()
    failed = failed.cache()

    rows_passed = passed.count()
    rows_deadlettered = route_deadletter(failed)
    merge_silver(spark, passed)

    stats = {
        "rows_in": rows_in,
        "rows_passed": rows_passed,
        "rows_deadlettered": rows_deadlettered,
    }
    log.info("silver validation complete", extra=stats)

    passed.unpersist()
    failed.unpersist()
    return stats