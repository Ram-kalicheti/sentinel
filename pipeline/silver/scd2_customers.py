from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from pipeline.common.logging_config import get_logger

log = get_logger("silver.scd2")

DIM_TABLE = "silver.dim_customers"

# only identity and risk attributes drive a new version - account_open_date is
# immutable once set and is never a versioning trigger
TRACKED_ATTRS = ["risk_segment", "home_country"]
MUTATIONS = ("update", "delete")


def _latest_per_customer(changes: DataFrame) -> DataFrame:
    # a single batch may contain several events for one customer, some arriving
    # out of order - collapse to the highest change_ts so the batch is replay
    # safe and a stale event in the same batch cannot win
    w = Window.partitionBy("customer_id").orderBy(F.col("change_ts").desc())
    return (
        changes
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def _effective_changes(spark: SparkSession, latest: DataFrame) -> DataFrame:
    # an update whose tracked attributes match the live row is a no-op - filtering
    # it out keeps the dimension from accumulating identical versions
    if not DeltaTable.isDeltaTable(spark, DIM_PATH):
        # no existing dimension - all incoming inserts are effective, mutations dropped
        return latest.filter(F.col("op_type") == "insert")

    current = (
        spark.read.format("delta").load(DIM_PATH)
        .filter(F.col("is_current"))
        .select(
            F.col("customer_id").alias("_cur_customer_id"),
            F.col("effective_start").alias("_cur_effective_start"),
            *[F.col(a).alias(f"_cur_{a}") for a in TRACKED_ATTRS],
        )
    )

    joined = latest.join(
        current,
        latest.customer_id == current._cur_customer_id,
        "left",
    )

    attr_changed = F.lit(False)
    for a in TRACKED_ATTRS:
        attr_changed = attr_changed | (F.col(a) != F.col(f"_cur_{a}"))

    # a late event older than the live row must never reopen history
    not_stale = F.col("_cur_effective_start").isNull() | (
        F.col("change_ts") > F.col("_cur_effective_start")
    )

    keep = not_stale & (
        F.col("_cur_customer_id").isNull()
        | (F.col("op_type") == "delete")
        | ((F.col("op_type") == "update") & attr_changed)
        | (F.col("op_type") == "insert")
    )

    return joined.filter(keep).drop(
        "_cur_customer_id",
        "_cur_effective_start",
        *[f"_cur_{a}" for a in TRACKED_ATTRS],
    )


def _assign_surrogate_keys(spark: SparkSession, df: DataFrame) -> DataFrame:
    current_max = (
        spark.table(DIM_TABLE).agg(F.max("customer_sk")).collect()[0][0] or 0
    )
    w = Window.orderBy("customer_id", "change_ts")
    return df.withColumn(
        "customer_sk", F.lit(current_max) + F.row_number().over(w)
    )


def _close_superseded(spark: SparkSession, latest: DataFrame) -> None:
    target = DeltaTable.forName(spark, DIM_TABLE)
    # the change_ts > effective_start predicate is the out-of-order guard - a late
    # arriving older event cannot close a row that already reflects newer state
    (
        target.alias("t")
        .merge(
            latest.alias("s"),
            "t.customer_id = s.customer_id AND t.is_current = true",
        )
        .whenMatchedUpdate(
            condition=(
                "s.change_ts > t.effective_start AND s.op_type IN ('update','delete')"
            ),
            set={
                "effective_end": "s.change_ts",
                "is_current": "false",
            },
        )
        .execute()
    )


def _insert_new_versions(spark: SparkSession, new_versions: DataFrame) -> None:
    target = DeltaTable.forName(spark, DIM_TABLE)
    # matching on (customer_id, effective_start) means a replayed batch finds the
    # version already present and inserts nothing - the second run is a no-op
    (
        target.alias("t")
        .merge(
            new_versions.alias("s"),
            "t.customer_id = s.customer_id AND t.effective_start = s.change_ts",
        )
        .whenNotMatchedInsert(
            condition="s.op_type IN ('insert','update')",
            values={
                "customer_sk": "s.customer_sk",
                "customer_id": "s.customer_id",
                "risk_segment": "s.risk_segment",
                "home_country": "s.home_country",
                "account_open_date": "s.account_open_date",
                "effective_start": "s.change_ts",
                "effective_end": F.lit(None).cast("timestamp"),
                "is_current": F.lit(True),
            },
        )
        .execute()
    )


def run(spark: SparkSession, customer_changes: DataFrame) -> dict:
    changes_in = customer_changes.count()
    latest = _latest_per_customer(customer_changes)
    effective = _effective_changes(spark, latest).cache()

    rows_effective = effective.count()

    # close before insert so a delete leaves no open successor and an update has
    # its predecessor closed in the same logical operation
    _close_superseded(spark, effective)

    new_versions = _assign_surrogate_keys(
        spark, effective.filter(F.col("op_type").isin("insert", "update"))
    )
    _insert_new_versions(spark, new_versions)

    log.info(
        "scd2 merge complete",
        extra={"rows_in": changes_in, "rows_passed": rows_effective},
    )
    effective.unpersist()
    return {"changes_in": changes_in, "effective_changes": rows_effective}
