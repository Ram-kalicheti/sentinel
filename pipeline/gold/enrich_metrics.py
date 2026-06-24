from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from pipeline.common.logging_config import get_logger

log = get_logger("gold.enrich")

SILVER_PATH = "abfss://silver@sentinelstgrk1.dfs.core.windows.net/silver_transactions"
DIM_PATH = "abfss://silver@sentinelstgrk1.dfs.core.windows.net/dim_customers"
GOLD_PATH = "abfss://gold@sentinelstgrk1.dfs.core.windows.net/transaction_enriched"

HOUR_SECONDS = 3600
THIRTY_DAY_SECONDS = 30 * 24 * 3600

# the 200 default shuffle partition count is sized for cluster-scale data - at dev
# volume it spawns 200 near-empty tasks whose scheduling overhead dominates the
# real work, so a small explicit count removes that overhead
TUNED_SHUFFLE_PARTITIONS = 8
DEFAULT_SHUFFLE_PARTITIONS = 200


def _current_dimension(spark: SparkSession) -> DataFrame:
    # only the open SCD2 version carries the customer's present risk_segment and
    # surrogate key - closed history rows would double-count the join
    return (
        spark.read.format("delta").load(DIM_PATH)
        .filter(F.col("is_current"))
        .select("customer_id", "customer_sk", "risk_segment")
    )


def compute_rolling_features(df: DataFrame) -> DataFrame:
    # range frames need a numeric ordering column - epoch seconds lets the window
    # span a real time interval rather than a fixed number of preceding rows
    df = df.withColumn("_ts_sec", F.col("txn_ts").cast("long"))

    w_1h = (
        Window.partitionBy("customer_id").orderBy("_ts_sec")
        .rangeBetween(-HOUR_SECONDS, 0)
    )
    w_30d = (
        Window.partitionBy("customer_id").orderBy("_ts_sec")
        .rangeBetween(-THIRTY_DAY_SECONDS, 0)
    )

    df = df.withColumn("txn_count_1h", F.count("*").over(w_1h))
    df = df.withColumn("amount_mean_30d", F.avg("amount").over(w_30d))
    df = df.withColumn("amount_std_30d", F.stddev("amount").over(w_30d))

    # a zero or null std means too few points to score deviation - emit 0 rather
    # than divide by zero, so a brand-new customer is neutral, not infinite
    df = df.withColumn(
        "amount_zscore_30d",
        F.when(
            F.col("amount_std_30d") > 0,
            (F.col("amount") - F.col("amount_mean_30d")) / F.col("amount_std_30d"),
        ).otherwise(F.lit(0.0)),
    )

    # velocity is raw 1h count surfaced as a double so the ML layer can scale it
    df = df.withColumn("velocity_score", F.col("txn_count_1h").cast("double"))
    return df.drop("_ts_sec")


def compute_peer_benchmark(df: DataFrame, broadcast_peer: bool = True) -> DataFrame:
    # peer group is the risk_segment cohort - deviation from peers catches a
    # customer behaving unlike others of the same assessed risk
    peer = df.groupBy("risk_segment").agg(
        F.avg("amount").alias("_peer_mean"),
        F.stddev("amount").alias("_peer_std"),
    )

    # the peer table is one row per risk_segment - broadcasting it sends that tiny
    # frame to every executor and turns a shuffle join into a map-side join
    peer_side = F.broadcast(peer) if broadcast_peer else peer
    df = df.join(peer_side, "risk_segment", "left")
    df = df.withColumn(
        "peer_deviation",
        F.when(
            F.col("_peer_std") > 0,
            (F.col("amount") - F.col("_peer_mean")) / F.col("_peer_std"),
        ).otherwise(F.lit(0.0)),
    )
    return df.drop("_peer_mean", "_peer_std")


def enrich(spark: SparkSession, broadcast_dim: bool = True) -> DataFrame:
    silver = spark.read.format("delta").load(SILVER_PATH)
    dim = _current_dimension(spark)

    # dim_customers is small relative to the fact stream - broadcasting it avoids
    # shuffling the whole transaction set across the network to align join keys
    dim_side = F.broadcast(dim) if broadcast_dim else dim

    # attach customer_sk + risk_segment before feature work so peer grouping and
    # the gold grain both have the dimension context
    joined = silver.join(dim_side, "customer_id", "left")

    enriched = compute_rolling_features(joined)
    enriched = compute_peer_benchmark(enriched, broadcast_peer=broadcast_dim)

    return enriched.select(
        "transaction_id", "customer_sk", "customer_id", "risk_segment",
        "channel", "amount", "txn_ts", "is_fraud",
        "txn_count_1h", "amount_mean_30d", "amount_std_30d",
        "amount_zscore_30d", "velocity_score", "peer_deviation",
        F.lit(datetime.now(timezone.utc)).alias("_enriched_ts"),
    )


def run(spark: SparkSession, tuned: bool = True) -> dict:
    # the tuned path right-sizes shuffle partitions and broadcasts the small
    # joins; the untuned path is the platform default, kept so the timing
    # comparison measures a real before and after
    spark.conf.set(
        "spark.sql.shuffle.partitions",
        str(TUNED_SHUFFLE_PARTITIONS if tuned else DEFAULT_SHUFFLE_PARTITIONS),
    )

    enriched = enrich(spark, broadcast_dim=tuned)
    # gold is a full rebuild from silver each run - overwrite keeps it a pure
    # function of current silver state and avoids stale feature rows
    enriched.write.format("delta").mode("overwrite") \
        .option("overwriteSchema", "true").save(GOLD_PATH)

    rows_out = spark.read.format("delta").load(GOLD_PATH).count()
    log.info("gold enrichment complete", extra={"rows_passed": rows_out})
    return {"rows_enriched": rows_out}
