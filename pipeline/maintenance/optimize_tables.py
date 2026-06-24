from delta.tables import DeltaTable
from pyspark.sql import SparkSession

from pipeline.common.logging_config import get_logger

log = get_logger("maintenance.optimize")

SILVER_PATH = "abfss://silver@sentinelstgrk1.dfs.core.windows.net/silver_transactions"
GOLD_PATH = "abfss://gold@sentinelstgrk1.dfs.core.windows.net/transaction_enriched"

# customer_id is the dominant filter and join key for both tables, so colocating
# rows by it lets data skipping prune files on point lookups instead of full scans
ZORDER_COLUMN = "customer_id"


def optimize_path(spark: SparkSession, path: str) -> None:
    # streaming and merge workloads leave many small files - OPTIMIZE compacts them
    # into right-sized files, and ZORDER clusters those files on the lookup key
    dt = DeltaTable.forPath(spark, path)
    dt.optimize().executeZOrderBy(ZORDER_COLUMN)


def run(spark: SparkSession) -> None:
    for name, path in (("silver", SILVER_PATH), ("gold", GOLD_PATH)):
        optimize_path(spark, path)
        log.info("optimize complete", extra={"stage": name})
