"""turn the enriched gold table into a model ready feature matrix and label"""

import time

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from common.logging_config import get_logger

log = get_logger("ml.features")

# the metastore is path based on this workspace so the trainer reads gold by storage path
GOLD_ENRICHED_PATH = "abfss://gold@sentinelstgrk1.dfs.core.windows.net/transaction_enriched"

LABEL_COLUMN = "is_fraud"

# numeric signals already engineered upstream in the gold layer plus the derived hour
NUMERIC_FEATURES = [
    "amount",
    "amount_zscore_30d",
    "txn_count_1h",
    "velocity_score",
    "peer_deviation",
    "txn_hour",
]

# low cardinality categoricals that become one hot columns in the matrix
CATEGORICAL_FEATURES = [
    "channel",
    "risk_segment",
]


def load_enriched(spark: SparkSession) -> DataFrame:
    """read the enriched gold table from storage because table names are unavailable here"""
    return spark.read.format("delta").load(GOLD_ENRICHED_PATH)


def add_derived_columns(df: DataFrame) -> DataFrame:
    """expose time of day from the raw event timestamp since hour carries fraud signal"""
    return df.withColumn("txn_hour", F.hour(F.col("txn_ts")))


def to_training_frame(spark: SparkSession) -> pd.DataFrame:
    """collect the small dataset into one pandas frame so the trainer can split it in driver memory"""
    started = time.time()

    enriched = add_derived_columns(load_enriched(spark))

    selected = enriched.select(
        *NUMERIC_FEATURES,
        *CATEGORICAL_FEATURES,
        LABEL_COLUMN,
    )

    pdf = selected.toPandas()

    # closed customers drop to null risk_segment through the gold dimension join so fill before encoding
    pdf["risk_segment"] = pdf["risk_segment"].fillna("unknown")

    # the classifier needs an integer target not a boolean
    pdf[LABEL_COLUMN] = pdf[LABEL_COLUMN].astype(int)

    log.info(
        "feature frame built",
        extra={"rows_in": len(pdf), "duration_ms": int((time.time() - started) * 1000)},
    )
    return pdf


def build_feature_matrix(pdf: pd.DataFrame):
    """separate the encoded feature matrix from the label so training and scoring share one definition"""
    features = pd.get_dummies(
        pdf[NUMERIC_FEATURES + CATEGORICAL_FEATURES],
        columns=CATEGORICAL_FEATURES,
    )
    label = pdf[LABEL_COLUMN]
    return features, label, list(features.columns)
