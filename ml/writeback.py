"""score the serving mart and merge the model distress score back so predictions reach the gold contract"""

import time

import mlflow
import mlflow.xgboost
import pandas as pd
from mlflow.tracking import MlflowClient
from pyspark.sql import SparkSession

from pipeline.common.logging_config import get_logger
from ml.features import NUMERIC_FEATURES, CATEGORICAL_FEATURES

log = get_logger("ml.writeback")

MODEL_NAME = "sentinel_adb.sentinel.fraud_classifier"
CHAMPION_ALIAS = "champion"
MART = "sentinel_adb.sentinel.fct_transaction_health"

# the mart already carries txn_hour so scoring reuses the exact training feature columns
SCORE_COLUMNS = NUMERIC_FEATURES + CATEGORICAL_FEATURES


def load_serving_model():
    """prefer the promoted champion but fall back to the newest version so the merge can still run"""
    client = MlflowClient()
    try:
        client.get_model_version_by_alias(MODEL_NAME, CHAMPION_ALIAS)
        return mlflow.xgboost.load_model(f"models:/{MODEL_NAME}@{CHAMPION_ALIAS}"), CHAMPION_ALIAS
    except Exception:
        latest = max(int(v.version) for v in client.search_model_versions(f"name='{MODEL_NAME}'"))
        return mlflow.xgboost.load_model(f"models:/{MODEL_NAME}/{latest}"), f"version-{latest}"


def resolve_expected_columns(spark: SparkSession, model) -> list:
    """align scoring columns to what the model trained on so encoding never silently drifts"""
    trained = list(getattr(model, "feature_names_in_", []))
    if trained:
        return trained
    from ml.features import to_training_frame, build_feature_matrix

    _, _, names = build_feature_matrix(to_training_frame(spark))
    return names


def build_scoring_matrix(mart_pdf: pd.DataFrame, expected: list) -> pd.DataFrame:
    """encode the mart rows into the exact training columns so the model sees the shape it learned on"""
    frame = mart_pdf.copy()
    frame["risk_segment"] = frame["risk_segment"].fillna("unknown")
    encoded = pd.get_dummies(frame[SCORE_COLUMNS], columns=CATEGORICAL_FEATURES)
    return encoded.reindex(columns=expected, fill_value=0.0).astype("float64")


def writeback(spark: SparkSession) -> dict:
    """merge model scores into the mart contract column keyed on the surrogate so replays stay safe"""
    started = time.time()

    model, source = load_serving_model()
    expected = resolve_expected_columns(spark, model)

    mart_pdf = spark.table(MART).select("transaction_health_sk", *SCORE_COLUMNS).toPandas()

    matrix = build_scoring_matrix(mart_pdf, expected)
    scores = model.predict_proba(matrix)[:, 1]

    updates = mart_pdf[["transaction_health_sk"]].copy()
    updates["model_distress_score"] = scores.astype(float)
    spark.createDataFrame(updates).createOrReplaceTempView("score_updates")

    # update only merge on the surrogate key so a rerun rewrites the same rows with the same values
    spark.sql(
        f"""
        MERGE INTO {MART} t
        USING score_updates u
        ON t.transaction_health_sk = u.transaction_health_sk
        WHEN MATCHED THEN UPDATE SET t.model_distress_score = u.model_distress_score
        """
    )

    scored = spark.table(MART).where("model_distress_score is not null").count()

    log.info(
        "writeback complete",
        extra={"rows_in": len(updates), "duration_ms": int((time.time() - started) * 1000)},
    )
    return {
        "model_source": source,
        "rows_scored": len(updates),
        "non_null_after": scored,
        "score_checksum": round(float(scores.sum()), 6),
    }
