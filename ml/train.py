"""train the fraud classifier and record the run so iterations stay comparable in one experiment"""

import time

import mlflow
import mlflow.xgboost
from sklearn.metrics import precision_score, recall_score, roc_auc_score
from sklearn.model_selection import train_test_split
from xgboost import XGBClassifier
from pyspark.sql import SparkSession

from pipeline.common.logging_config import get_logger
from ml.features import to_training_frame, build_feature_matrix

log = get_logger("ml.train")

# every run lands under one workspace experiment so metrics compare across retrains
EXPERIMENT_PATH = "/Users/sitharamkalicheti@zohomail.com/sentinel-fraud"

# held out fraction and seed are fixed so the split and the reported metrics reproduce
TEST_SIZE = 0.2
RANDOM_STATE = 42

# precision and recall depend on where the cutoff sits so it is stated not implied
DECISION_THRESHOLD = 0.5


def build_model(scale_pos_weight: float) -> XGBClassifier:
    """shallow lightly sampled trees keep the model from memorising a tiny fraud set"""
    return XGBClassifier(
        n_estimators=100,
        max_depth=3,
        learning_rate=0.1,
        subsample=0.8,
        colsample_bytree=0.8,
        scale_pos_weight=scale_pos_weight,
        eval_metric="logloss",
        random_state=RANDOM_STATE,
    )


def train(spark: SparkSession) -> dict:
    """fit on gold features and log params and holdout metrics to mlflow for later promotion"""
    started = time.time()

    pdf = to_training_frame(spark)
    features, label, feature_names = build_feature_matrix(pdf)

    # xgboost expects a numeric matrix and reads nan as missing so cast once up front
    features = features.astype("float64")

    x_train, x_test, y_train, y_test = train_test_split(
        features,
        label,
        test_size=TEST_SIZE,
        stratify=label,
        random_state=RANDOM_STATE,
    )

    # the rare fraud class is upweighted so the model does not collapse to always legitimate
    negatives = int((y_train == 0).sum())
    positives = int((y_train == 1).sum())
    scale_pos_weight = negatives / positives if positives else 1.0

    model = build_model(scale_pos_weight)

    mlflow.set_experiment(EXPERIMENT_PATH)
    mlflow.xgboost.autolog()

    with mlflow.start_run(run_name="xgboost-fraud-v1"):
        model.fit(x_train, y_train)

        # ranking quality reads from the probability while precision and recall read from the label
        proba = model.predict_proba(x_test)[:, 1]
        preds = (proba >= DECISION_THRESHOLD).astype(int)

        precision = precision_score(y_test, preds, zero_division=0)
        recall = recall_score(y_test, preds, zero_division=0)
        roc_auc = roc_auc_score(y_test, proba)

        mlflow.log_param("features", ", ".join(feature_names))
        mlflow.log_param("decision_threshold", DECISION_THRESHOLD)
        mlflow.log_param("n_train", len(x_train))
        mlflow.log_param("n_test", len(x_test))
        mlflow.log_param("fraud_train", positives)
        mlflow.log_param("fraud_test", int((y_test == 1).sum()))

        mlflow.log_metric("test_precision", precision)
        mlflow.log_metric("test_recall", recall)
        mlflow.log_metric("test_roc_auc", roc_auc)

    log.info(
        "training complete",
        extra={"rows_in": len(features), "duration_ms": int((time.time() - started) * 1000)},
    )
    return {"precision": precision, "recall": recall, "roc_auc": roc_auc}
