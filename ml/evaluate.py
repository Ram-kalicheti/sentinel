"""evaluate the fraud model on stable cross validated metrics and gate promotion on what it earns"""

import time

import mlflow
import mlflow.xgboost
import numpy as np
from mlflow.models import infer_signature
from mlflow.tracking import MlflowClient
from sklearn.metrics import confusion_matrix, precision_score, recall_score, roc_auc_score
from sklearn.model_selection import StratifiedKFold, cross_val_predict
from pyspark.sql import SparkSession

from pipeline.common.logging_config import get_logger
from ml.features import to_training_frame, build_feature_matrix
from ml.train import build_model, EXPERIMENT_PATH, RANDOM_STATE, DECISION_THRESHOLD

log = get_logger("ml.evaluate")

# unity catalog registers models under a three level name and marks the serving version with an alias
REGISTERED_MODEL = "sentinel_adb.sentinel.fraud_classifier"
CHAMPION_ALIAS = "champion"

# a promoted model must clear random by a margin and beat the heuristic already serving in the mart
MIN_ROC_AUC = 0.60

CV_FOLDS = 5


def population_stability_index(reference, current, bins: int = 10) -> float:
    """quantify input drift so a shifting distribution is caught before it silently degrades scoring"""
    edges = np.unique(np.quantile(reference, np.linspace(0, 1, bins + 1)))
    ref_counts, _ = np.histogram(reference, bins=edges)
    cur_counts, _ = np.histogram(current, bins=edges)
    ref_pct = np.clip(ref_counts / max(ref_counts.sum(), 1), 1e-6, None)
    cur_pct = np.clip(cur_counts / max(cur_counts.sum(), 1), 1e-6, None)
    return float(np.sum((cur_pct - ref_pct) * np.log(cur_pct / ref_pct)))


def heuristic_roc_auc(features, label) -> float:
    """score the existing rule so the model has to justify itself against what already ships"""
    # the mart heuristic leans on amount deviation so the absolute z score stands in as its proxy
    proxy = features["amount_zscore_30d"].abs().fillna(0.0)
    return roc_auc_score(label, proxy)


def evaluate(spark: SparkSession) -> dict:
    """cross validate the classifier and set the champion alias only when the metrics earn it"""
    started = time.time()

    pdf = to_training_frame(spark)
    features, label, feature_names = build_feature_matrix(pdf)
    features = features.astype("float64")

    negatives = int((label == 0).sum())
    positives = int((label == 1).sum())
    scale_pos_weight = negatives / positives if positives else 1.0

    model = build_model(scale_pos_weight)

    # out of fold probabilities give every fraud row a held out prediction so the score does not swing
    splitter = StratifiedKFold(n_splits=CV_FOLDS, shuffle=True, random_state=RANDOM_STATE)
    oof_proba = cross_val_predict(model, features, label, cv=splitter, method="predict_proba")[:, 1]
    oof_preds = (oof_proba >= DECISION_THRESHOLD).astype(int)

    cv_roc_auc = roc_auc_score(label, oof_proba)
    cv_precision = precision_score(label, oof_preds, zero_division=0)
    cv_recall = recall_score(label, oof_preds, zero_division=0)
    matrix = confusion_matrix(label, oof_preds)

    base_auc = heuristic_roc_auc(features, label)

    # drift is measured between two halves of the current data as a wired up demonstration
    half = len(features) // 2
    psi_amount = population_stability_index(
        features["amount"].iloc[:half].to_numpy(), features["amount"].iloc[half:].to_numpy()
    )
    psi_velocity = population_stability_index(
        features["velocity_score"].iloc[:half].to_numpy(),
        features["velocity_score"].iloc[half:].to_numpy(),
    )

    promote = cv_roc_auc >= MIN_ROC_AUC and cv_roc_auc > base_auc

    mlflow.set_experiment(EXPERIMENT_PATH)
    with mlflow.start_run(run_name="fraud-eval"):
        mlflow.log_param("min_roc_auc_gate", MIN_ROC_AUC)
        mlflow.log_param("promoted", promote)
        mlflow.log_metric("cv_roc_auc", cv_roc_auc)
        mlflow.log_metric("cv_precision", cv_precision)
        mlflow.log_metric("cv_recall", cv_recall)
        mlflow.log_metric("heuristic_roc_auc", base_auc)
        mlflow.log_metric("psi_amount", psi_amount)
        mlflow.log_metric("psi_velocity", psi_velocity)
        mlflow.log_metric("confusion_tn", int(matrix[0, 0]))
        mlflow.log_metric("confusion_fp", int(matrix[0, 1]))
        mlflow.log_metric("confusion_fn", int(matrix[1, 0]))
        mlflow.log_metric("confusion_tp", int(matrix[1, 1]))

        # the full data fit is the candidate that gets registered whatever the gate then decides
        model.fit(features, label)
        signature = infer_signature(features, model.predict(features))
        logged = mlflow.xgboost.log_model(
            model,
            artifact_path="model",
            signature=signature,
            registered_model_name=REGISTERED_MODEL,
        )

    version = logged.registered_model_version

    # registering proves the mechanism but only an earned model takes the serving alias
    if promote:
        MlflowClient().set_registered_model_alias(REGISTERED_MODEL, CHAMPION_ALIAS, version)

    log.info(
        "evaluation complete",
        extra={"rows_in": len(features), "duration_ms": int((time.time() - started) * 1000)},
    )
    return {
        "cv_roc_auc": cv_roc_auc,
        "cv_precision": cv_precision,
        "cv_recall": cv_recall,
        "heuristic_roc_auc": base_auc,
        "psi_amount": psi_amount,
        "psi_velocity": psi_velocity,
        "registered_version": version,
        "promoted_to_champion": promote,
    }
