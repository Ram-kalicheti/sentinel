"""Real-time fraud scoring endpoint.

     Model and redis are initialised once at startup and reused per request so the
     warm path stays fast. The serving model loads by champion alias when one
     exists and otherwise by newest version, matching the writeback fallback so
     the api and the batch scorer never disagree on which model is live.
"""
from __future__ import annotations

import os
import time
from datetime import datetime

import mlflow
import pandas as pd
import redis
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from ml.feature_store import ONLINE_NUMERIC, get_online_features

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
MODEL_PATH = os.getenv("MODEL_PATH")
MODEL_NAME = os.getenv("MODEL_NAME", "sentinel_adb.sentinel.fraud_classifier")
# keep in sync with ml.train.DECISION_THRESHOLD (env-injected to keep the container standalone)
THRESHOLD = float(os.getenv("DECISION_THRESHOLD", "0.5"))

app = FastAPI(title="Sentinel Fraud Scorer", version="0.1.0")
_state: dict = {"model": None, "features": None, "redis": None, "model_ref": "uninitialized"}


class ScoreRequest(BaseModel):
    customer_id: str
    amount: float
    channel: str
    txn_ts: str
    transaction_id: str | None = None


class ScoreResponse(BaseModel):
    transaction_id: str | None
    fraud_score: float
    decision: str
    model_ref: str
    model_stage: str
    latency_ms: float


def _load_model():
    if MODEL_PATH:
        return mlflow.xgboost.load_model(MODEL_PATH), f"local:{MODEL_PATH}"
    client = mlflow.tracking.MlflowClient()
    try:
        client.get_model_version_by_alias(MODEL_NAME, "champion")
        return mlflow.xgboost.load_model(f"models:/{MODEL_NAME}@champion"), f"{MODEL_NAME}@champion"
    except Exception:  # no champion alias gate refused it; fall back to newest, as writeback does
        vers = client.search_model_versions(f"name='{MODEL_NAME}'")
        newest = max(vers, key=lambda v: int(v.version))
        return mlflow.xgboost.load_model(f"models:/{MODEL_NAME}/{newest.version}"), f"{MODEL_NAME}/v{newest.version}"


@app.on_event("startup")
def _startup():
    model, ref = _load_model()
    _state["model"] = model
    _state["model_ref"] = ref
    # feature_names_in_ is the single source of truth for train/serve column alignment
    _state["features"] = list(model.feature_names_in_)
    _state["redis"] = redis.from_url(REDIS_URL, decode_responses=True)
    _state["redis"].ping()


@app.get("/health")
def health():
    r = _state["redis"]
    redis_ok = False
    try:
        redis_ok = bool(r and r.ping())
    except Exception:
        redis_ok = False
    ok = _state["model"] is not None and redis_ok
    return {"status": "ok" if ok else "degraded", "model_ref": _state["model_ref"], "redis": redis_ok}


def _encode(req: ScoreRequest, online: dict) -> pd.DataFrame:
    txn_hour = datetime.fromisoformat(req.txn_ts).hour
    raw = {
        "amount": req.amount,
        "amount_zscore_30d": online["amount_zscore_30d"],
        "txn_count_1h": online["txn_count_1h"],
        "velocity_score": online["velocity_score"],
        "peer_deviation": online["peer_deviation"],
        "txn_hour": txn_hour,
        "channel": req.channel,
        "risk_segment": online.get("risk_segment", "unknown"),
    }
    df = pd.DataFrame([raw])
    df = pd.get_dummies(df, columns=["channel", "risk_segment"])  # default naming: channel_*, risk_segment_*
    # reindex to the training columns, unseen dummies become 0 so encoding never drifts
    return df.reindex(columns=_state["features"], fill_value=0)


@app.post("/score", response_model=ScoreResponse)
def score(req: ScoreRequest):
    t0 = time.perf_counter()
    r = _state["redis"]
    if r is None:
        raise HTTPException(503, "feature store unavailable")
    online = get_online_features(r, req.customer_id)
    if online is None:
        raise HTTPException(404, f"no online features for customer_id={req.customer_id}")
    X = _encode(req, online)
    proba = float(_state["model"].predict_proba(X)[0, 1])
    return ScoreResponse(
        transaction_id=req.transaction_id,
        fraud_score=round(proba, 6),
        decision="review" if proba >= THRESHOLD else "pass",
        model_ref=_state["model_ref"],
        model_stage="unpromoted-dev-v1",  # no champion alias exists
        latency_ms=round((time.perf_counter() - t0) * 1000, 3),
    )