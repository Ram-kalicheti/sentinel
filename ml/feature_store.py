"""Online feature store backed by redis.

Per-customer slow features are batch-computed in the mart and lifted into redis
so scoring reads them without recomputation. Serving the last batch-built
aggregates trades some staleness for a fast lookup; a streaming update path
would remove the staleness.
"""
from __future__ import annotations

import argparse
import time

import pandas as pd
import redis

from pipeline.common.logging_config import get_logger

log = get_logger(__name__)

ONLINE_NUMERIC = ["amount_zscore_30d", "txn_count_1h", "velocity_score", "peer_deviation"]
ONLINE_CATEGORICAL = ["risk_segment"]
KEY_PREFIX = "feat:"
META_KEY = "feat:_meta"


def _key(customer_id: str) -> str:
    return f"{KEY_PREFIX}{customer_id}"


def load_online_frame(source_parquet: str) -> pd.DataFrame:
    df = pd.read_parquet(source_parquet)
    need = ["customer_id", "scored_ts", *ONLINE_NUMERIC, *ONLINE_CATEGORICAL]
    missing = [c for c in need if c not in df.columns]
    if missing:
        raise ValueError(f"online feature source missing columns: {missing}")
    df = df.sort_values("scored_ts").drop_duplicates("customer_id", keep="last")
    df["risk_segment"] = df["risk_segment"].fillna("unknown")  # parity with ml.features
    return df[["customer_id", *ONLINE_NUMERIC, *ONLINE_CATEGORICAL]]


def populate_redis(df: pd.DataFrame, redis_url: str) -> int:
    r = redis.from_url(redis_url, decode_responses=True)
    r.ping()  # fail fast if Redis is down
    pipe = r.pipeline()
    for row in df.itertuples(index=False):
        mapping = {c: str(getattr(row, c)) for c in (*ONLINE_NUMERIC, *ONLINE_CATEGORICAL)}
        pipe.hset(_key(row.customer_id), mapping=mapping)
    pipe.hset(META_KEY, mapping={"count": str(len(df)), "built_ts": str(int(time.time()))})
    pipe.execute()
    log.info("online feature store populated", extra={"batch_id": "feature_store", "rows_in": len(df)})
    return len(df)


def get_online_features(r: "redis.Redis", customer_id: str) -> dict | None:
    raw = r.hgetall(_key(customer_id))
    if not raw:
        return None
    out: dict = {}
    for c in ONLINE_NUMERIC:
        out[c] = float(raw.get(c, 0.0))
    for c in ONLINE_CATEGORICAL:
        out[c] = raw.get(c, "unknown")
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description="Populate Sentinel online feature store")
    ap.add_argument("--source", required=True, help="parquet exported from fct_transaction_health")
    ap.add_argument("--redis-url", default="redis://localhost:6379/0")
    args = ap.parse_args()
    df = load_online_frame(args.source)
    n = populate_redis(df, args.redis_url)
    print(f"populated {n} customers into {args.redis_url}")


if __name__ == "__main__":
    main()