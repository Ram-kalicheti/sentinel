import json
import logging
import sys
from datetime import datetime, timezone

_METRIC_FIELDS = (
    "batch_id",
    "rows_in",
    "rows_passed",
    "rows_deadlettered",
    "duration_ms",
    "attempt",
)


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "stage": getattr(record, "stage", None),
            "message": record.getMessage(),
        }
        # batch metrics ride on the record so a log line can be filtered without parsing prose
        for field in _METRIC_FIELDS:
            value = getattr(record, field, None)
            if value is not None:
                payload[field] = value
        if record.exc_info:
            payload["error"] = self.formatException(record.exc_info)
        return json.dumps(payload)


def get_logger(stage: str) -> logging.LoggerAdapter:
    logger = logging.getLogger(stage)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        # propagate off so the databricks root handler does not re-emit a plain duplicate line
        logger.propagate = False
    return logging.LoggerAdapter(logger, {"stage": stage})
