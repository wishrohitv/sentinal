import contextvars
import json
import logging
import os
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler

from app.config import settings

request_id_context: contextvars.ContextVar[str] = contextvars.ContextVar("request_id", default="")


class JsonLogFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        row = {
            "timestamp": datetime.fromtimestamp(record.created, timezone.utc).isoformat(),
            "level": record.levelname,
            "service": settings.monitored_service_id,
            "event": getattr(record, "event", "application_log"),
            "message": record.getMessage(),
            "request_id": request_id_context.get() or None,
        }
        if record.exc_info:
            row["exception"] = self.formatException(record.exc_info)
        for field in ("path", "method", "status_code", "pool_size", "previous_pool_size", "new_pool_size", "held_seconds"): 
            value = getattr(record, field, None)
            if value is not None:
                row[field] = value
        return json.dumps(row, separators=(",", ":"), default=str)


def configure_logging(log_path: str) -> None:
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    formatter = JsonLogFormatter()
    handlers = [logging.StreamHandler(), RotatingFileHandler(log_path, maxBytes=10_000_000, backupCount=3, encoding="utf-8")]
    for handler in handlers:
        handler.setFormatter(formatter)
    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(logging.INFO)
    for handler in handlers:
        root.addHandler(handler)
