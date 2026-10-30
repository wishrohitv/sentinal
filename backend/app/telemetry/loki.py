import json
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx

from app.config import settings
from app.telemetry.base import TelemetryError


class LokiAdapter:
    def __init__(self, base_url: str | None = None, transport: httpx.AsyncBaseTransport | None = None) -> None:
        self.base_url = (base_url or settings.loki_url).rstrip("/")
        self.timeout = settings.telemetry_request_timeout_seconds
        self.transport = transport

    async def query_logs(
        self,
        *,
        minutes: int = 60,
        severity: str | None = None,
        text: str | None = None,
        limit: int = 100,
        around: datetime | None = None,
    ) -> dict[str, Any]:
        query = f"{{service={json.dumps(settings.monitored_service_id)}}}"
        if severity:
            query += f' |~ "(?i){re_escape(severity)}"'
        if text:
            query += f" |= {json.dumps(text)}"
        if around:
            start = around - timedelta(minutes=minutes / 2)
            end = around + timedelta(minutes=minutes / 2)
        else:
            end = datetime.now(timezone.utc)
            start = end - timedelta(minutes=minutes)
        params = {
            "query": query,
            "start": str(int(start.timestamp() * 1_000_000_000)),
            "end": str(int(end.timestamp() * 1_000_000_000)),
            "limit": min(max(limit, 1), 500),
            "direction": "backward",
        }
        try:
            async with httpx.AsyncClient(timeout=self.timeout, transport=self.transport) as client:
                response = await client.get(f"{self.base_url}/loki/api/v1/query_range", params=params)
                response.raise_for_status()
                body = response.json()
        except (httpx.HTTPError, ValueError) as exc:
            raise TelemetryError(f"Loki query failed: {type(exc).__name__}") from exc
        if not isinstance(body, dict) or body.get("status") != "success":
            raise TelemetryError("Loki returned an unsuccessful query response")
        data = body.get("data")
        streams = data.get("result") if isinstance(data, dict) else None
        if not isinstance(streams, list):
            raise TelemetryError("Loki query result is malformed")
        logs: list[dict[str, Any]] = []
        for stream in streams:
            labels = stream.get("stream", {})
            for value in stream.get("values", []):
                if not isinstance(value, list) or len(value) != 2:
                    continue
                timestamp_ns, raw_line = value
                try:
                    structured = json.loads(raw_line)
                    if not isinstance(structured, dict):
                        structured = {"message": str(raw_line)}
                except (json.JSONDecodeError, TypeError):
                    structured = {"message": str(raw_line)}
                logs.append({
                    "id": f"loki:{timestamp_ns}:{len(logs)}",
                    "timestamp": ns_to_iso(timestamp_ns),
                    "level": str(structured.get("level", labels.get("level", "UNKNOWN"))),
                    "service": str(structured.get("service", labels.get("service", "monitored-api"))),
                    "event": structured.get("event", labels.get("event")),
                    "message": str(structured.get("message", raw_line))[:2000],
                    "fields": {key: val for key, val in structured.items()
                               if key not in {"timestamp", "level", "service", "event", "message"}},
                    "raw": str(raw_line)[:2000],
                })
        logs.sort(key=lambda row: row["timestamp"], reverse=True)
        return {
            "source": "loki",
            "query": query,
            "count": len(logs[:params["limit"]]),
            "logs": logs[:params["limit"]],
            "observed_at": datetime.now(timezone.utc).isoformat(),
        }

    async def health(self) -> dict[str, Any]:
        try:
            async with httpx.AsyncClient(timeout=self.timeout, transport=self.transport) as client:
                response = await client.get(f"{self.base_url}/ready")
                response.raise_for_status()
            return {"available": True, "source": "loki"}
        except httpx.HTTPError as exc:
            return {"available": False, "source": "loki", "error": type(exc).__name__}


def ns_to_iso(value: Any) -> str:
    try:
        return datetime.fromtimestamp(int(value) / 1_000_000_000, timezone.utc).isoformat()
    except (ValueError, TypeError, OverflowError):
        return datetime.now(timezone.utc).isoformat()


def re_escape(value: str) -> str:
    # LogQL regex literal is escaped before it reaches the query parser.
    return "".join("\\" + char if char in r".\+*?()|[]{}^$" else char for char in value)
