import asyncio
from datetime import datetime, timedelta, timezone
import math
from typing import Any

import httpx

from app.config import settings
from app.telemetry.base import TelemetryError


PROMQL = {
    "http_request_rate": 'sum(rate(http_requests_total{job="monitored-api"}[30s]))',
    "http_5xx_rate": 'sum(rate(http_errors_total{job="monitored-api"}[30s])) / clamp_min(sum(rate(http_requests_total{job="monitored-api"}[30s])), 0.000001)',
    "request_latency_ms": 'histogram_quantile(0.95, sum by (le) (rate(http_request_duration_seconds_bucket{job="monitored-api"}[30s]))) * 1000',
    "db_pool_size": 'max(db_pool_size{job="monitored-api"})',
    "db_pool_checked_out": 'max(db_pool_connections_checked_out{job="monitored-api"})',
    "db_pool_utilization": 'max(db_pool_connections_checked_out{job="monitored-api"}) / clamp_min(max(db_pool_size{job="monitored-api"}), 1)',
    "db_connection_timeout_rate": 'sum(rate(db_connection_timeouts_total{job="monitored-api"}[30s]))',
    "process_cpu_cores": 'rate(process_cpu_seconds_total{job="monitored-api"}[1m])',
    "process_memory_bytes": 'max(process_resident_memory_bytes{job="monitored-api"})',
}


class PrometheusAdapter:
    def __init__(self, base_url: str | None = None, transport: httpx.AsyncBaseTransport | None = None) -> None:
        self.base_url = (base_url or settings.prometheus_url).rstrip("/")
        self.timeout = settings.telemetry_request_timeout_seconds
        self.transport = transport

    async def _request(self, client: httpx.AsyncClient, endpoint: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        try:
            response = await client.get(f"{self.base_url}{endpoint}", params=params)
            response.raise_for_status()
            body = response.json()
        except (httpx.HTTPError, ValueError) as exc:
            raise TelemetryError(f"Prometheus request failed: {type(exc).__name__}") from exc
        if not isinstance(body, dict) or body.get("status") != "success" or not isinstance(body.get("data"), dict):
            raise TelemetryError("Prometheus returned an invalid or unsuccessful response")
        return body["data"]

    async def query_scalar(self, client: httpx.AsyncClient, expression: str) -> float | None:
        data = await self._request(client, "/api/v1/query", {"query": expression})
        results = data.get("result")
        if not isinstance(results, list):
            raise TelemetryError("Prometheus instant-query result is malformed")
        if not results:
            return None
        try:
            value = float(results[0]["value"][1])
        except (KeyError, IndexError, TypeError, ValueError) as exc:
            raise TelemetryError("Prometheus returned a malformed metric sample") from exc
        return value if math.isfinite(value) else None

    async def current_metrics(self) -> dict[str, Any]:
        async with httpx.AsyncClient(timeout=self.timeout, transport=self.transport) as client:
            values = await asyncio.gather(*(self.query_scalar(client, query) for query in PROMQL.values()))
        return {
            "source": "prometheus",
            "observed_at": datetime.now(timezone.utc).isoformat(),
            **dict(zip(PROMQL.keys(), values, strict=True)),
        }

    async def metric_samples(self, minutes: int, limit: int | None = None) -> dict[str, Any]:
        max_samples = limit or settings.max_metric_samples
        end = datetime.now(timezone.utc)
        start = end - timedelta(minutes=minutes)
        step = max(5, min(60, int(minutes * 60 / max_samples)))
        params_base = {
            "start": start.timestamp(),
            "end": end.timestamp(),
            "step": step,
        }
        async with httpx.AsyncClient(timeout=self.timeout, transport=self.transport) as client:
            async def one(name: str, expression: str) -> tuple[str, list[dict[str, Any]]]:
                data = await self._request(client, "/api/v1/query_range", {**params_base, "query": expression})
                result = data.get("result")
                if not isinstance(result, list):
                    raise TelemetryError("Prometheus range-query result is malformed")
                points: list[dict[str, Any]] = []
                for series in result:
                    if not isinstance(series, dict) or not isinstance(series.get("values", []), list):
                        raise TelemetryError("Prometheus range-query series is malformed")
                    for sample in series.get("values", []):
                        try:
                            timestamp, raw_value = sample
                            value = float(raw_value)
                            if math.isfinite(value):
                                points.append({
                                    "value": value,
                                    "timestamp": datetime.fromtimestamp(float(timestamp), timezone.utc).isoformat(),
                                })
                        except (TypeError, ValueError, OverflowError):
                            continue
                points.sort(key=lambda point: point["timestamp"])
                return name, points[-max_samples:]

            metrics = await asyncio.gather(*(one(name, query) for name, query in PROMQL.items()))
        aggregates: dict[str, Any] = {}
        for name, points in metrics:
            values = [point["value"] for point in points]
            aggregates[name] = {
                "count": len(points),
                "min": min(values) if values else None,
                "max": max(values) if values else None,
                "avg": sum(values) / len(values) if values else None,
                "latest": values[-1] if values else None,
                "samples": points,
            }
        return {
            "source": "prometheus",
            "window_minutes": minutes,
            "observed_at": end.isoformat(),
            "metrics": aggregates,
        }

    async def health(self) -> dict[str, Any]:
        try:
            async with httpx.AsyncClient(timeout=self.timeout, transport=self.transport) as client:
                response = await client.get(f"{self.base_url}/api/v1/targets", params={"state": "active"})
                response.raise_for_status()
                body = response.json()
            if not isinstance(body, dict) or body.get("status") != "success":
                raise TelemetryError("Prometheus target status response is malformed")
            data = body.get("data")
            targets = data.get("activeTargets") if isinstance(data, dict) else None
            if not isinstance(targets, list):
                raise TelemetryError("Prometheus target status response is malformed")
            target = next((item for item in targets if item.get("labels", {}).get("job") == "monitored-api"), None)
            if target is None:
                return {"available": False, "source": "prometheus", "error": "monitored-api scrape target is missing"}
            healthy = target.get("health") == "up"
            return {"available": healthy, "source": "prometheus", "target_health": target.get("health"),
                    "last_error": target.get("lastError") or None}
        except (httpx.HTTPError, ValueError) as exc:
            return {"available": False, "source": "prometheus", "error": type(exc).__name__}
        except TelemetryError as exc:
            return {"available": False, "source": "prometheus", "error": str(exc)}
