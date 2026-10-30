import json
from datetime import datetime, timezone

import httpx
import pytest

from app.telemetry.base import TelemetryError
from app.telemetry.loki import LokiAdapter
from app.telemetry.prometheus import PrometheusAdapter


@pytest.mark.asyncio
async def test_prometheus_scalar_parses_actual_api_value():
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/v1/query"
        return httpx.Response(200, json={
            "status": "success",
            "data": {"resultType": "vector", "result": [{"value": [1234, "0.975"]}]},
        })

    adapter = PrometheusAdapter("http://prometheus.test", transport=httpx.MockTransport(handler))
    async with httpx.AsyncClient(transport=adapter.transport) as client:
        assert await adapter.query_scalar(client, "db_pool_utilization") == 0.975


@pytest.mark.asyncio
async def test_prometheus_missing_series_is_none_not_simulator_data():
    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"status": "success", "data": {"result": []}})

    adapter = PrometheusAdapter("http://prometheus.test", transport=httpx.MockTransport(handler))
    async with httpx.AsyncClient(transport=adapter.transport) as client:
        assert await adapter.query_scalar(client, "missing_metric") is None


@pytest.mark.asyncio
async def test_prometheus_rejects_invalid_response():
    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"status": "error", "error": "bad query"})

    adapter = PrometheusAdapter("http://prometheus.test", transport=httpx.MockTransport(handler))
    async with httpx.AsyncClient(transport=adapter.transport) as client:
        with pytest.raises(TelemetryError, match="unsuccessful response"):
            await adapter.query_scalar(client, "bad query")


@pytest.mark.asyncio
async def test_loki_parses_json_and_preserves_non_json_lines():
    now_ns = str(int(datetime.now(timezone.utc).timestamp() * 1_000_000_000))
    structured = json.dumps({"timestamp": "2026-10-30T12:00:00Z", "level": "ERROR",
                             "service": "monitored-api", "event": "database_connection_timeout",
                             "message": "checkout timed out", "request_id": "r1"})

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/loki/api/v1/query_range"
        assert "service=\"monitored-api\"" in request.url.params["query"]
        return httpx.Response(200, json={"status": "success", "data": {"result": [{
            "stream": {"service": "monitored-api", "level": "ERROR"},
            "values": [[now_ns, structured], [now_ns, "not-json-but-still-a-log"]],
        }]}})

    adapter = LokiAdapter("http://loki.test", transport=httpx.MockTransport(handler))
    result = await adapter.query_logs(severity="ERROR", text="timeout", limit=10)
    assert result["source"] == "loki"
    assert result["count"] == 2
    assert result["logs"][0]["message"] in {"checkout timed out", "not-json-but-still-a-log"}
    assert all(record["timestamp"] for record in result["logs"])
