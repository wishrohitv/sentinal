from typing import Any

import httpx

from app.config import settings
from app.telemetry.base import TelemetryError


class MonitoredAppAdapter:
    def __init__(self, base_url: str | None = None, control_token: str | None = None) -> None:
        self.base_url = (base_url or settings.monitored_app_url).rstrip("/")
        self.control_token = settings.monitored_control_token if control_token is None else control_token
        self.timeout = settings.telemetry_request_timeout_seconds

    def _auth_headers(self) -> dict[str, str]:
        if not self.control_token:
            raise TelemetryError("Monitored application control token is not configured")
        return {"Authorization": f"Bearer {self.control_token}"}

    async def _get(self, path: str, *, authenticated: bool = False) -> dict[str, Any]:
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.get(
                    f"{self.base_url}{path}",
                    headers=self._auth_headers() if authenticated else None,
                )
                response.raise_for_status()
                body = response.json()
        except (httpx.HTTPError, ValueError) as exc:
            raise TelemetryError(f"Monitored application request failed: {type(exc).__name__}") from exc
        if not isinstance(body, dict):
            raise TelemetryError("Monitored application returned an invalid response")
        return body

    async def health(self) -> dict[str, Any]:
        try:
            health, readiness = await self._health_pair()
            return {
                "available": health.get("status") == "ok",
                "ready": readiness.get("status") == "ready",
                "service": health.get("service", settings.monitored_service_id),
                "version": health.get("version"),
                "source": "monitored_application",
            }
        except TelemetryError as exc:
            return {"available": False, "ready": False, "source": "monitored_application", "error": str(exc)}

    async def _health_pair(self) -> tuple[dict[str, Any], dict[str, Any]]:
        import asyncio

        return await asyncio.gather(self._get("/health"), self._get("/ready"))

    async def metadata(self) -> dict[str, Any]:
        return await self._get("/internal/metadata", authenticated=True)

    async def configuration_changes(self) -> dict[str, Any]:
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.get(
                    f"{self.base_url}/internal/configuration-changes",
                    headers=self._auth_headers(),
                )
                response.raise_for_status()
                records = response.json()
        except (httpx.HTTPError, ValueError) as exc:
            raise TelemetryError(f"Monitored configuration history request failed: {type(exc).__name__}") from exc
        if not isinstance(records, list):
            raise TelemetryError("Monitored application returned malformed configuration history")
        return {"source": "monitored_application", "changes": records}

    async def increase_pool_size(self, new_size: int) -> dict[str, Any]:
        if not 1 <= new_size <= settings.monitored_pool_max_size:
            raise ValueError(f"Local monitored pool size must be between 1 and {settings.monitored_pool_max_size}")
        try:
            async with httpx.AsyncClient(timeout=max(self.timeout, 20.0)) as client:
                response = await client.post(
                    f"{self.base_url}/internal/remediation/pool-size",
                    headers=self._auth_headers(),
                    json={"new_size": new_size},
                )
                response.raise_for_status()
                result = response.json()
        except httpx.HTTPStatusError as exc:
            message = "Monitored application rejected the controlled pool update"
            try:
                message = str(exc.response.json().get("detail", message))
            except ValueError:
                pass
            raise TelemetryError(message) from exc
        except (httpx.HTTPError, ValueError) as exc:
            raise TelemetryError(f"Monitored application remediation request failed: {type(exc).__name__}") from exc
        if not isinstance(result, dict) or result.get("status") != "pool_reconfigured":
            raise TelemetryError("Monitored application returned an invalid remediation result")
        return {"source": "monitored_application", **result}
