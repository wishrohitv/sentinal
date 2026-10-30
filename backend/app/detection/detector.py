from datetime import datetime, timedelta, timezone
from uuid import uuid4
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from app.config import settings
from app.models import Incident, MetricSample, Service
from app.simulator.state import SERVICE_ID, current_metrics
from app.services.timeline import add_event
from app.telemetry.prometheus import PrometheusAdapter

async def detect_incident(session: AsyncSession) -> Incident | None:
    if settings.telemetry_provider == "prometheus_loki":
        return await detect_monitored_incident(session)
    latest = await current_metrics(session)
    start = datetime.now(timezone.utc) - timedelta(minutes=settings.sentinel_detection_window_minutes)
    rows = list((await session.scalars(select(MetricSample).where(
        MetricSample.service_id == SERVICE_ID,
        MetricSample.timestamp >= start,
        MetricSample.name.in_(["http_5xx_rate", "db_pool_utilization", "db_connection_timeout_rate", "worker_healthy"])
    ).order_by(MetricSample.timestamp, MetricSample.id))).all())
    by_name: dict[str, list[float]] = {}
    for row in rows:
        by_name.setdefault(row.name, []).append(row.value)
    window = {name: sum(values) / len(values) for name, values in by_name.items() if values}
    error_rate = window.get("http_5xx_rate", latest.get("http_5xx_rate", 0.0))
    pool_utilization = max(by_name.get("db_pool_utilization", [latest.get("db_pool_utilization", 0.0)]))
    timeout_rate = window.get("db_connection_timeout_rate", latest.get("db_connection_timeout_rate", 0.0))
    worker_healthy = latest.get("worker_healthy", 1.0)
    reasons = []
    if error_rate > settings.sentinel_5xx_threshold:
        reasons.append(f"rolling HTTP 5xx rate {error_rate:.0%} exceeds {settings.sentinel_5xx_threshold:.0%}")
    if pool_utilization > settings.sentinel_db_pool_threshold:
        reasons.append(f"rolling database pool utilization {pool_utilization:.0%} exceeds {settings.sentinel_db_pool_threshold:.0%}")
    if timeout_rate > 0:
        reasons.append(f"database connection timeouts observed in {settings.sentinel_detection_window_minutes}m window")
    if worker_healthy < 1.0:
        reasons.append("background worker process is unhealthy or down")
    if not reasons:
        return None
    existing = await session.scalar(select(Incident).where(Incident.service_id == SERVICE_ID,
        Incident.status.in_(["active", "investigating", "investigated", "awaiting_approval", "remediating", "escalated"])).limit(1))
    if existing:
        await session.commit()
        return existing
    incident_metrics = {**latest, "detection_window_minutes": settings.sentinel_detection_window_minutes,
        "rolling_http_5xx_rate": error_rate, "rolling_db_pool_utilization": pool_utilization,
        "rolling_db_connection_timeout_rate": timeout_rate}
    is_worker = worker_healthy < 1.0
    incident = Incident(id=str(uuid4()), service_id=SERVICE_ID,
        title="Worker process failure" if is_worker else "Database connection pool exhaustion",
        severity="critical" if is_worker or error_rate >= 0.5 else "major",
        status="active", trigger="; ".join(reasons), trigger_metrics=incident_metrics)
    session.add(incident)
    await session.flush()
    await add_event(session, incident.id, "INCIDENT_DETECTED", {"trigger": incident.trigger, "metrics": incident_metrics})
    await session.commit()
    return incident


async def detect_monitored_incident(session: AsyncSession) -> Incident | None:
    latest = await PrometheusAdapter().current_metrics()
    service_id = settings.monitored_service_id
    service = await session.get(Service, service_id)
    if service is None:
        service = Service(id=service_id, name="Monitored Orders API", status="unknown")
        session.add(service)
        await session.flush()

    observed_at = datetime.now(timezone.utc)
    sample_names = (
        "http_request_rate", "http_5xx_rate", "request_latency_ms", "db_pool_size",
        "db_pool_checked_out", "db_pool_utilization", "db_connection_timeout_rate",
        "process_cpu_cores", "process_memory_bytes",
    )
    for name in sample_names:
        value = latest.get(name)
        if isinstance(value, (int, float)):
            session.add(MetricSample(service_id=service_id, name=name, value=float(value), timestamp=observed_at))
    if service is not None:
        ready = latest.get("db_pool_size") is not None
        service.status = "healthy" if ready else "unknown"

    error_rate = latest.get("http_5xx_rate")
    pool_utilization = latest.get("db_pool_utilization")
    timeout_rate = latest.get("db_connection_timeout_rate")
    latency_ms = latest.get("request_latency_ms")
    available = [value for value in (error_rate, pool_utilization, timeout_rate) if isinstance(value, (int, float))]
    if not available:
        await session.commit()
        return None

    saturated = isinstance(pool_utilization, (int, float)) and pool_utilization >= settings.sentinel_db_pool_threshold
    errors_high = isinstance(error_rate, (int, float)) and error_rate >= settings.http_error_rate_threshold
    timeouts_high = isinstance(timeout_rate, (int, float)) and timeout_rate >= settings.db_timeout_rate_threshold
    reasons: list[str] = []
    if saturated:
        reasons.append(f"database pool utilization {pool_utilization:.0%} meets/exceeds {settings.sentinel_db_pool_threshold:.0%}")
    if errors_high:
        reasons.append(f"HTTP 5xx ratio {error_rate:.0%} meets/exceeds {settings.http_error_rate_threshold:.0%}")
    if timeouts_high:
        reasons.append(f"database checkout timeout rate {timeout_rate:.3f}/s meets/exceeds {settings.db_timeout_rate_threshold:.3f}/s")
    if isinstance(latency_ms, (int, float)) and latency_ms >= 500:
        reasons.append(f"p95 request latency is elevated at {latency_ms:.0f} ms")

    # Pool exhaustion is correlated from saturation plus a real timeout or elevated 5xx signal.
    pool_incident = saturated and (timeouts_high or errors_high)
    generic_error_incident = errors_high and (timeouts_high or (isinstance(latency_ms, (int, float)) and latency_ms >= 500))
    if not pool_incident and not generic_error_incident:
        await session.commit()
        return None

    existing = await session.scalar(select(Incident).where(
        Incident.service_id == service_id,
        Incident.status.in_(["active", "investigating", "investigated", "awaiting_approval", "remediating", "escalated"]),
    ).limit(1))
    if existing:
        await session.commit()
        return existing

    trigger_metrics = {
        **latest,
        "telemetry_source": "prometheus",
        "detection_window_seconds": 30,
    }
    title = "Database connection pool exhaustion" if pool_incident else "Elevated monitored API error rate"
    incident = Incident(
        id=str(uuid4()), service_id=service_id, title=title,
        severity="critical" if (timeouts_high and errors_high) else "major",
        status="active", trigger="; ".join(reasons), trigger_metrics=trigger_metrics,
    )
    session.add(incident)
    await session.flush()
    await add_event(session, incident.id, "INCIDENT_DETECTED", {
        "trigger": incident.trigger, "metrics": trigger_metrics, "telemetry_source": "prometheus",
    })
    await session.commit()
    return incident
