from datetime import datetime, timedelta, timezone
from uuid import uuid4
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from app.config import settings
from app.models import Incident, MetricSample
from app.simulator.state import SERVICE_ID, current_metrics
from app.services.timeline import add_event

async def detect_incident(session: AsyncSession) -> Incident | None:
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
        Incident.status.in_(["active", "investigating", "investigated", "awaiting_approval", "remediating"])).limit(1))
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
