from dataclasses import dataclass
from datetime import datetime, timezone
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession
from app.models import Deployment, LogRecord, MetricSample, Service

SERVICE_ID = "demo-api"

@dataclass
class SimulatorState:
    failed: bool = False
    pool_limit: int = 10
    worker_healthy: bool = True
    deployment_version: str = "1.8.2"
    previous_pool_limit: int = 10
    requests: int = 0
    errors: int = 0
    timeouts: int = 0
    queue_backlog: int = 0
    failed_jobs: int = 0

state = SimulatorState()


def now() -> datetime:
    return datetime.now(timezone.utc)


async def initialize_simulator(session: AsyncSession) -> None:
    service = await session.get(Service, SERVICE_ID)
    if not service:
        session.add(Service(id=SERVICE_ID, name="Sentinel Demo API", status="healthy"))
        await session.flush()
        session.add(Deployment(service_id=SERVICE_ID, version=state.deployment_version,
                               configuration={"db_pool_limit": state.pool_limit, "worker_enabled": True}))
        await session.flush()
    await generate_workload(session, 20)
    await session.commit()


async def generate_workload(session: AsyncSession, count: int = 20) -> dict[str, float]:
    timestamp = now()
    failures = max(1, int(count * 0.65)) if state.failed and count else 0
    timeouts = max(1, int(count * 0.55)) if state.failed and count else 0
    state.requests += count
    state.errors += failures
    state.timeouts += timeouts
    utilization = 0.99 if state.failed else 0.35
    latency = 850.0 if state.failed else 42.0
    error_rate = failures / count if count else 0.0
    timeout_rate = timeouts / count if count else 0.0
    metrics = {
        "http_requests_total": float(state.requests),
        "http_5xx_rate": error_rate,
        "request_latency_ms": latency,
        "db_pool_utilization": utilization,
        "db_connection_timeouts_total": float(state.timeouts),
        "db_connection_timeout_rate": timeout_rate,
        "worker_healthy": 1.0 if state.worker_healthy else 0.0,
        "worker_queue_backlog": float(state.queue_backlog),
        "failed_jobs_total": float(state.failed_jobs),
    }
    for name, value in metrics.items():
        session.add(MetricSample(service_id=SERVICE_ID, name=name, value=value, timestamp=timestamp))
    message = ("database connection acquisition timed out" if state.failed
               else "request completed successfully")
    severity = "ERROR" if state.failed else "INFO"
    for index in range(min(count, 50)):
        session.add(LogRecord(service_id=SERVICE_ID, severity=severity, message=message,
                              attributes={"request_index": index, "path": "/api/request",
                                          "db_pool_limit": state.pool_limit,
                                          "deployment": state.deployment_version}, timestamp=timestamp))
    service = await session.get(Service, SERVICE_ID)
    if service:
        service.status = "degraded" if state.failed or not state.worker_healthy else "healthy"
    await session.flush()
    return metrics


async def inject_pool_exhaustion(session: AsyncSession) -> dict[str, float]:
    if not state.failed:
        state.previous_pool_limit = state.pool_limit
        state.failed = True
        state.pool_limit = 2
        state.deployment_version = "1.8.3"
        session.add(Deployment(service_id=SERVICE_ID, version=state.deployment_version,
                               configuration={"db_pool_limit": 2, "worker_enabled": True}))
        await session.flush()
    return await generate_workload(session, 100)


async def recover_pool(session: AsyncSession, new_limit: int) -> None:
    state.pool_limit = new_limit
    state.failed = False
    state.deployment_version = "1.8.4"
    session.add(Deployment(service_id=SERVICE_ID, version=state.deployment_version,
                           configuration={"db_pool_limit": new_limit, "worker_enabled": True}))
    await session.flush()


async def reset_simulator(session: AsyncSession) -> dict[str, float]:
    state.failed = False
    state.pool_limit = 10
    state.worker_healthy = True
    state.deployment_version = "1.8.2"
    state.requests = state.errors = state.timeouts = 0
    state.queue_backlog = state.failed_jobs = 0
    await session.execute(delete(MetricSample).where(MetricSample.service_id == SERVICE_ID))
    await session.execute(delete(LogRecord).where(LogRecord.service_id == SERVICE_ID))
    await session.execute(delete(Deployment).where(Deployment.service_id == SERVICE_ID))
    session.add(Deployment(service_id=SERVICE_ID, version="1.8.2",
                           configuration={"db_pool_limit": 10, "worker_enabled": True}))
    await session.flush()
    return await generate_workload(session, 20)


async def inject_worker_failure(session: AsyncSession) -> dict[str, float]:
    if state.worker_healthy:
        state.worker_healthy = False
        state.queue_backlog = max(state.queue_backlog, 12)
        state.failed_jobs = max(state.failed_jobs, 5)
        # Inject logs indicating worker death
        session.add(LogRecord(
            service_id=SERVICE_ID,
            severity="CRITICAL",
            message="Background worker process heartbeat lost",
            attributes={"process_id": 4201, "error": "SIGSEGV"},
            timestamp=now()
        ))
        await session.flush()
    return await generate_workload(session, 20)


async def restart_worker_action(session: AsyncSession) -> None:
    state.worker_healthy = True
    session.add(LogRecord(
        service_id=SERVICE_ID,
        severity="INFO",
        message="Background worker restarted successfully",
        attributes={"process_id": 4205},
        timestamp=now()
    ))
    await session.flush()


async def retry_failed_jobs_action(session: AsyncSession, max_jobs: int) -> dict[str, float]:
    if not state.worker_healthy:
        raise ValueError("Cannot retry jobs while the worker is unhealthy")
    retried = min(state.failed_jobs, max_jobs)
    state.failed_jobs -= retried
    state.queue_backlog = max(0, state.queue_backlog - retried)
    session.add(LogRecord(service_id=SERVICE_ID, severity="INFO",
        message="Eligible failed jobs retried by simulator action",
        attributes={"retried": retried, "remaining_failed_jobs": state.failed_jobs}, timestamp=now()))
    await session.flush()
    return {"retried": float(retried), "remaining_failed_jobs": float(state.failed_jobs)}


async def current_metrics(session: AsyncSession) -> dict[str, float]:
    names = ["http_requests_total", "http_5xx_rate", "request_latency_ms", "db_pool_utilization",
             "db_connection_timeouts_total", "db_connection_timeout_rate", "worker_healthy",
             "worker_queue_backlog", "failed_jobs_total"]
    result: dict[str, float] = {}
    for name in names:
        row = await session.scalar(select(MetricSample).where(MetricSample.service_id == SERVICE_ID,
            MetricSample.name == name).order_by(MetricSample.timestamp.desc(), MetricSample.id.desc()).limit(1))
        result[name] = row.value if row else 0.0
    return result
