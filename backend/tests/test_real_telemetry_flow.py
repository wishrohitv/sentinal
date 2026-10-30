from datetime import datetime, timezone

import pytest
import pytest_asyncio
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.database import Base
from app.detection.detector import detect_incident
from app.models import EvidenceRecord, Incident, MetricSample, Service
from app.services.investigation import execute_tool
from app.config import settings
from app.telemetry.prometheus import PrometheusAdapter


@pytest_asyncio.fixture
async def real_session(monkeypatch):
    monkeypatch.setattr(settings, "telemetry_provider", "prometheus_loki")
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
    async with factory() as session:
        session.add(Service(id=settings.monitored_service_id, name="Monitored Orders API", status="unknown"))
        await session.commit()
        yield session
    await engine.dispose()


@pytest.mark.asyncio
async def test_real_mode_startup_creates_monitored_service_record(monkeypatch, real_session):
    from sqlalchemy.ext.asyncio import async_sessionmaker
    import app.main as main

    factory = async_sessionmaker(real_session.bind, expire_on_commit=False, class_=AsyncSession)
    monkeypatch.setattr(main, "SessionLocal", factory)
    await main.initialize_real_service_record()
    service = await real_session.get(Service, settings.monitored_service_id)
    assert service is not None
    assert service.name == "Monitored Orders API"


@pytest.mark.asyncio
async def test_real_detector_creates_incident_from_prometheus_only(monkeypatch, real_session):
    async def metrics(self):
        return {"source": "prometheus", "observed_at": datetime.now(timezone.utc).isoformat(),
                "http_5xx_rate": 0.40, "db_pool_utilization": 1.0,
                "db_connection_timeout_rate": 0.5, "request_latency_ms": 900.0,
                "db_pool_size": 2.0, "db_pool_checked_out": 2.0}

    monkeypatch.setattr(PrometheusAdapter, "current_metrics", metrics)
    incident = await detect_incident(real_session)
    assert incident is not None
    assert incident.service_id == settings.monitored_service_id
    assert incident.trigger_metrics["telemetry_source"] == "prometheus"
    simulator_rows = await real_session.scalars(select(MetricSample).where(MetricSample.service_id == "demo-api"))
    assert list(simulator_rows) == []


@pytest.mark.asyncio
async def test_real_detector_deduplicates_active_incident(monkeypatch, real_session):
    async def metrics(self):
        return {"source": "prometheus", "http_5xx_rate": 0.40,
                "db_pool_utilization": 1.0, "db_connection_timeout_rate": 0.5,
                "request_latency_ms": 900.0, "db_pool_size": 2.0, "db_pool_checked_out": 2.0}

    monkeypatch.setattr(PrometheusAdapter, "current_metrics", metrics)
    first = await detect_incident(real_session)
    second = await detect_incident(real_session)
    assert first.id == second.id
    incidents = list((await real_session.scalars(select(Incident))).all())
    assert len(incidents) == 1


@pytest.mark.asyncio
async def test_real_detector_does_not_open_incident_on_healthy_metrics(monkeypatch, real_session):
    async def metrics(self):
        return {"source": "prometheus", "http_5xx_rate": 0.0,
                "db_pool_utilization": 0.25, "db_connection_timeout_rate": 0.0,
                "request_latency_ms": 20.0, "db_pool_size": 2.0, "db_pool_checked_out": 0.0}

    monkeypatch.setattr(PrometheusAdapter, "current_metrics", metrics)
    assert await detect_incident(real_session) is None


@pytest.mark.asyncio
async def test_real_query_metrics_records_prometheus_source(monkeypatch, real_session):
    incident = Incident(id="test-real-incident", service_id=settings.monitored_service_id,
        title="DB pool exhaustion", severity="major", status="active", trigger="pool saturated",
        trigger_metrics={"telemetry_source": "prometheus"})
    real_session.add(incident)
    await real_session.commit()

    async def metric_samples(self, minutes, limit=None):
        return {"source": "prometheus", "observed_at": datetime.now(timezone.utc).isoformat(),
                "metrics": {"db_pool_utilization": {"count": 1, "latest": 1.0, "samples": []}}}

    monkeypatch.setattr(PrometheusAdapter, "metric_samples", metric_samples)
    result = await execute_tool(real_session, incident.id, "query_metrics", {
        "service_id": settings.monitored_service_id, "minutes": 5})
    evidence = await real_session.get(EvidenceRecord, result["evidence_id"])
    assert result["source"] == "prometheus"
    assert evidence.observation["source"] == "prometheus"
