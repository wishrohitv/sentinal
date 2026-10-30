import pytest
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
import app.database as database
import app.api.router as router
import app.main as main
from app.config import settings
from app.simulator.state import state

@pytest.fixture
def test_client(monkeypatch):
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    factory = async_sessionmaker(engine, expire_on_commit=False)
    monkeypatch.setattr(database, "engine", engine)
    monkeypatch.setattr(database, "SessionLocal", factory)
    monkeypatch.setattr(main, "engine", engine)
    monkeypatch.setattr(main, "SessionLocal", factory)
    monkeypatch.setattr(router, "SessionLocal", factory)
    monkeypatch.setattr(router, "selected_provider", "demo")
    state.failed = False
    state.pool_limit = 10
    state.worker_healthy = True
    state.queue_backlog = state.failed_jobs = 0
    state.deployment_version = "1.8.2"
    state.requests = state.errors = state.timeouts = 0
    with TestClient(main.app) as client:
        yield client
    import asyncio
    asyncio.run(engine.dispose())

@pytest.mark.asyncio
async def test_real_pool_scenario_endpoint_schedules_controlled_requests(monkeypatch):
    from fastapi import BackgroundTasks

    monkeypatch.setattr(settings, "telemetry_provider", "prometheus_loki")
    background = BackgroundTasks()
    result = await router.monitored_db_pool_exhaustion(background)
    assert result["source"] == "monitored_application"
    assert result["concurrent_requests"] == 4
    assert len(background.tasks) == 1


def test_failure_injection_sse_approval_and_verified_resolution(test_client):
    client = test_client
    trigger = client.post("/api/demo/scenarios/db-pool-exhaustion")
    assert trigger.status_code == 200
    incident_id = trigger.json()["incident_id"]
    assert incident_id
    incident = client.get(f"/api/incidents/{incident_id}").json()
    assert incident["status"] == "awaiting_approval"
    evidence = client.get(f"/api/incidents/{incident_id}/evidence").json()
    assert len(evidence) >= 4
    timeline = client.get(f"/api/incidents/{incident_id}/events").json()
    assert any(event["event_type"] == "TOOL_CALL_COMPLETED" for event in timeline)
    assert any(event["event_type"] == "INCIDENT_DETECTED" for event in timeline)
    proposals = client.get(f"/api/incidents/{incident_id}/remediations").json()
    proposal = proposals[0]
    result = client.post(f"/api/incidents/{incident_id}/approve", json={
        "proposal_id": proposal["id"], "parameters": proposal["parameters"],
        "idempotency_key": "api-approve-test-001"})
    assert result.status_code == 200
    final = client.get(f"/api/incidents/{incident_id}").json()
    assert final["status"] == "resolved"
    remediation = client.get(f"/api/incidents/{incident_id}/remediations").json()[0]
    assert remediation["execution"]["status"] == "succeeded"
    assert remediation["execution"]["post_metrics"]["http_5xx_rate"] == 0
