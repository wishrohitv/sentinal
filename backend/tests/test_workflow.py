import pytest
import pytest_asyncio
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from app.database import Base
from app.models import Incident, EvidenceRecord, RemediationExecution
from app.simulator.state import state, initialize_simulator, inject_pool_exhaustion, inject_worker_failure, current_metrics, generate_workload, SERVICE_ID
from app.detection.detector import detect_incident
from app.agent.loop import AgentLoop
from app.services.investigation import execute_tool, search_previous_incidents, ServiceArgs
from app.services.remediation import propose_remediation, decide_approval
from app.policy.actions import validate_action, evaluate_policy
from app.services.timeline import add_event, get_events
from app.config import settings
import app.api.router as api_router

@pytest_asyncio.fixture
async def session():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
    state.failed = False
    state.pool_limit = 10
    state.worker_healthy = True
    state.deployment_version = "1.8.2"
    state.requests = state.errors = state.timeouts = 0
    state.queue_backlog = state.failed_jobs = 0
    async with factory() as db:
        await initialize_simulator(db)
        yield db
    await engine.dispose()

def test_nemotron_can_be_selected_when_credentials_exist(monkeypatch):
    monkeypatch.setattr(settings, "ai_provider", "nemotron")
    monkeypatch.setattr(settings, "nebius_api_key", "")
    monkeypatch.setattr(settings, "nvidia_model_name", "")
    assert not settings.nemotron_available
    assert settings.effective_provider == "demo"
    monkeypatch.setattr(settings, "ai_provider", "demo")
    monkeypatch.setattr(settings, "nebius_api_key", "test-key")
    monkeypatch.setattr(settings, "nvidia_model_name", "configured/model-id")
    assert settings.nemotron_available
    assert settings.effective_provider == "demo"

@pytest.mark.asyncio
async def test_normal_metrics_do_not_create_incident(session):
    assert await detect_incident(session) is None

@pytest.mark.asyncio
async def test_detection_and_duplicate_suppression(session):
    await inject_pool_exhaustion(session)
    first = await detect_incident(session)
    second = await detect_incident(session)
    assert first is not None
    assert first.id == second.id
    assert first.trigger_metrics["rolling_db_pool_utilization"] > 0.90
    assert first.trigger_metrics["rolling_http_5xx_rate"] > 0.10
    assert len((await session.scalars(select(Incident))).all()) == 1

@pytest.mark.asyncio
async def test_tools_read_actual_metrics_and_reject_unknown_tool(session):
    await inject_pool_exhaustion(session)
    incident = await detect_incident(session)
    result = await execute_tool(session, incident.id, "query_metrics", {"service_id": SERVICE_ID, "minutes": 15})
    assert result["metrics"]["db_pool_utilization"]["max"] >= 0.99
    evidence = await session.get(EvidenceRecord, result["evidence_id"])
    assert evidence and evidence.incident_id == incident.id and evidence.source_record_ids
    with pytest.raises(ValueError, match="Unknown investigation tool"):
        await execute_tool(session, incident.id, "run_shell", {})

@pytest.mark.asyncio
async def test_demo_agent_evidence_and_approval_verified_recovery(session):
    await inject_pool_exhaustion(session)
    incident = await detect_incident(session)
    diagnosis = await AgentLoop(session, incident.id).run("demo")
    assert len(diagnosis["supporting_evidence"]) >= 4
    for evidence_id in diagnosis["supporting_evidence"]:
        assert await session.get(EvidenceRecord, evidence_id)
    proposal = await propose_remediation(session, incident.id, diagnosis)
    assert proposal.status == "pending_approval"
    before = await current_metrics(session)
    assert before["http_5xx_rate"] > 0.10
    await decide_approval(session, incident.id, proposal.id, "approve-pool-001", True, parameters=proposal.parameters)
    refreshed = await session.get(Incident, incident.id)
    execution = await session.scalar(select(RemediationExecution).where(RemediationExecution.proposal_id == proposal.id))
    assert refreshed.status == "resolved"
    assert execution.status == "succeeded"
    assert execution.pre_metrics["db_pool_utilization"] >= 0.99
    assert execution.post_metrics["db_pool_utilization"] < 0.90
    assert execution.post_metrics["db_connection_timeout_rate"] == 0
    assert refreshed.resolved_at is not None
    with pytest.raises(ValueError, match="not pending approval"):
        await decide_approval(session, incident.id, proposal.id, "approve-pool-002", True, parameters=proposal.parameters)

@pytest.mark.asyncio
async def test_invalid_parameters_and_high_risk_actions_are_blocked(session):
    with pytest.raises(ValueError, match="Invalid remediation parameters"):
        validate_action("increase_db_pool", {"new_limit": 10000})
    policy = evaluate_policy("delete_data", 100, 1.0, True)
    assert policy["decision"] == "blocked"
    assert evaluate_policy("increase_db_pool", 4, 0.9, True)["decision"] == "approval_required"

@pytest.mark.asyncio
async def test_retry_failed_jobs_operates_on_simulator_queue(session):
    from uuid import uuid4
    from app.services.investigation import query_metrics, query_logs, MetricsArgs, LogsArgs
    state.failed_jobs = 3
    state.queue_backlog = 3
    await generate_workload(session, 20)
    incident = Incident(id=str(uuid4()), service_id=SERVICE_ID, title="Failed jobs backlog",
        severity="major", status="active", trigger="eligible failed jobs accumulated", trigger_metrics=await current_metrics(session))
    session.add(incident)
    await session.commit()
    metric_evidence = await query_metrics(session, incident.id, MetricsArgs(service_id=SERVICE_ID))
    log_evidence = await query_logs(session, incident.id, LogsArgs(service_id=SERVICE_ID))
    diagnosis = {"supporting_evidence": [metric_evidence["evidence_id"], log_evidence["evidence_id"]],
        "confidence": 0.9, "remediation": "retry_failed_jobs", "parameters": {"max_jobs": 10},
        "explanation": "Worker is healthy and simulator records eligible failed jobs.",
        "expected_outcome": "Eligible failed jobs are retried and backlog returns to zero."}
    proposal = await propose_remediation(session, incident.id, diagnosis)
    assert proposal.status == "executed"
    assert (await current_metrics(session))["failed_jobs_total"] == 0
    assert (await session.get(Incident, incident.id)).status == "resolved"

@pytest.mark.asyncio
async def test_low_risk_worker_action_executes_and_verifies(session):
    await inject_worker_failure(session)
    incident = await detect_incident(session)
    assert incident.title == "Worker process failure"
    diagnosis = await AgentLoop(session, incident.id).run("demo")
    assert diagnosis["remediation"] == "restart_worker"
    proposal = await propose_remediation(session, incident.id, diagnosis)
    assert proposal.status == "executed"
    assert (await session.get(Incident, incident.id)).status == "resolved"
    assert (await current_metrics(session))["worker_healthy"] == 1.0

@pytest.mark.asyncio
async def test_rejected_approval_is_persisted_and_not_executed(session):
    await inject_pool_exhaustion(session)
    incident = await detect_incident(session)
    diagnosis = await AgentLoop(session, incident.id).run("demo")
    proposal = await propose_remediation(session, incident.id, diagnosis)
    await decide_approval(session, incident.id, proposal.id, "reject-pool-001", False,
                          reason="Need a rollback instead", parameters=proposal.parameters)
    assert proposal.status == "rejected"
    assert await session.scalar(select(RemediationExecution).where(RemediationExecution.proposal_id == proposal.id)) is None
    event_types = [event.event_type for event in await get_events(session, incident.id)]
    assert "ACTION_REJECTED" in event_types
    assert "INCIDENT_ESCALATED" in event_types

@pytest.mark.asyncio
async def test_failed_verification_does_not_resolve(monkeypatch, session):
    await inject_pool_exhaustion(session)
    incident = await detect_incident(session)
    diagnosis = await AgentLoop(session, incident.id).run("demo")
    proposal = await propose_remediation(session, incident.id, diagnosis)
    async def suppress_fresh_samples(*args, **kwargs):
        return {}
    monkeypatch.setattr("app.simulator.state.generate_workload", suppress_fresh_samples)
    await decide_approval(session, incident.id, proposal.id, "approve-failure-001", True, parameters=proposal.parameters)
    assert (await session.get(Incident, incident.id)).status == "escalated"
    execution = await session.scalar(select(RemediationExecution).where(RemediationExecution.proposal_id == proposal.id))
    assert execution.status == "verification_failed"
    assert "INCIDENT_RESOLVED" not in [event.event_type for event in await get_events(session, incident.id)]

@pytest.mark.asyncio
async def test_incident_memory_retrieves_resolved_service_incidents(session):
    await inject_pool_exhaustion(session)
    previous = await detect_incident(session)
    previous.status = "resolved"
    previous.outcome = "success"
    previous.root_cause = {"root_cause": "Pool limit change", "remediation": "increase_db_pool"}
    await session.commit()
    current = await detect_incident(session)
    memory = await search_previous_incidents(session, current.id, ServiceArgs(service_id=SERVICE_ID))
    assert any(item["id"] == previous.id for item in memory["incidents"])
    assert memory["incidents"][0]["root_cause"]["remediation"] == "increase_db_pool"

@pytest.mark.asyncio
async def test_agent_rejects_fabricated_or_cross_incident_evidence(session):
    await inject_pool_exhaustion(session)
    incident = await detect_incident(session)
    loop = AgentLoop(session, incident.id)
    with pytest.raises(ValueError, match="evidence references"):
        await loop._validate_diagnosis({"root_cause": "unverified", "explanation": "x",
            "supporting_evidence": ["E-invented"], "contradicting_evidence": [],
            "missing_information": [], "confidence": 0.5, "remediation": None,
            "parameters": {}, "expected_outcome": None})

@pytest.mark.asyncio
async def test_persistent_sse_stream_emits_persisted_event(session, monkeypatch):
    await inject_pool_exhaustion(session)
    incident = await detect_incident(session)
    await add_event(session, incident.id, "TOOL_CALL_STARTED", {"tool": "query_metrics"})
    await session.commit()
    factory = async_sessionmaker(session.bind, expire_on_commit=False, class_=AsyncSession)
    monkeypatch.setattr(api_router, "SessionLocal", factory)
    response = await api_router.stream_incident(incident.id, None, 0)
    body = response.body_iterator
    first_chunk = await body.__anext__()
    second_chunk = await body.__anext__()
    assert "event: update" in first_chunk
    assert "INCIDENT_DETECTED" in first_chunk
    assert "TOOL_CALL_STARTED" in second_chunk
    await body.aclose()

@pytest.mark.asyncio
async def test_persistent_timeline_events_replay_in_order(session):
    await inject_pool_exhaustion(session)
    incident = await detect_incident(session)
    await add_event(session, incident.id, "TOOL_CALL_STARTED", {"tool": "query_metrics"})
    await session.commit()
    events = await get_events(session, incident.id)
    assert [event.event_type for event in events] == ["INCIDENT_DETECTED", "TOOL_CALL_STARTED"]
    assert (await get_events(session, incident.id, after_id=events[0].id))[0].id == events[1].id
