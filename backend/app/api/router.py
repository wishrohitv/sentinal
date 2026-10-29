import asyncio
import json
import logging
from collections.abc import AsyncIterator
from fastapi import APIRouter, BackgroundTasks, Depends, Header, HTTPException, Query
from fastapi.responses import StreamingResponse
from sqlalchemy import select, desc
from sqlalchemy.ext.asyncio import AsyncSession
from app.config import settings
from app.database import get_session, SessionLocal
from app.models import Service, Incident, EvidenceRecord, RemediationProposal, RemediationExecution, LogRecord, MetricSample
from app.schemas import IncidentOut, ApprovalIn, RejectIn, ScenarioOut, InvestigateIn, WorkloadRequest
from app.simulator.state import SERVICE_ID, inject_pool_exhaustion, inject_worker_failure, reset_simulator, current_metrics, generate_workload
from app.detection.detector import detect_incident
from app.agent.loop import AgentLoop
from app.services.remediation import decide_approval, propose_remediation
from app.services.timeline import get_events

router = APIRouter()
logger = logging.getLogger(__name__)
selected_provider = settings.effective_provider
ACTIVE_INCIDENT_STATUSES = {"active", "investigating", "investigated", "awaiting_approval", "remediating", "escalated"}

async def run_investigation(incident_id: str, provider: str) -> None:
    # Background tasks own their session; they never retain a request-scoped session.
    async with SessionLocal() as session:
        try:
            agent = AgentLoop(session, incident_id)
            diagnosis = await agent.run(provider)
            await propose_remediation(session, incident_id, diagnosis)
        except Exception:
            logger.exception("Background investigation failed for incident %s", incident_id)
            await session.rollback()

@router.get("/health")
async def health():
    return {"status": "ok", "provider": selected_provider, "configured_provider": settings.effective_provider}

@router.get("/system/provider")
async def get_provider():
    return {"active": selected_provider, "demo_available": True,
        "nemotron_available": settings.nemotron_available,
        "model": settings.nvidia_model_name or None}

@router.post("/system/provider")
async def set_provider(request: InvestigateIn):
    global selected_provider
    provider = request.provider or "demo"
    if provider == "nemotron" and not settings.nemotron_available:
        raise HTTPException(400, "Configure NEBIUS_API_KEY and NVIDIA_MODEL_NAME before selecting Nemotron")
    selected_provider = provider
    return await get_provider()

@router.get("/services")
async def list_services(session: AsyncSession = Depends(get_session)):
    services = list((await session.scalars(select(Service))).all())
    return [{"id": s.id, "name": s.name, "status": s.status} for s in services]

@router.get("/services/{service_id}/health")
async def service_health(service_id: str, session: AsyncSession = Depends(get_session)):
    service = await session.get(Service, service_id)
    if not service:
        raise HTTPException(404, "Service not found")
    return {"id": service.id, "name": service.name, "status": service.status, "metrics": await current_metrics(session)}

@router.post("/services/{service_id}/request")
async def simulated_request(service_id: str, session: AsyncSession = Depends(get_session)):
    from fastapi.responses import JSONResponse
    if service_id != SERVICE_ID or not await session.get(Service, service_id):
        raise HTTPException(404, "Service not found")
    metrics = await generate_workload(session, 1)
    incident = await detect_incident(session)
    if incident and incident.status == "active":
        # Explicit API requests still get deterministic detection; the UI may launch an investigation.
        pass
    await session.commit()
    if metrics["http_5xx_rate"] > 0:
        return JSONResponse(status_code=503, content={"status": "error", "reason": "simulated database connection timeout"})
    return {"status": "ok", "request_latency_ms": metrics["request_latency_ms"]}

@router.get("/services/{service_id}/metrics")
async def get_service_metrics(service_id: str, minutes: int = Query(60, ge=1, le=1440), session: AsyncSession = Depends(get_session)):
    if not await session.get(Service, service_id):
        raise HTTPException(404, "Service not found")
    rows = list((await session.scalars(select(MetricSample).where(MetricSample.service_id == service_id)
        .order_by(desc(MetricSample.timestamp), desc(MetricSample.id)).limit(1000))).all())
    return {"service_id": service_id, "current": await current_metrics(session), "samples": [
        {"id": r.id, "name": r.name, "value": r.value, "timestamp": r.timestamp.isoformat()} for r in reversed(rows)]}

@router.get("/services/{service_id}/logs")
async def get_service_logs(service_id: str, severity: str | None = None, text: str | None = None,
                           limit: int = Query(100, ge=1, le=500), session: AsyncSession = Depends(get_session)):
    if not await session.get(Service, service_id):
        raise HTTPException(404, "Service not found")
    query = select(LogRecord).where(LogRecord.service_id == service_id)
    if severity: query = query.where(LogRecord.severity == severity.upper())
    if text: query = query.where(LogRecord.message.ilike(f"%{text}%"))
    rows = list((await session.scalars(query.order_by(desc(LogRecord.timestamp)).limit(limit))).all())
    return [{"id": r.id, "severity": r.severity, "message": r.message, "attributes": r.attributes,
             "timestamp": r.timestamp.isoformat()} for r in rows]

@router.get("/incidents", response_model=list[IncidentOut])
async def list_incidents(session: AsyncSession = Depends(get_session)):
    return list((await session.scalars(select(Incident).order_by(desc(Incident.created_at)))).all())

@router.get("/incidents/{incident_id}", response_model=IncidentOut)
async def get_incident(incident_id: str, session: AsyncSession = Depends(get_session)):
    incident = await session.get(Incident, incident_id)
    if not incident: raise HTTPException(404, "Incident not found")
    return incident

@router.get("/incidents/{incident_id}/events")
async def get_incident_events(incident_id: str, after_id: int = 0, session: AsyncSession = Depends(get_session)):
    if not await session.get(Incident, incident_id): raise HTTPException(404, "Incident not found")
    events = await get_events(session, incident_id, after_id)
    return [{"id": e.id, "incident_id": e.incident_id, "event_type": e.event_type,
             "payload": e.payload, "timestamp": e.timestamp.isoformat()} for e in events]

@router.get("/incidents/{incident_id}/stream")
async def stream_incident(incident_id: str, last_event_id: str | None = Header(None, alias="Last-Event-ID"),
                          after: int = Query(0, ge=0)):
    async with SessionLocal() as session:
        if not await session.get(Incident, incident_id): raise HTTPException(404, "Incident not found")
    try: cursor = max(after, int(last_event_id or 0))
    except ValueError: cursor = after
    async def event_stream() -> AsyncIterator[str]:
        nonlocal cursor
        while True:
            async with SessionLocal() as session:
                events = await get_events(session, incident_id, cursor)
            if events:
                for event in events:
                    cursor = event.id
                    data = {"id": event.id, "incident_id": incident_id, "event_type": event.event_type,
                            "payload": event.payload, "timestamp": event.timestamp.isoformat()}
                    yield f"id: {event.id}\nevent: update\ndata: {json.dumps(data, default=str)}\n\n"
            else:
                yield ": keep-alive\n\n"
            await asyncio.sleep(1)
    return StreamingResponse(event_stream(), media_type="text/event-stream", headers={
        "Cache-Control": "no-cache", "Connection": "keep-alive", "X-Accel-Buffering": "no"})

@router.get("/incidents/{incident_id}/evidence")
async def list_evidence(incident_id: str, session: AsyncSession = Depends(get_session)):
    if not await session.get(Incident, incident_id): raise HTTPException(404, "Incident not found")
    rows = list((await session.scalars(select(EvidenceRecord).where(EvidenceRecord.incident_id == incident_id)
        .order_by(EvidenceRecord.timestamp))).all())
    return [{"id": r.id, "source_tool": r.source_tool, "evidence_type": r.evidence_type,
        "observation": r.observation, "summary": r.summary, "source_record_ids": r.source_record_ids,
        "timestamp": r.timestamp.isoformat()} for r in rows]

@router.get("/incidents/{incident_id}/remediations")
async def list_remediations(incident_id: str, session: AsyncSession = Depends(get_session)):
    if not await session.get(Incident, incident_id): raise HTTPException(404, "Incident not found")
    rows = list((await session.scalars(select(RemediationProposal).where(RemediationProposal.incident_id == incident_id)
        .order_by(RemediationProposal.created_at))).all())
    results = []
    for row in rows:
        execution = await session.scalar(select(RemediationExecution).where(RemediationExecution.proposal_id == row.id))
        results.append({"id": row.id, "action": row.action, "parameters": row.parameters, "risk": row.risk,
            "status": row.status, "rationale": row.rationale, "expected_outcome": row.expected_outcome,
            "execution": None if not execution else {"status": execution.status, "result": execution.result,
                "pre_metrics": execution.pre_metrics, "post_metrics": execution.post_metrics}})
    return results

@router.post("/incidents/{incident_id}/investigate")
async def investigate_incident(incident_id: str, request: InvestigateIn, background: BackgroundTasks,
                               session: AsyncSession = Depends(get_session)):
    incident = await session.get(Incident, incident_id)
    if not incident: raise HTTPException(404, "Incident not found")
    if incident.status not in ACTIVE_INCIDENT_STATUSES: raise HTTPException(409, "Incident is not active")
    provider = request.provider or selected_provider
    if provider == "nemotron" and not settings.nemotron_available:
        raise HTTPException(400, "Nemotron credentials and model are not configured")
    background.add_task(run_investigation, incident_id, provider)
    return {"status": "investigation_started", "provider": provider}

async def create_incident_and_schedule(session: AsyncSession, background: BackgroundTasks, workload: dict):
    incident = await detect_incident(session)
    if incident:
        background.add_task(run_investigation, incident.id, selected_provider)
    return incident

@router.post("/incidents/{incident_id}/approve")
async def approve(incident_id: str, request: ApprovalIn, session: AsyncSession = Depends(get_session)):
    try:
        result = await decide_approval(session, incident_id, request.proposal_id, request.idempotency_key,
            True, parameters=request.parameters)
    except LookupError as exc: raise HTTPException(404, str(exc)) from exc
    except ValueError as exc: raise HTTPException(409, str(exc)) from exc
    return {"status": result.status, "proposal_id": result.id}

@router.post("/incidents/{incident_id}/reject")
async def reject(incident_id: str, request: RejectIn, session: AsyncSession = Depends(get_session)):
    try:
        result = await decide_approval(session, incident_id, request.proposal_id, request.idempotency_key,
            False, reason=request.reason, parameters=request.parameters)
    except LookupError as exc: raise HTTPException(404, str(exc)) from exc
    except ValueError as exc: raise HTTPException(409, str(exc)) from exc
    return {"status": result.status, "proposal_id": result.id}

@router.post("/demo/scenarios/db-pool-exhaustion", response_model=ScenarioOut)
async def demo_db_pool(background: BackgroundTasks, session: AsyncSession = Depends(get_session)):
    await inject_pool_exhaustion(session)
    incident = await detect_incident(session)
    await session.commit()
    if incident: background.add_task(run_investigation, incident.id, selected_provider)
    return {"status": "failure_injected", "incident_id": incident.id if incident else None}

@router.post("/demo/scenarios/worker-failure", response_model=ScenarioOut)
async def demo_worker_failure(background: BackgroundTasks, session: AsyncSession = Depends(get_session)):
    await inject_worker_failure(session)
    incident = await detect_incident(session)
    await session.commit()
    if incident: background.add_task(run_investigation, incident.id, selected_provider)
    return {"status": "failure_injected", "incident_id": incident.id if incident else None}

@router.post("/demo/reset")
async def demo_reset(session: AsyncSession = Depends(get_session)):
    await reset_simulator(session)
    await session.commit()
    return {"status": "simulator_reset"}

@router.post("/demo/workload")
async def trigger_workload(request: WorkloadRequest, background: BackgroundTasks, session: AsyncSession = Depends(get_session)):
    metrics = await generate_workload(session, request.count)
    incident = await detect_incident(session)
    if incident and incident.status == "active": background.add_task(run_investigation, incident.id, selected_provider)
    await session.commit()
    return {"metrics": metrics, "incident_id": incident.id if incident else None}
