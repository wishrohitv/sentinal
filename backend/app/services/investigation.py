from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import uuid4
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import select, desc
from sqlalchemy.ext.asyncio import AsyncSession
from app.models import MetricSample, LogRecord, Deployment, Incident, EvidenceRecord
from app.simulator.state import SERVICE_ID, current_metrics
from app.services.timeline import add_event

class ToolArgs(BaseModel):
    model_config = ConfigDict(extra="forbid")

class MetricsArgs(ToolArgs):
    service_id: str = SERVICE_ID
    minutes: int = Field(default=15, ge=1, le=1440)
class LogsArgs(ToolArgs):
    service_id: str = SERVICE_ID
    severity: str | None = None
    minutes: int = Field(default=60, ge=1, le=1440)
    text: str | None = Field(default=None, max_length=200)
    limit: int = Field(default=100, ge=1, le=500)
class ServiceArgs(ToolArgs):
    service_id: str = SERVICE_ID
class EmptyArgs(ToolArgs):
    pass

async def record_evidence(session: AsyncSession, incident_id: str, tool: str, ev_type: str,
                          observation: dict[str, Any], summary: str,
                          source_record_ids: list[Any] | None = None) -> EvidenceRecord:
    evidence = EvidenceRecord(id=f"E-{uuid4().hex[:10]}", incident_id=incident_id,
        source_tool=tool, evidence_type=ev_type, observation=observation,
        summary=summary, source_record_ids=source_record_ids or [])
    session.add(evidence)
    await session.flush()
    await add_event(session, incident_id, "EVIDENCE_RECORDED", {
        "evidence_id": evidence.id, "source_tool": tool, "summary": summary,
        "observation": observation, "source_record_ids": evidence.source_record_ids})
    return evidence

async def query_metrics(session: AsyncSession, incident_id: str, args: MetricsArgs) -> dict[str, Any]:
    since = datetime.now(timezone.utc) - timedelta(minutes=args.minutes)
    rows = list((await session.scalars(select(MetricSample).where(
        MetricSample.service_id == args.service_id, MetricSample.timestamp >= since
    ).order_by(MetricSample.timestamp, MetricSample.id))).all())
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(row.name, []).append({"value": row.value, "timestamp": row.timestamp.isoformat()})
    aggregates = {name: {"count": len(points), "min": min(p["value"] for p in points),
        "max": max(p["value"] for p in points), "avg": sum(p["value"] for p in points)/len(points),
        "latest": points[-1]["value"], "samples": points} for name, points in grouped.items()}
    evidence = await record_evidence(session, incident_id, "query_metrics", "metrics",
        {"service_id": args.service_id, "window_minutes": args.minutes, "metrics": aggregates},
        f"Read {len(rows)} persisted metric samples over {args.minutes} minutes.", [r.id for r in rows])
    return {"evidence_id": evidence.id, "metrics": aggregates}

async def query_logs(session: AsyncSession, incident_id: str, args: LogsArgs) -> dict[str, Any]:
    since = datetime.now(timezone.utc) - timedelta(minutes=args.minutes)
    query = select(LogRecord).where(LogRecord.service_id == args.service_id, LogRecord.timestamp >= since)
    if args.severity:
        query = query.where(LogRecord.severity == args.severity.upper())
    if args.text:
        query = query.where(LogRecord.message.ilike(f"%{args.text}%"))
    rows = list((await session.scalars(query.order_by(desc(LogRecord.timestamp)).limit(args.limit))).all())
    records = [{"id": r.id, "severity": r.severity, "message": r.message,
                "attributes": r.attributes, "timestamp": r.timestamp.isoformat()} for r in rows]
    evidence = await record_evidence(session, incident_id, "query_logs", "logs",
        {"count": len(rows), "records": records}, f"Matched {len(rows)} structured log records.", [r.id for r in rows])
    return {"evidence_id": evidence.id, "count": len(rows), "records": records}

async def inspect_deployment(session: AsyncSession, incident_id: str, args: ServiceArgs) -> dict[str, Any]:
    rows = list((await session.scalars(select(Deployment).where(Deployment.service_id == args.service_id)
        .order_by(desc(Deployment.timestamp), desc(Deployment.id)).limit(2))).all())
    current = rows[0] if rows else None
    previous = rows[1] if len(rows) > 1 else None
    value = {"current": None if current is None else {"id": current.id, "version": current.version,
        "configuration": current.configuration, "timestamp": current.timestamp.isoformat()},
        "previous": None if previous is None else {"id": previous.id, "version": previous.version,
        "configuration": previous.configuration, "timestamp": previous.timestamp.isoformat()}}
    summary = "No deployment records found." if current is None else f"Latest deployment is {current.version}."
    evidence = await record_evidence(session, incident_id, "inspect_deployment", "deployment", value, summary,
        [r.id for r in rows])
    return {"evidence_id": evidence.id, "deployment": value}

async def inspect_configuration_changes(session: AsyncSession, incident_id: str, args: ServiceArgs) -> dict[str, Any]:
    rows = list((await session.scalars(select(Deployment).where(Deployment.service_id == args.service_id)
        .order_by(desc(Deployment.timestamp), desc(Deployment.id)).limit(2))).all())
    current = rows[0].configuration if rows else {}
    previous = rows[1].configuration if len(rows) > 1 else {}
    changes = {key: {"before": previous.get(key), "after": current.get(key)}
               for key in set(current) | set(previous) if current.get(key) != previous.get(key)}
    value = {"changes": changes, "deployment_ids": [r.id for r in rows]}
    evidence = await record_evidence(session, incident_id, "inspect_configuration_changes", "configuration_diff",
        value, f"Detected {len(changes)} configuration value changes.", [r.id for r in rows])
    return {"evidence_id": evidence.id, **value}

async def inspect_service_health(session: AsyncSession, incident_id: str, args: ServiceArgs) -> dict[str, Any]:
    from app.models import Service
    service = await session.get(Service, args.service_id)
    metrics = await current_metrics(session)
    value = {"service_id": args.service_id, "status": service.status if service else "unknown", "metrics": metrics}
    evidence = await record_evidence(session, incident_id, "inspect_service_health", "health", value,
        f"Service state is {value['status']} with worker {'healthy' if metrics.get('worker_healthy', 1) else 'unhealthy'}.")
    return {"evidence_id": evidence.id, **value}

async def search_previous_incidents(session: AsyncSession, incident_id: str, args: ServiceArgs) -> dict[str, Any]:
    current = await session.get(Incident, incident_id)
    rows = list((await session.scalars(select(Incident).where(Incident.service_id == args.service_id,
        Incident.id != incident_id, Incident.status == "resolved").order_by(desc(Incident.resolved_at)).limit(25))).all())
    symptoms = set((current.trigger.lower() if current else "").replace(";", " ").split())
    relevant = []
    for prior in rows:
        searchable = " ".join([prior.title or "", prior.trigger or "",
            str(prior.root_cause or ""), str(prior.outcome or "")]).lower()
        same_incident_type = bool(current and prior.title == current.title)
        symptom_match = any(term in searchable for term in symptoms if len(term) > 4)
        if same_incident_type or symptom_match:
            relevant.append(prior)
        if len(relevant) >= 10:
            break
    matches = [{"id": i.id, "title": i.title, "trigger": i.trigger, "trigger_metrics": i.trigger_metrics,
        "root_cause": i.root_cause, "outcome": i.outcome, "resolved_at": i.resolved_at.isoformat() if i.resolved_at else None}
        for i in relevant]
    evidence = await record_evidence(session, incident_id, "search_previous_incidents", "incident_memory",
        {"count": len(matches), "incidents": matches}, f"Found {len(matches)} resolved incidents for service.", [i.id for i in rows])
    return {"evidence_id": evidence.id, "incidents": matches}

TOOL_FUNCTIONS = {
    "query_metrics": (MetricsArgs, query_metrics),
    "query_logs": (LogsArgs, query_logs),
    "inspect_deployment": (ServiceArgs, inspect_deployment),
    "inspect_configuration_changes": (ServiceArgs, inspect_configuration_changes),
    "inspect_service_health": (ServiceArgs, inspect_service_health),
    "search_previous_incidents": (ServiceArgs, search_previous_incidents),
}

async def execute_tool(session: AsyncSession, incident_id: str, name: str, raw_args: dict[str, Any]) -> dict[str, Any]:
    if name not in TOOL_FUNCTIONS:
        raise ValueError(f"Unknown investigation tool: {name}")
    schema, function = TOOL_FUNCTIONS[name]
    args = schema.model_validate(raw_args)
    if hasattr(args, "service_id") and args.service_id != SERVICE_ID:
        raise ValueError("Tool cannot query services outside the isolated simulator")
    return await function(session, incident_id, args)
