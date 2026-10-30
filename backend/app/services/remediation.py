import asyncio
import logging
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from app.config import settings
from app.models import Incident, RemediationProposal, RemediationExecution, ApprovalDecision
from app.policy.actions import ACTION_REGISTRY, evaluate_policy, validate_action
from app.simulator.state import recover_pool, restart_worker_action, current_metrics
from app.services.timeline import add_event
from app.telemetry.monitored_app import MonitoredAppAdapter
from app.telemetry.prometheus import PrometheusAdapter

logger = logging.getLogger(__name__)

async def propose_remediation(session: AsyncSession, incident_id: str, hypothesis: dict) -> RemediationProposal | None:
    incident = await session.get(Incident, incident_id)
    if not incident:
        raise ValueError("Incident not found")
    action = hypothesis.get("remediation")
    if not action:
        incident.status = "escalated"
        await add_event(session, incident_id, "INCIDENT_ESCALATED", {"reason": "Diagnosis inconclusive; no remediation proposed"})
        await session.commit()
        return None
    params = validate_action(action, hypothesis.get("parameters", {}))
    if settings.telemetry_provider == "prometheus_loki" and action == "increase_db_pool":
        if params["new_limit"] > settings.monitored_pool_max_size:
            raise ValueError(f"Monitored pool remediation cannot exceed {settings.monitored_pool_max_size}")
    policy = evaluate_policy(action, len(set(hypothesis.get("supporting_evidence", []))),
                             hypothesis.get("confidence", 0), incident.status in {"active", "investigated", "investigating"})
    if settings.telemetry_provider == "prometheus_loki" and action != "increase_db_pool":
        policy = {"decision": "blocked", "reason": "Action is not registered for the monitored application environment"}
    risk = ACTION_REGISTRY[action]["risk"]
    status = {"approval_required": "pending_approval", "auto_allowed": "approved", "blocked": "blocked"}[policy["decision"]]
    proposal = RemediationProposal(id=str(uuid4()), incident_id=incident_id, action=action,
        parameters=params, risk=risk, status=status, rationale=hypothesis.get("explanation", "Agent proposal"),
        expected_outcome=hypothesis.get("expected_outcome") or "Verify health using fresh telemetry.")
    session.add(proposal)
    await session.flush()
    await add_event(session, incident_id, "REMEDIATION_PROPOSED", {"proposal_id": proposal.id,
        "action": action, "parameters": params, "risk": risk, "policy": policy, "status": status})
    if status == "pending_approval":
        incident.status = "awaiting_approval"
        await add_event(session, incident_id, "APPROVAL_REQUESTED", {"proposal_id": proposal.id, "risk": risk})
    elif status == "blocked":
        incident.status = "escalated"
        await add_event(session, incident_id, "INCIDENT_ESCALATED", {"proposal_id": proposal.id, "reason": policy["reason"]})
    else:
        await execute_remediation(session, proposal.id)
    await session.commit()
    return proposal

async def collect_current_metrics(session: AsyncSession) -> dict:
    if settings.telemetry_provider == "prometheus_loki":
        return await PrometheusAdapter().current_metrics()
    return await current_metrics(session)


async def execute_remediation(session: AsyncSession, proposal_id: str) -> RemediationExecution:
    # Row lock plus a unique execution.proposal_id protects concurrent approval requests on PostgreSQL.
    proposal = await session.scalar(select(RemediationProposal).where(
        RemediationProposal.id == proposal_id).with_for_update())
    if not proposal or proposal.status != "approved":
        raise ValueError("Proposal is not approved for execution")
    existing = await session.scalar(select(RemediationExecution).where(RemediationExecution.proposal_id == proposal_id))
    if existing:
        return existing
    incident = await session.scalar(select(Incident).where(Incident.id == proposal.incident_id).with_for_update())
    if not incident or incident.status not in {"active", "investigated", "awaiting_approval", "remediating"}:
        raise ValueError("Incident is no longer active")
    validated = validate_action(proposal.action, proposal.parameters)
    if validated != proposal.parameters:
        raise ValueError("Stored proposal parameters no longer match the action schema")
    pre = await collect_current_metrics(session)
    execution = RemediationExecution(proposal_id=proposal.id, status="running", pre_metrics=pre)
    session.add(execution)
    incident.status = "remediating"
    await session.flush()
    try:
        real_mode = settings.telemetry_provider == "prometheus_loki"
        fresh: dict[str, Any]
        if real_mode:
            if proposal.action != "increase_db_pool":
                raise ValueError(f"Action {proposal.action} has no monitored-application executor")
            result = await MonitoredAppAdapter().increase_pool_size(proposal.parameters["new_limit"])
            sample_count = 0
        else:
            from app.simulator.state import generate_workload
            if proposal.action == "increase_db_pool":
                await recover_pool(session, proposal.parameters["new_limit"])
                result = {"status": "simulator_pool_limit_changed", "new_limit": proposal.parameters["new_limit"]}
                sample_count = 100
            elif proposal.action == "restart_worker":
                await restart_worker_action(session)
                result = {"status": "simulator_worker_restarted"}
                sample_count = 20
            elif proposal.action == "retry_failed_jobs":
                from app.simulator.state import retry_failed_jobs_action
                result = {"status": "simulator_jobs_retried", **await retry_failed_jobs_action(session, proposal.parameters["max_jobs"])}
                sample_count = 20
            else:
                raise ValueError(f"Action {proposal.action} is registered but has no safe simulator executor")
        await add_event(session, incident.id, "ACTION_EXECUTED", {"proposal_id": proposal.id,
            "action": proposal.action, "parameters": proposal.parameters, "risk": proposal.risk,
            "pre_metrics": pre, "result": result, "source": "monitored_application" if real_mode else "simulator"})
        await add_event(session, incident.id, "VERIFICATION_STARTED", {"action": proposal.action,
            "source": "prometheus" if real_mode else "simulator"})
        if real_mode:
            async with asyncio.timeout(settings.verification_window_seconds):
                health: dict[str, Any] = {}
                while True:
                    fresh = await PrometheusAdapter().current_metrics()
                    health = await MonitoredAppAdapter().health()
                    if verify_metrics(proposal.action, fresh, real_mode=True) and health.get("ready"):
                        break
                    await asyncio.sleep(settings.verification_poll_interval_seconds)
            fresh["service_health"] = health
        else:
            from app.simulator.state import generate_workload
            async with asyncio.timeout(settings.sentinel_verification_timeout):
                await generate_workload(session, sample_count)
                fresh = await current_metrics(session)
        execution.result = result
        execution.post_metrics = fresh
        recovered = verify_metrics(proposal.action, fresh, real_mode=real_mode) and (
            not real_mode or bool(fresh.get("service_health", {}).get("ready")))
        execution.status = "succeeded" if recovered else "verification_failed"
        proposal.status = "executed" if recovered else "failed"
        if recovered:
            incident.status = "resolved"
            incident.outcome = "success"
            incident.resolved_at = datetime.now(timezone.utc)
            await add_event(session, incident.id, "VERIFICATION_PASSED", {"metrics": fresh})
            await add_event(session, incident.id, "INCIDENT_RESOLVED", {"execution_id": execution.id})
        else:
            incident.status = "escalated"
            incident.outcome = "unsuccessful_remediation"
            await add_event(session, incident.id, "VERIFICATION_FAILED", {"metrics": fresh})
            await add_event(session, incident.id, "INCIDENT_ESCALATED", {"reason": "Recovery criteria were not met; automatic remediation stopped"})
    except Exception as exc:
        logger.exception("Remediation action failed")
        execution.status = "failed"
        execution.result = {"error": "Registered remediation action failed", "error_type": type(exc).__name__}
        proposal.status = "failed"
        incident.status = "escalated"
        incident.outcome = "unsuccessful_remediation"
        await add_event(session, incident.id, "VERIFICATION_FAILED", execution.result)
        await add_event(session, incident.id, "INCIDENT_ESCALATED", {"reason": "Remediation failed; automatic retries stopped"})
    await session.flush()
    return execution

def verify_metrics(action: str, metrics: dict[str, Any], real_mode: bool = False) -> bool:
    if action == "increase_db_pool":
        error_threshold = settings.http_error_rate_threshold if real_mode else 0.10
        pool_threshold = settings.sentinel_db_pool_threshold
        timeout_threshold = settings.db_timeout_rate_threshold if real_mode else 0.0
        latency = metrics.get("request_latency_ms")
        return (isinstance(metrics.get("http_5xx_rate"), (int, float)) and metrics["http_5xx_rate"] < error_threshold and
                isinstance(metrics.get("db_pool_utilization"), (int, float)) and metrics["db_pool_utilization"] < pool_threshold and
                isinstance(metrics.get("db_connection_timeout_rate"), (int, float)) and metrics["db_connection_timeout_rate"] <= timeout_threshold and
                (not real_mode or (isinstance(latency, (int, float)) and latency < 500)))
    if action == "restart_worker":
        return metrics.get("worker_healthy", 0) == 1.0
    if action == "retry_failed_jobs":
        return metrics.get("worker_healthy", 0) == 1.0 and metrics.get("failed_jobs_total", 1) == 0
    return False

async def decide_approval(session: AsyncSession, incident_id: str, proposal_id: str,
                          idempotency_key: str, approved: bool, actor: str = "human",
                          reason: str = "Rejected by operator", parameters: dict | None = None):
    proposal = await session.scalar(select(RemediationProposal).where(
        RemediationProposal.id == proposal_id).with_for_update())
    incident = await session.scalar(select(Incident).where(Incident.id == incident_id).with_for_update())
    if not proposal or proposal.incident_id != incident_id:
        raise LookupError("Proposal not found")
    if proposal.status != "pending_approval":
        raise ValueError("Proposal is not pending approval")
    if not incident or incident.status not in {"active", "investigated", "awaiting_approval"}:
        raise ValueError("Incident is no longer active")
    if parameters is None or parameters != proposal.parameters:
        raise ValueError("Approval must include the unchanged proposal parameters")
    valid = validate_action(proposal.action, parameters)
    policy = evaluate_policy(proposal.action, 99, 1.0, True)
    if policy["decision"] != "approval_required" or ACTION_REGISTRY[proposal.action]["risk"] != proposal.risk:
        raise ValueError("Proposal is not currently eligible for human approval")
    decision = ApprovalDecision(proposal_id=proposal.id, decision="approved" if approved else "rejected",
        actor=actor, idempotency_key=idempotency_key)
    session.add(decision)
    if approved:
        proposal.status = "approved"
        incident.status = "awaiting_approval"
        await add_event(session, incident_id, "ACTION_APPROVED", {"proposal_id": proposal.id, "parameters": valid})
    else:
        proposal.status = "rejected"
        incident.status = "escalated"
        await add_event(session, incident_id, "ACTION_REJECTED", {"proposal_id": proposal.id, "reason": reason})
        await add_event(session, incident_id, "INCIDENT_ESCALATED", {"reason": "Operator rejected proposed remediation"})
    await session.flush()
    if approved:
        await execute_remediation(session, proposal.id)
    await session.commit()
    return proposal
