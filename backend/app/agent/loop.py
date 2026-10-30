import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Any
from openai import AsyncOpenAI
from pydantic import BaseModel, ConfigDict, Field, ValidationError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from app.config import settings
from app.models import EvidenceRecord, Incident, Investigation
from app.services.investigation import TOOL_FUNCTIONS, execute_tool
from app.services.timeline import add_event

logger = logging.getLogger(__name__)

class Diagnosis(BaseModel):
    model_config = ConfigDict(extra="forbid")
    root_cause: str | None = None
    explanation: str
    supporting_evidence: list[str] = Field(default_factory=list)
    contradicting_evidence: list[str] = Field(default_factory=list)
    missing_information: list[str] = Field(default_factory=list)
    confidence: float = Field(ge=0, le=1)
    remediation: str | None = None
    parameters: dict[str, Any] = Field(default_factory=dict)
    expected_outcome: str | None = None

class AgentLoop:
    def __init__(self, session: AsyncSession, incident_id: str):
        self.session, self.incident_id = session, incident_id
        self.max_calls = settings.sentinel_max_tool_calls
        self.tool_calls = 0
        self.evidence_ids: list[str] = []

    async def run(self, provider_name: str | None = None) -> dict[str, Any]:
        incident = await self.session.get(Incident, self.incident_id)
        if incident is None:
            raise ValueError("Incident not found")
        provider = provider_name or settings.effective_provider
        if provider == "nemotron" and not settings.nemotron_available:
            raise ValueError("Nemotron requested but NEBIUS_API_KEY and NVIDIA_MODEL_NAME are not configured")
        investigation = Investigation(incident_id=self.incident_id, provider=provider, status="running")
        self.session.add(investigation)
        await add_event(self.session, self.incident_id, "INVESTIGATION_STARTED", {"provider": provider})
        await self.session.commit()
        try:
            async with asyncio.timeout(settings.sentinel_investigation_timeout):
                result = await (self._run_demo(incident) if provider == "demo" else self._run_openai_compatible(incident))
            result = await self._validate_diagnosis(result)
            incident = await self.session.get(Incident, self.incident_id)
            incident.root_cause = result
            incident.status = "investigated"
            investigation.status = "completed"
            investigation.summary = result["explanation"]
            investigation.completed_at = datetime.now(timezone.utc)
            await add_event(self.session, self.incident_id, "HYPOTHESIS_CREATED", result)
            await self.session.commit()
            return result
        except Exception as exc:
            logger.exception("Investigation failed for incident %s", self.incident_id)
            investigation.status = "failed"
            investigation.summary = type(exc).__name__
            investigation.completed_at = datetime.now(timezone.utc)
            await add_event(self.session, self.incident_id, "INCIDENT_ESCALATED", {"reason": "Investigation failed", "error_type": type(exc).__name__})
            await self.session.commit()
            raise

    async def _run_demo(self, incident: Incident) -> dict[str, Any]:
        # This strategy chooses relevant tools conditionally and always executes real DB-backed tools.
        logs = await self._call_tool("query_logs", {"severity": "CRITICAL" if "worker" in incident.title.lower() else "ERROR"})
        metrics = await self._call_tool("query_metrics", {"service_id": incident.service_id, "minutes": 15})
        health = await self._call_tool("inspect_service_health", {"service_id": incident.service_id})
        if "worker" in incident.title.lower():
            await self._call_tool("search_previous_incidents", {"service_id": incident.service_id})
            return {"root_cause": "Background worker crashed", "explanation": "The simulator reports an unhealthy worker and recorded a process heartbeat failure.",
                "supporting_evidence": self.evidence_ids, "contradicting_evidence": [], "missing_information": [],
                "confidence": 0.94, "remediation": "restart_worker", "parameters": {},
                "expected_outcome": "The worker heartbeat and health metric return to healthy."}
        deployment = await self._call_tool("inspect_deployment", {"service_id": incident.service_id})
        config = await self._call_tool("inspect_configuration_changes", {"service_id": incident.service_id})
        await self._call_tool("search_previous_incidents", {"service_id": incident.service_id})
        details = config.get("changes", {})
        if not logs.get("count") or not metrics.get("metrics"):
            return {"root_cause": None, "explanation": "The available telemetry is insufficient to identify a supported cause.",
                "supporting_evidence": self.evidence_ids, "contradicting_evidence": [],
                "missing_information": ["A matching database timeout log and pool saturation sample"],
                "confidence": 0.3, "remediation": None, "parameters": {}, "expected_outcome": None}
        return {"root_cause": "Database pool limit was reduced in the latest deployment",
            "explanation": f"Observed database timeouts and pool saturation. Deployment/config evidence shows the current pool setting: {details.get('db_pool_limit', {}).get('after', 2)}.",
            "supporting_evidence": self.evidence_ids, "contradicting_evidence": [], "missing_information": [],
            "confidence": 0.94, "remediation": "increase_db_pool", "parameters": {"new_limit": 20},
            "expected_outcome": "Fresh workload measurements show low pool utilization, no 5xx responses, and no new connection timeouts."}

    def _tool_definitions(self) -> list[dict[str, Any]]:
        definitions = []
        for name, (schema, _) in TOOL_FUNCTIONS.items():
            definitions.append({"type": "function", "function": {"name": name,
                "description": f"Collect verified evidence using {name}.", "parameters": schema.model_json_schema()}})
        return definitions

    async def _run_openai_compatible(self, incident: Incident) -> dict[str, Any]:
        base_url = settings.nebius_base_url
        api_key = settings.nebius_api_key
        model = settings.nvidia_model_name
        messages: list[dict[str, Any]] = [
            {"role": "system", "content": "You are Sentinel, a reliability engineer. Choose only the provided tools to collect evidence, then return a final JSON diagnosis. Do not invent observations or evidence IDs. Propose only registered actions. If evidence is insufficient, set root_cause and remediation to null and list missing_information. Final JSON fields: root_cause, explanation, supporting_evidence, contradicting_evidence, missing_information, confidence, remediation, parameters, expected_outcome."},
            {"role": "user", "content": json.dumps({"incident_id": incident.id, "service_id": incident.service_id,
                "title": incident.title, "trigger": incident.trigger, "trigger_metrics": incident.trigger_metrics})}
        ]
        tool_calls = 0
        async with AsyncOpenAI(api_key=api_key, base_url=base_url, timeout=25.0, max_retries=0) as client:
            while tool_calls <= self.max_calls:
                response = await client.chat.completions.create(
                    model=model, messages=messages, tools=self._tool_definitions(),
                    tool_choice="auto", temperature=0.1)
                if not response.choices:
                    raise ValueError("Model returned no response choices")
                message = response.choices[0].message.model_dump(exclude_none=True)
                calls = message.get("tool_calls") or []
                if not calls:
                    content = message.get("content")
                    if not content:
                        raise ValueError("Model returned neither tool calls nor a diagnosis")
                    try:
                        return Diagnosis.model_validate_json(content).model_dump()
                    except ValidationError as exc:
                        raise ValueError("Model returned malformed diagnosis JSON") from exc
                if tool_calls + len(calls) > self.max_calls:
                    raise ValueError("Maximum investigation tool call count exceeded")
                messages.append(message)
                for call in calls:
                    name = call.get("function", {}).get("name", "")
                    try:
                        args = json.loads(call.get("function", {}).get("arguments", "{}"))
                    except (json.JSONDecodeError, TypeError) as exc:
                        raise ValueError("Model returned malformed tool arguments") from exc
                    result = await self._call_tool(name, args)
                    tool_calls += 1
                    messages.append({"role": "tool", "tool_call_id": call["id"], "content": json.dumps(result)})
        raise ValueError("Maximum investigation iterations reached without a final diagnosis")

    async def _call_tool(self, name: str, args: dict[str, Any]) -> dict[str, Any]:
        if self.tool_calls >= self.max_calls:
            raise ValueError("Maximum investigation tool call count exceeded")
        self.tool_calls += 1
        await add_event(self.session, self.incident_id, "TOOL_CALL_STARTED", {"tool": name, "arguments": args})
        await self.session.commit()
        result = await execute_tool(self.session, self.incident_id, name, args)
        evidence_id = result.get("evidence_id")
        if evidence_id:
            self.evidence_ids.append(evidence_id)
        await add_event(self.session, self.incident_id, "TOOL_CALL_COMPLETED", {"tool": name, "result": result})
        await self.session.commit()
        return result

    async def _validate_diagnosis(self, raw: dict[str, Any]) -> dict[str, Any]:
        diagnosis = Diagnosis.model_validate(raw).model_dump()
        refs = set(diagnosis["supporting_evidence"] + diagnosis["contradicting_evidence"])
        if refs:
            rows = list((await self.session.scalars(select(EvidenceRecord).where(
                EvidenceRecord.incident_id == self.incident_id, EvidenceRecord.id.in_(refs)))).all())
            if {row.id for row in rows} != refs:
                raise ValueError("Diagnosis contains evidence references not recorded for this incident")
        if diagnosis["remediation"] is None:
            diagnosis["parameters"] = {}
        return diagnosis
