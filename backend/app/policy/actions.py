from typing import Any
from pydantic import BaseModel, ConfigDict, Field, ValidationError

class ActionParameters(BaseModel):
    model_config = ConfigDict(extra="forbid")

class IncreasePoolParameters(ActionParameters):
    new_limit: int = Field(ge=3, le=64)
class EmptyParameters(ActionParameters):
    pass
class RetryJobsParameters(ActionParameters):
    max_jobs: int = Field(default=100, ge=1, le=1000)

ACTION_REGISTRY: dict[str, dict[str, Any]] = {
    "increase_db_pool": {"risk": "medium", "schema": IncreasePoolParameters, "minimum_evidence": 2},
    "restart_worker": {"risk": "low", "schema": EmptyParameters, "minimum_evidence": 2},
    "retry_failed_jobs": {"risk": "low", "schema": RetryJobsParameters, "minimum_evidence": 2},
    "database_schema_change": {"risk": "high", "schema": EmptyParameters, "minimum_evidence": 99},
    "delete_data": {"risk": "high", "schema": EmptyParameters, "minimum_evidence": 99},
}

def validate_action(action: str, params: dict[str, Any]) -> dict[str, Any]:
    if action not in ACTION_REGISTRY:
        raise ValueError("Action is not registered")
    try:
        return ACTION_REGISTRY[action]["schema"](**params).model_dump()
    except ValidationError as exc:
        raise ValueError(f"Invalid remediation parameters: {exc.errors(include_input=False)}") from exc

def evaluate_policy(action: str, evidence_count: int, confidence: float, active: bool) -> dict[str, Any]:
    spec = ACTION_REGISTRY.get(action)
    if not spec:
        return {"decision": "blocked", "reason": "Action is not registered"}
    if not active:
        return {"decision": "blocked", "reason": "Incident is not active"}
    if evidence_count < spec["minimum_evidence"] or confidence < 0.45:
        return {"decision": "approval_required", "reason": "Evidence or diagnostic certainty is insufficient"}
    if spec["risk"] == "high":
        return {"decision": "blocked", "reason": "High-risk actions are blocked and escalated"}
    if spec["risk"] == "medium":
        return {"decision": "approval_required", "reason": "Medium-risk actions require explicit human approval"}
    return {"decision": "auto_allowed", "reason": "Registered low-risk action meets policy requirements"}
