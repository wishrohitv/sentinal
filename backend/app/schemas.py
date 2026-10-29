from datetime import datetime
from typing import Any, Literal
from pydantic import BaseModel, ConfigDict, Field

class ORMModel(BaseModel):
    model_config = ConfigDict(from_attributes=True)

class IncidentOut(ORMModel):
    id: str
    service_id: str
    title: str
    severity: str
    status: str
    trigger: str
    trigger_metrics: dict[str, Any]
    root_cause: dict[str, Any] | None = None
    outcome: str | None = None
    created_at: datetime
    resolved_at: datetime | None = None

class ApprovalIn(BaseModel):
    proposal_id: str
    idempotency_key: str = Field(min_length=8, max_length=100)
    parameters: dict[str, Any]

class RejectIn(ApprovalIn):
    reason: str = Field(default="Rejected by operator", max_length=500)

class InvestigateIn(BaseModel):
    provider: Literal["demo", "nemotron"] | None = None

class ScenarioOut(BaseModel):
    status: str
    incident_id: str | None = None

class WorkloadRequest(BaseModel):
    count: int = Field(default=20, ge=1, le=500)
