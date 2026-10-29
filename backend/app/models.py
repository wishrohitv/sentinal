from datetime import datetime, timezone
from typing import Any
from sqlalchemy import DateTime, Float, ForeignKey, Index, Integer, String, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.types import JSON
from sqlalchemy.orm import Mapped, mapped_column, relationship
from app.database import Base


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def json_type():
    return JSON().with_variant(JSONB, "postgresql")


class Service(Base):
    __tablename__ = "services"
    id: Mapped[str] = mapped_column(String(100), primary_key=True)
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    status: Mapped[str] = mapped_column(String(30), default="healthy")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class MetricSample(Base):
    __tablename__ = "metric_samples"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    service_id: Mapped[str] = mapped_column(ForeignKey("services.id"), index=True)
    name: Mapped[str] = mapped_column(String(100), index=True)
    value: Mapped[float] = mapped_column(Float)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, index=True)


class LogRecord(Base):
    __tablename__ = "log_records"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    service_id: Mapped[str] = mapped_column(ForeignKey("services.id"), index=True)
    severity: Mapped[str] = mapped_column(String(20), index=True)
    message: Mapped[str] = mapped_column(Text)
    attributes: Mapped[dict[str, Any]] = mapped_column(json_type(), default=dict)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, index=True)


class Deployment(Base):
    __tablename__ = "deployments"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    service_id: Mapped[str] = mapped_column(ForeignKey("services.id"), index=True)
    version: Mapped[str] = mapped_column(String(50))
    configuration: Mapped[dict[str, Any]] = mapped_column(json_type(), default=dict)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, index=True)


class Incident(Base):
    __tablename__ = "incidents"
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    service_id: Mapped[str] = mapped_column(ForeignKey("services.id"), index=True)
    title: Mapped[str] = mapped_column(String(250))
    severity: Mapped[str] = mapped_column(String(20))
    status: Mapped[str] = mapped_column(String(30), default="active", index=True)
    trigger: Mapped[str] = mapped_column(Text)
    trigger_metrics: Mapped[dict[str, Any]] = mapped_column(json_type(), default=dict)
    root_cause: Mapped[dict[str, Any] | None] = mapped_column(json_type(), nullable=True)
    outcome: Mapped[str | None] = mapped_column(String(50), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, index=True)
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    events: Mapped[list["IncidentEvent"]] = relationship(back_populates="incident", cascade="all, delete-orphan")


class IncidentEvent(Base):
    __tablename__ = "incident_events"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    incident_id: Mapped[str] = mapped_column(ForeignKey("incidents.id", ondelete="CASCADE"), index=True)
    event_type: Mapped[str] = mapped_column(String(50), index=True)
    payload: Mapped[dict[str, Any]] = mapped_column(json_type(), default=dict)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, index=True)
    incident: Mapped[Incident] = relationship(back_populates="events")


class EvidenceRecord(Base):
    __tablename__ = "evidence_records"
    id: Mapped[str] = mapped_column(String(40), primary_key=True)
    incident_id: Mapped[str] = mapped_column(ForeignKey("incidents.id", ondelete="CASCADE"), index=True)
    source_tool: Mapped[str] = mapped_column(String(80))
    evidence_type: Mapped[str] = mapped_column(String(80))
    observation: Mapped[dict[str, Any]] = mapped_column(json_type(), default=dict)
    summary: Mapped[str] = mapped_column(Text)
    source_record_ids: Mapped[list[Any]] = mapped_column(json_type(), default=list)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, index=True)


class Investigation(Base):
    __tablename__ = "investigations"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    incident_id: Mapped[str] = mapped_column(ForeignKey("incidents.id", ondelete="CASCADE"), index=True)
    provider: Mapped[str] = mapped_column(String(40))
    status: Mapped[str] = mapped_column(String(30))
    summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class RemediationProposal(Base):
    __tablename__ = "remediation_proposals"
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    incident_id: Mapped[str] = mapped_column(ForeignKey("incidents.id", ondelete="CASCADE"), index=True)
    action: Mapped[str] = mapped_column(String(80))
    parameters: Mapped[dict[str, Any]] = mapped_column(json_type(), default=dict)
    risk: Mapped[str] = mapped_column(String(20))
    status: Mapped[str] = mapped_column(String(30), default="proposed", index=True)
    rationale: Mapped[str] = mapped_column(Text)
    expected_outcome: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class RemediationExecution(Base):
    __tablename__ = "remediation_executions"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    proposal_id: Mapped[str] = mapped_column(ForeignKey("remediation_proposals.id"), unique=True, index=True)
    status: Mapped[str] = mapped_column(String(30))
    pre_metrics: Mapped[dict[str, Any]] = mapped_column(json_type(), default=dict)
    result: Mapped[dict[str, Any]] = mapped_column(json_type(), default=dict)
    post_metrics: Mapped[dict[str, Any]] = mapped_column(json_type(), default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class ApprovalDecision(Base):
    __tablename__ = "approval_decisions"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    proposal_id: Mapped[str] = mapped_column(ForeignKey("remediation_proposals.id"), index=True)
    decision: Mapped[str] = mapped_column(String(20))
    actor: Mapped[str] = mapped_column(String(100), default="human")
    idempotency_key: Mapped[str] = mapped_column(String(100), unique=True, index=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


Index("ix_event_incident_id_id", IncidentEvent.incident_id, IncidentEvent.id)
