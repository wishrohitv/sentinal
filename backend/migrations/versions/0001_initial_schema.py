"""Create Sentinel's initial relational schema.

Revision ID: 0001_initial
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "0001_initial"
down_revision = None
branch_labels = None
depends_on = None

json_type = sa.JSON().with_variant(postgresql.JSONB(), "postgresql")

def upgrade() -> None:
    op.create_table("services",
        sa.Column("id", sa.String(100), primary_key=True),
        sa.Column("name", sa.String(200), nullable=False),
        sa.Column("status", sa.String(30), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False))
    op.create_table("incidents",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("service_id", sa.String(100), sa.ForeignKey("services.id"), nullable=False),
        sa.Column("title", sa.String(250), nullable=False),
        sa.Column("severity", sa.String(20), nullable=False),
        sa.Column("status", sa.String(30), nullable=False),
        sa.Column("trigger", sa.Text(), nullable=False),
        sa.Column("trigger_metrics", json_type, nullable=False),
        sa.Column("root_cause", json_type, nullable=True),
        sa.Column("outcome", sa.String(50), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=True))
    op.create_table("metric_samples",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("service_id", sa.String(100), sa.ForeignKey("services.id"), nullable=False),
        sa.Column("name", sa.String(100), nullable=False),
        sa.Column("value", sa.Float(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False))
    op.create_table("log_records",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("service_id", sa.String(100), sa.ForeignKey("services.id"), nullable=False),
        sa.Column("severity", sa.String(20), nullable=False),
        sa.Column("message", sa.Text(), nullable=False),
        sa.Column("attributes", json_type, nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False))
    op.create_table("deployments",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("service_id", sa.String(100), sa.ForeignKey("services.id"), nullable=False),
        sa.Column("version", sa.String(50), nullable=False),
        sa.Column("configuration", json_type, nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False))
    op.create_table("incident_events",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("incident_id", sa.String(36), sa.ForeignKey("incidents.id", ondelete="CASCADE"), nullable=False),
        sa.Column("event_type", sa.String(50), nullable=False),
        sa.Column("payload", json_type, nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False))
    op.create_table("evidence_records",
        sa.Column("id", sa.String(40), primary_key=True),
        sa.Column("incident_id", sa.String(36), sa.ForeignKey("incidents.id", ondelete="CASCADE"), nullable=False),
        sa.Column("source_tool", sa.String(80), nullable=False),
        sa.Column("evidence_type", sa.String(80), nullable=False),
        sa.Column("observation", json_type, nullable=False),
        sa.Column("summary", sa.Text(), nullable=False),
        sa.Column("source_record_ids", json_type, nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False))
    op.create_table("investigations",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("incident_id", sa.String(36), sa.ForeignKey("incidents.id", ondelete="CASCADE"), nullable=False),
        sa.Column("provider", sa.String(40), nullable=False),
        sa.Column("status", sa.String(30), nullable=False),
        sa.Column("summary", sa.Text(), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True))
    op.create_table("remediation_proposals",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("incident_id", sa.String(36), sa.ForeignKey("incidents.id", ondelete="CASCADE"), nullable=False),
        sa.Column("action", sa.String(80), nullable=False),
        sa.Column("parameters", json_type, nullable=False),
        sa.Column("risk", sa.String(20), nullable=False),
        sa.Column("status", sa.String(30), nullable=False),
        sa.Column("rationale", sa.Text(), nullable=False),
        sa.Column("expected_outcome", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False))
    op.create_table("remediation_executions",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("proposal_id", sa.String(36), sa.ForeignKey("remediation_proposals.id"), nullable=False, unique=True),
        sa.Column("status", sa.String(30), nullable=False),
        sa.Column("pre_metrics", json_type, nullable=False),
        sa.Column("result", json_type, nullable=False),
        sa.Column("post_metrics", json_type, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False))
    op.create_table("approval_decisions",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("proposal_id", sa.String(36), sa.ForeignKey("remediation_proposals.id"), nullable=False),
        sa.Column("decision", sa.String(20), nullable=False),
        sa.Column("actor", sa.String(100), nullable=False),
        sa.Column("idempotency_key", sa.String(100), nullable=False, unique=True),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False))
    for table, column in (("incidents", "service_id"), ("incidents", "status"),
        ("incidents", "created_at"), ("metric_samples", "service_id"),
        ("metric_samples", "name"), ("metric_samples", "timestamp"),
        ("log_records", "service_id"), ("log_records", "severity"),
        ("log_records", "timestamp"), ("deployments", "service_id"),
        ("deployments", "timestamp"), ("incident_events", "incident_id"),
        ("incident_events", "event_type"), ("incident_events", "timestamp"),
        ("evidence_records", "incident_id"), ("evidence_records", "timestamp"),
        ("investigations", "incident_id"), ("remediation_proposals", "incident_id"),
        ("remediation_proposals", "status"), ("remediation_executions", "proposal_id"),
        ("approval_decisions", "proposal_id"), ("approval_decisions", "idempotency_key")):
        op.create_index(f"ix_{table}_{column}", table, [column])
    op.create_index("ix_event_incident_id_id", "incident_events", ["incident_id", "id"])

def downgrade() -> None:
    op.drop_table("approval_decisions")
    op.drop_table("remediation_executions")
    op.drop_table("remediation_proposals")
    op.drop_table("investigations")
    op.drop_table("evidence_records")
    op.drop_table("incident_events")
    op.drop_table("deployments")
    op.drop_table("log_records")
    op.drop_table("metric_samples")
    op.drop_table("incidents")
    op.drop_table("services")
