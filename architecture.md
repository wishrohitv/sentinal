# Sentinel Architecture

## Purpose

Sentinel is a local incident-response system that turns observed service failures into an auditable investigation and a policy-controlled remediation. The initial use case is database connection-pool exhaustion. The goal is to demonstrate a safe end-to-end workflow—not to provide unrestricted production access or an autonomous chatbot.

## Two isolated telemetry environments

The `TELEMETRY_PROVIDER` setting selects one of two backends for the same detection and investigation workflows:

- `simulator` (default): Sentinel's original deterministic simulator. It persists synthetic-but-runtime-generated metrics, logs, deployment changes, and incidents in Sentinel's database. Its scenario controls and simulated remediation remain available.
- `prometheus_loki`: a separate FastAPI monitored application, using its own PostgreSQL database and actual SQLAlchemy connection pool. Prometheus scrapes its exported metrics; Alloy reads its JSONL application log volume and forwards records to Loki. Sentinel queries these services over their HTTP APIs. If either telemetry service is unavailable or data is missing, Sentinel reports the failure or missing observation; it does not fall back to simulator data.

The two environments share Sentinel's incident schema, policy, event history, evidence records, frontend, and historical incident search. Each real observation records its source (Prometheus, Loki, or monitored-app runtime). Detection is threshold-based Python code; the model is not asked to decide whether a measured threshold is exceeded.

## Real monitoring Compose path

```text
Monitored FastAPI app ── SQLAlchemy pool ── monitored PostgreSQL
        │                         │
        ├── /metrics ── Prometheus ───────────┐
        └── JSONL file ── Alloy ── Loki ──────┤
                                              ▼
                                  Sentinel telemetry adapters
                                              │
                                 detector + investigation tools
                                              │
                                 demo agent or Nemotron provider
                                              │
                                  deterministic action policy
                                              │
                     authenticated, allowlisted monitored-app API
                                              │
                              fresh Prometheus/readiness verification
```

Sentinel does not inspect application process memory, read its filesystem, issue Docker commands, or access its database. The only control integration is the monitored application's explicit, bearer-token-protected pool reconfiguration endpoint. The application's internal control endpoints are not published as separate Compose ports; the monitored API is bound to localhost for the test demo.

## Telemetry details

### Metrics

The monitored API instruments HTTP request/error counters and latency histogram in ASGI middleware. SQLAlchemy pool `connect`, `checkout`, and `checkin` events update gauges/counters; SQLAlchemy pool checkout timeout handling increments a timeout counter and logs a structured error. The `/metrics` endpoint is scraped by Prometheus at a five-second interval.

The Sentinel Prometheus adapter uses the HTTP instant and range query APIs for request rate, HTTP 5xx ratio, p95 latency, pool size/usage, checkout timeout rate, CPU, and memory. Empty series remain `null`; malformed responses and request failures raise an explicit telemetry error. The detector stores actual sampled values as Sentinel observations and correlates elevated 5xx/timeout signals with pool saturation before opening a pool incident.

### Logs

The monitored application writes newline-delimited JSON records to the shared `monitored_app_logs` volume. Alloy tails only that path, parses useful JSON fields, and sends the records to Loki. Sentinel's Loki adapter queries by service and time window and preserves timestamps, levels, messages, and structured fields. Invalid JSON log lines are retained as plain messages rather than discarded.

## AI agent and trust boundary

`AI_PROVIDER=demo` selects the explicitly deterministic demo agent. It invokes the same allowlisted Python tools and persists real tool results as evidence, but its investigation strategy and diagnosis are deterministic; it is not autonomous model reasoning.

When `AI_PROVIDER=nebius` (or the supported Nemotron setting) and the required API key and configured model name are present, Sentinel uses the existing OpenAI-compatible SDK integration. The model selects registered investigation tools dynamically. Tool names, arguments, call count, and investigation duration are bounded and validated by the backend. The model receives telemetry as untrusted data and can propose a diagnosis/action only. No model-generated SQL, shell, Python, filesystem access, or arbitrary HTTP target is executed.

Every evidence record belongs to one incident and is created by a backend tool from a real simulator query or selected telemetry provider. Diagnosis references are validated against those records. The UI labels diagnoses as hypotheses, distinct from observed evidence.

## Policy and remediation

The action registry validates the action and parameters independently from the model:

- `increase_db_pool` is medium risk and requires human approval.
- `restart_worker` and `retry_failed_jobs` remain simulator-only low-risk actions under their registered policy.
- High-risk schema/data actions are blocked; no destructive executor is implemented.

In real telemetry mode, the only implemented action is `increase_db_pool`. The monitored application accepts a bounded new pool size (local default maximum 5), pauses entry into new DB sessions, waits for active sessions to drain, creates and tests a replacement engine, then swaps it in and records the configuration change. It does not execute shell commands or modify external infrastructure.

After an action, Sentinel queries fresh Prometheus values and the monitored application's readiness endpoint within a bounded verification window. Only passing recovery criteria resolve the incident. Failed actions or verification remain escalated and are persisted with pre/post metrics and timeline events.

## Persistence and events

Sentinel's PostgreSQL database stores services, actual sampled metrics, incidents, investigation records, evidence, remediation proposals/executions, approvals, and timeline events. The backend streams persisted events over SSE and supports replay from an event ID. Historical incident search uses structured PostgreSQL queries; there is no vector database or embedding service.

The monitored application has a separate PostgreSQL database and credentials. Prometheus, Loki, and the monitored DB are internal Compose services. Prometheus is published on localhost for debugging; Loki is not published to the host. Redis, Kafka, Kubernetes, and distributed workers are intentionally not used.

## Configuration

- `TELEMETRY_PROVIDER=simulator` or `prometheus_loki`
- `PROMETHEUS_URL=http://prometheus:9090`
- `LOKI_URL=http://loki:3100`
- `MONITORED_SERVICE_ID=monitored-api`
- `MONITORED_APP_URL=http://monitored-app:8000`
- `AI_PROVIDER=demo` or `nebius`
- `NEBIUS_API_KEY`, `NEBIUS_BASE_URL`, and `NVIDIA_MODEL_NAME` for a configured OpenAI-compatible Nemotron endpoint/model
- `DB_POOL_SIZE=2`, `DB_MAX_OVERFLOW=0`, `DB_POOL_TIMEOUT=2`, and `MAX_DB_POOL_SIZE=5` for the isolated pool test

Use `.env.example` as the environment template and never commit `.env` or provider credentials. The local control-token fallback is only suitable for this isolated development Compose environment; replace it for any shared deployment.

## Current limits

This milestone implements actual metrics/logs, pool exhaustion, bounded pool resizing, and recovery checks. Real-mode worker remediation, automatic deployment discovery from an orchestrator, Grafana dashboards, authentication for the Sentinel UI/API, and production-grade multi-process metric aggregation are not implemented. The real Nemotron flow requires a valid credential and model configuration and must be tested against the provider separately; deterministic mode is credential-free.
