# Sentinel — Autonomous AI Reliability Engineer MVP

Sentinel is a local, isolated incident-response system for the NVIDIA × Nebius AI Hackathon. It combines a stateful production simulator, deterministic metric detection, a bounded evidence-collecting agent, a deterministic action policy, human approval for medium-risk changes, fresh-metric verification, a persistent incident ledger, and a live React interface.

The default provider is explicitly labeled **Deterministic demo agent**. It uses the same DB-backed investigation tools, evidence ledger, policy and remediation executors as the configured Nemotron provider; no API credential is required for local demo mode.

## Start with Docker Compose

Requirements: Docker Engine with Docker Compose v2.

```sh
cp .env.example .env
docker compose up --build
```

Open:

- UI: <http://localhost:5173>
- API docs: <http://localhost:8000/docs>
- API health: <http://localhost:8000/api/health>

Compose starts PostgreSQL, applies Alembic migrations, and then starts FastAPI and Vite. PostgreSQL data is retained in the `postgres_data` volume. Redis is intentionally omitted; PostgreSQL is both the source of persisted events and the SSE replay store.

## Database Pool Exhaustion Demo

1. Open the overview and verify normal request/error/pool/worker telemetry.
2. Click **DB pool exhaustion**. The simulator changes its in-process pool limit, records a deployment/configuration update, generates a workload, and persists its resulting metrics and structured timeout logs.
3. The deterministic detector evaluates the configured rolling window and creates an incident. The selected provider begins an investigation automatically.
4. Open the incident. The persistent SSE stream shows actual tool arguments/results and evidence IDs; it does not expose hidden chain-of-thought.
5. The agent collects logs, metric samples, current service health, deployment/config diffs, and matching resolved incident history. The diagnosis references evidence records belonging to this incident.
6. `increase_db_pool` is medium risk. Review the structured proposal and click **Approve & execute** (or reject it).
7. The registered action changes simulator state and generates a new workload. Sentinel resolves the incident only if fresh 5xx, pool, and timeout metrics pass verification.
8. After resolution, trigger the same scenario again; `search_previous_incidents` returns the prior resolved incident from PostgreSQL.

The **Worker failure** control is a second scenario. It injects a worker heartbeat failure and failed-job backlog. Its evidence-backed `restart_worker` proposal is low risk and is executed automatically when policy requirements pass; the action is verified against the resulting worker-health metric. A registered `retry_failed_jobs` action operates only on the simulator's eligible failed jobs and requires a healthy worker.

**Reset simulator** restores simulator state and resets its metric/log/deployment observations. It does not delete incident history or fabricate a resolved outcome.

## Optional Nebius Token Factory / NVIDIA Nemotron

The provider uses the Token Factory OpenAI-compatible chat-completions API. Official references:

- [Nebius Token Factory quickstart](https://docs.tokenfactory.nebius.com/quickstart)
- [OpenAI-compatible API / switching guide](https://docs.tokenfactory.nebius.com/switch)

The documented default API base URL is `https://api.tokenfactory.nebius.com/v1/`; it is configurable. Use the model identifier and region endpoint shown for the Nemotron model you have enabled in Token Factory. Sentinel intentionally does **not** hardcode a Nemotron model ID or send requests unless the user selects Nemotron and both the key and model are configured.

Set in `.env` before starting Compose:

```env
NEBIUS_API_KEY=your_token_factory_key
NEBIUS_BASE_URL=https://api.tokenfactory.nebius.com/v1
NVIDIA_MODEL_NAME=the-model-id-enabled-in-your-token-factory-project
AI_PROVIDER=demo
```

The UI will enable the model provider selector when the configured key and model are present. Provider calls are backend-only through the OpenAI Python SDK, using `NEBIUS_BASE_URL` as the configurable OpenAI-compatible endpoint and `NEBIUS_API_KEY` / `NVIDIA_MODEL_NAME` for authentication and model selection. The same client works with compatible chat-completions endpoints when those existing settings are configured accordingly. Calls have bounded timeouts, a maximum tool-call count, and an investigation deadline. Tool names and Pydantic arguments are validated server-side; malformed, unknown, or cross-service calls are rejected and recorded. Model output can propose actions only. The backend policy and action registry own execution.

## Local development without Docker

Python 3.11+ and Node 20+ are suitable. For PostgreSQL-free local development, use SQLite:

```sh
cd backend
python -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
DATABASE_URL=sqlite+aiosqlite:///./sentinel.db alembic upgrade head
DATABASE_URL=sqlite+aiosqlite:///./sentinel.db uvicorn app.main:app --reload
```

In another terminal:

```sh
cd frontend
npm ci
npm run dev
```

## Tests

```sh
cd backend
pip install -r requirements.txt
pytest -q
```

The test suite uses in-memory SQLite and the deterministic provider; it does not call Nebius or require PostgreSQL credentials.

## Architecture and safety boundaries

- **Backend:** FastAPI, Pydantic v2, SQLAlchemy 2 async ORM, PostgreSQL in Compose, Alembic migrations. SQLite is supported for local/test use.
- **Simulator:** State changes are isolated to the demo service. Generated request batches persist metric samples and structured logs. No action changes real infrastructure.
- **Detection:** Python thresholds and rolling-window calculations; the model does not decide whether a metric breaches a threshold.
- **Agent tools:** `query_metrics`, `query_logs`, `inspect_deployment`, `inspect_configuration_changes`, `inspect_service_health`, and `search_previous_incidents`. Each output is persisted as incident-scoped evidence.
- **Policy:** `increase_db_pool` requires a human approval; `restart_worker` and `retry_failed_jobs` may auto-execute only if registered action parameters, evidence, confidence, and incident state pass policy. High-risk `database_schema_change` and `delete_data` are always blocked. Confidence alone never authorizes an action.
- **Executor and verification:** No shell, `eval`, unrestricted SQL, or arbitrary model-generated execution. Remediation functions are explicit registry entries. A failed verification leaves the incident unresolved/escalated and prevents automatic retries.
- **Timeline/SSE:** Incident events are persisted in PostgreSQL and replayed by event ID from `/api/incidents/{id}/stream`.
- **Memory:** Resolved incidents are searchable with structured PostgreSQL queries; there are no embeddings or external vector services.

## Main API endpoints

- `GET /api/health`, `GET /api/system/provider`, `POST /api/system/provider`
- `GET /api/services`, `GET /api/services/{service_id}/health`, `/metrics`, `/logs`
- `GET /api/incidents`, `GET /api/incidents/{id}`, `/events`, `/stream`, `/evidence`, `/remediations`
- `POST /api/incidents/{id}/investigate`, `/approve`, `/reject`
- `POST /api/demo/scenarios/db-pool-exhaustion`, `/worker-failure`, `/api/demo/reset`
- `POST /api/services/{service_id}/request` for one simulated request, `POST /api/demo/workload` for a bounded synthetic batch

## Configuration

See [`.env.example`](.env.example). Never commit `.env` or provider secrets. `AI_PROVIDER=demo` is the safe local default; Redis is not required.
