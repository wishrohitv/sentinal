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
- `POST /api/monitored/scenarios/db-pool-exhaustion` (real-mode local test only)
- `POST /api/demo/scenarios/db-pool-exhaustion`, `/worker-failure`, `/api/demo/reset`
- `POST /api/services/{service_id}/request` for one simulated request, `POST /api/demo/workload` for a bounded synthetic batch

## Configuration

See [`.env.example`](.env.example). Never commit `.env` or provider secrets. `AI_PROVIDER=demo` is the safe local default; Redis is not required.

## Real local monitoring environment

Sentinel can monitor a separate FastAPI + PostgreSQL application using actual runtime telemetry while retaining the original simulator. Select the environment independently from the AI provider:

```env
TELEMETRY_PROVIDER=prometheus_loki
AI_PROVIDER=demo
PROMETHEUS_URL=http://prometheus:9090
LOKI_URL=http://loki:3100
MONITORED_SERVICE_ID=monitored-api
MONITORED_APP_URL=http://monitored-app:8000
DB_POOL_SIZE=2
DB_MAX_OVERFLOW=0
DB_POOL_TIMEOUT=2
MAX_DB_POOL_SIZE=5
HTTP_ERROR_RATE_THRESHOLD=0.20
DB_TIMEOUT_RATE_THRESHOLD=0.10
SENTINEL_DB_POOL_THRESHOLD=0.90
SENTINEL_DETECTION_INTERVAL_SECONDS=5
VERIFICATION_WINDOW_SECONDS=30
VERIFICATION_POLL_INTERVAL_SECONDS=5
TELEMETRY_REQUEST_TIMEOUT_SECONDS=5
```

`AI_PROVIDER=demo` remains useful for credential-free end-to-end tests and is explicitly deterministic. For model-driven tool selection, set `AI_PROVIDER=nebius`, configure `NEBIUS_API_KEY`, `NEBIUS_BASE_URL`, and the Nemotron model name available to your account, then restart Compose. The model integration uses the OpenAI-compatible SDK and does not make external requests in demo mode.

The Compose environment adds `monitored-db`, `monitored-app`, Prometheus, Loki, and Alloy. The monitored app writes actual JSON logs to a shared named volume; Alloy forwards them to Loki. Prometheus scrapes the app's real `/metrics` endpoint. Sentinel queries Prometheus and Loki directly when `TELEMETRY_PROVIDER=prometheus_loki`. There is no fallback to simulator data if those services are unavailable. The monitored app is published on localhost port 8001 for this isolated demo, Prometheus on 9090 for debugging; Loki and the monitored PostgreSQL are internal-only. The local control token default is not appropriate outside an isolated workstation.

### Start and inspect the real environment

Set the values above in `.env` (retain the existing database and AI variables) and start the stack:

```sh
cp .env.example .env   # only if .env does not already exist; preserve existing credentials
# Set TELEMETRY_PROVIDER=prometheus_loki and other values in .env
# For actual model-driven tool selection also set AI_PROVIDER=nebius and credentials

docker compose up --build -d
```

Verify the monitored app and Prometheus target:

```sh
curl -fsS http://localhost:8001/health
curl -fsS http://localhost:8001/ready
curl -fsS http://localhost:8001/metrics | grep -E 'http_requests_total|db_pool|db_connection_timeouts'
curl -fsS 'http://localhost:9090/api/v1/targets?state=active'
curl -fsS http://localhost:8000/api/system/telemetry
```

Create normal, database-backed traffic (these are real monitored-app requests):

```sh
curl -fsS -X POST http://localhost:8001/api/orders \
  -H 'content-type: application/json' \
  -d '{"item":"demo order","quantity":1}'
curl -fsS http://localhost:8001/api/orders
```

After a request, check that Alloy has delivered logs to Loki:

```sh
START=$(date -u -d '10 minutes ago' +%Y-%m-%dT%H:%M:%SZ)
END=$(date -u +%Y-%m-%dT%H:%M:%SZ)
curl -G 'http://localhost:8000/api/services/monitored-api/logs' \
  --data-urlencode 'minutes=10' --data-urlencode 'limit=20'
```

That Sentinel endpoint queries Loki in real mode; it does not read simulator logs. Loki itself is internal-only. The backend telemetry status endpoint also reports Prometheus/Loki/application connectivity.

### Reproduce pool exhaustion and approve recovery

The test endpoint holds actual SQLAlchemy connections and is enabled only in the local Compose environment. You can start the controlled concurrent load with the real-mode UI button **Exhaust monitored DB pool** (backend endpoint `POST /api/monitored/scenarios/db-pool-exhaustion`), or issue requests directly. With pool size 2, run several concurrent requests, then issue a normal DB-backed request while the held connections are active:

```sh
for i in 1 2 3 4; do
  curl -sS 'http://localhost:8001/api/test/hold-db-connection?seconds=10' \
    -o "/tmp/sentinel-hold-$i.json" &
done
sleep 1
curl -i http://localhost:8001/api/orders
wait
```

The extra requests should cause real SQLAlchemy checkout timeouts, HTTP 503s, Prometheus counter changes, and JSON error logs. Sentinel polls Prometheus every configured interval, deduplicates the active incident, and starts the configured agent. In demo mode, its strategy is deterministic but all observations are still actual Prometheus/Loki/runtime observations. With configured Nemotron selected, tool selection is model-driven. Open the Sentinel UI at <http://localhost:5173> or inspect `GET /api/incidents`; the timeline and evidence panel show persisted tool calls and source-labeled results.

For a pool incident, the registered `increase_db_pool` proposal is medium risk. Review its parameters and evidence in the UI, then explicitly approve it. The backend calls only the monitored app's authenticated, bounded pool-resize endpoint. It pauses new DB sessions, drains active sessions, swaps to a tested engine, records the actual config change, and verifies recovery using fresh Prometheus metrics plus readiness. The incident is not resolved if verification fails or telemetry is missing. The application's configuration endpoint and remediation are not exposed as unrestricted host commands.

A repeatable clean pool scenario can be prepared by restarting the monitored app; its configured initial pool size is restored from the environment:

```sh
docker compose restart monitored-app
```

To return to the original simulator, set `TELEMETRY_PROVIDER=simulator` in `.env` and recreate the backend:

```sh
docker compose up -d --force-recreate backend
```

`docker compose down` stops services and preserves named volumes. `docker compose down -v` deletes persisted Sentinel, monitored database, Prometheus, Loki, log, and Alloy state; use it only when you intentionally want a destructive full reset.

### Troubleshooting real telemetry

- **No Prometheus target / no metrics:** check `docker compose ps`, `docker compose logs monitored-app prometheus`, and the target health at `http://localhost:9090/targets`. Prometheus scrapes `monitored-app:8000` on the Compose network, not `localhost`.
- **No Loki logs:** generate an order request, then inspect `docker compose logs alloy loki`. Alloy reads `/var/log/monitored-app/app.jsonl` from a shared named volume; Sentinel queries Loki at the internal `http://loki:3100` URL.
- **Sentinel telemetry unavailable:** ensure `TELEMETRY_PROVIDER=prometheus_loki` and the internal service URLs are correct. Telemetry errors intentionally surface as unavailable/503; they are never replaced with simulator observations.
- **No incident detected:** confirm the pool is saturated and that 5xx or timeout rates exceed the configured thresholds in Prometheus. Rates use a rolling query window, so allow scraping/rate windows to collect samples and trigger overlapping requests.
- **Nemotron errors:** check API key, configured base URL, model identifier enabled for the account, and backend logs. A missing key/model uses the deterministic demo provider; it does not claim model autonomy.
- **Remediation/verification failure:** inspect the incident execution result and backend/monitored-app logs. The app control token must match on both services, requested size cannot exceed `MAX_DB_POOL_SIZE`, and verification requires fresh Prometheus data and ready service state.

See [`architecture.md`](architecture.md) for data flow, trust boundaries, policy, and implemented limitations. Real-mode worker remediation and orchestrator-based deployment discovery are not implemented; the existing worker scenario remains simulator-only.
