# Healthcare Claims Data Engineering Pipeline

An end-to-end batch data platform for synthetic healthcare claims. It ingests CSV files, validates and transforms records, preserves rejects, loads PostgreSQL through staging, exposes read-only analytics through a REST API and Streamlit dashboard, and records operational lineage and alerts.

Only synthetic data belongs in this repository. Do not place real patient information or protected health information in `data/`, logs, API responses, or screenshots.

## What is complete

1. **Airflow orchestration** — a thin, retryable DAG calls the same tested pipeline stages used by the CLI.
2. **Configuration-driven validation** — strict YAML rules are validated before processing and executed as vectorized pandas checks.
3. **Streamlit dashboard** — shared analytics queries power KPI, trend, quality, run, alert, and claim-lineage views.
4. **REST API** — versioned, read-only FastAPI endpoints provide claim summaries, operational metrics, quality data, and lineage.
5. **Data lineage and alerting** — file hashes, source rows, run IDs, history, OpenLineage events, threshold alerts, webhooks, and email are supported.
6. **Idempotent processing** — file-level SHA-256 controls and claim-level upserts make retries and reruns safe.

## Architecture

```text
CSV files in data/incoming
          |
          v
Airflow sensor / CLI + PostgreSQL advisory lock
          |
          v
Ingestion: schema check, file hash, source row and run metadata
          |
          v
Declarative validation from config/validation_rules.yaml
       /                                      \
valid records                              rejected records
       |                                      |
       v                                      v
transformation                         rejected_claims
       |
       v
stg_healthcare_claims -- transaction --> fact_claims --\
                                      history + lineage  |
                                                         v
                                  API / dashboard / SQL analytics
```

The load transaction writes staging rows, rejected rows, fact upserts, fact history, source-file status, and duplicate flags atomically. Files are archived only after the transaction, reconciliation, and alert stage complete.

## Repository layout

```text
config/                         Runtime and validation configuration
dags/                           Airflow 3.x orchestration
dashboard/                      Streamlit application
data/{incoming,processed,...}/  Input, reports, rejects, and archives
src/pipeline.py                 CLI and reusable stage orchestration
src/ingestion.py                File discovery and lossless CSV reading
src/rules.py, validation.py     Rule schema and validation engine
src/transformation.py, loading.py ETL and PostgreSQL merge
src/analytics.py                Shared read-only query layer
src/api/                        FastAPI application and response models
src/lineage.py, alerting.py     Lineage events and alert delivery
src/audit.py, database.py        Logging, audit, transactions, migrations
sql/                            Schema, migrations, indexes, reference SQL
tests/                          Unit, API, lineage, alerting, and DB tests
```

## Quick start

```bash
cp .env.example .env
# Set POSTGRES_USER, POSTGRES_PASSWORD and POSTGRES_DB in .env
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
docker compose up -d postgres
python3 -m src.database
```

Generate synthetic input if needed, then run the batch pipeline:

```bash
python3 generate_claims.py --help
python3 -m src.pipeline
```

Quality reports are written below `data/processed/<run_id>/`. Successful inputs move to `data/archive/<run_id>/`; rejected files move to `data/rejected/<run_id>/`.

```bash
python3 -m src.pipeline --help
python3 -m src.pipeline --force-reprocess
```

`--force-reprocess` bypasses the successful-file SHA-256 skip check. It does not bypass validation or database constraints.

## 1. Airflow orchestration

Airflow is isolated in Docker because its dependency constraints are much larger than the pipeline runtime:

```bash
docker compose --profile airflow up -d
```

Open <http://localhost:8080>, enable `healthcare_claims_pipeline`, and trigger it. The DAG is hourly when enabled and paused by default.

Task flow:

```text
wait_for_files -> start_run -> process -> reconcile -> quality_alerts -> archive -> finish
                                                     \------------------------------/
```

The sensor uses reschedule mode and soft-fails when no files arrive. `process` has exponential-backoff retries. `max_active_runs=1` prevents overlapping file ownership, while a PostgreSQL advisory lock protects CLI/cron executions. Retry attempts reuse a deterministic run ID, so staging and audit records are replaced safely instead of duplicated.

For production, split the standalone container into Airflow scheduler/API/DAG processor/workers, use a managed metadata database, add secret management, and replace local bind mounts with durable storage.

## 2. Configuration-driven validation

Runtime settings are in `config/settings.yaml`; row-level rules are in `config/validation_rules.yaml`. The rule file is checked against a strict schema before processing. Supported rule types include `required`, `must_be_empty`, `date_parseable`, `not_future_date`, `numeric`, `compare`, and `allowed_values`.

Rules can be `reject` rules, which route rows to `rejected_claims`, or `warn` rules, which keep the row and increment the quality summary. Each rejected row retains all matching rule codes, its raw record, source filename, source row number, run ID, and rejection timestamp.

To add a rule, update the YAML, update any matching PostgreSQL constraint, add a focused test, and run `pytest -q`. Thresholds such as `high_value_threshold`, `late_submission_days`, rejection rate, duplicate rate, and volume ratio are configuration values rather than hardcoded business logic.

## 3. Streamlit dashboard

```bash
streamlit run dashboard/app.py
```

The dashboard uses `src/analytics.py`, the same query layer used by the API, so metrics do not diverge. Results are cached for 60 seconds; use **Refresh data** after a pipeline run.

It includes overview KPIs and trends, provider/denial analysis, high-value claims and duplicate groups, rejection trends and active rules, pipeline runs and alerts, and claim-level lineage from source row through fact history.

Dashboard queries use read-only database connections. Protect the deployment environment when exposing it beyond a developer machine.

## 4. REST API for claims summaries

```bash
uvicorn src.api.main:app --reload --port 8000
```

Interactive docs: <http://localhost:8000/docs>. `/health` is unauthenticated for load balancers. If `API_KEY` is set in `.env`, every `/api/v1` request requires `X-API-Key`.

Important endpoints:

```text
GET /api/v1/claims/summary       GET /api/v1/claims/by-status
GET /api/v1/claims/monthly       GET /api/v1/claims/processing-time
GET /api/v1/denials/top          GET /api/v1/providers/top
GET /api/v1/claims/{claim_id}    GET /api/v1/claims/high-value
GET /api/v1/claims/duplicates    GET /api/v1/pipeline/runs
GET /api/v1/alerts               GET /api/v1/quality/rejections
GET /api/v1/quality/rejected-records
GET /api/v1/quality/rules         GET /api/v1/lineage/claims/{claim_id}
```

Summary endpoints accept date, claim type, insurance plan, status, and provider filters. Pagination is bounded by `api.max_page_size`; SQL values are bound parameters and provider sorting uses a fixed allow-list.

```bash
curl -H "X-API-Key: $API_KEY" \
  'http://localhost:8000/api/v1/claims/summary?claim_status=Paid'
```

The API never writes to claim tables. Database failures return `503` with a generic client message while details remain in server logs.

## 5. Data lineage and alerting

Lineage is captured at three levels:

- **Row level:** `source_filename`, `source_row_number`, and `pipeline_run_id` travel through staging, rejects, and facts.
- **File level:** `source_files` stores SHA-256, size, row count, status, errors, and archive path.
- **Run/history level:** `pipeline_audit` records counts and status; `fact_claims_history` records inserts and real claim changes; OpenLineage events describe run inputs and outputs.

Trace a claim through the API or dashboard:

```bash
curl 'http://localhost:8000/api/v1/lineage/claims/CLM00000001'
```

OpenLineage transport in `settings.yaml` can be `file` (offline JSON events below `logs/lineage/`), `http` (POST to `OPENLINEAGE_URL` with file fallback), or `none` (database lineage remains enabled).

Every alert is stored in `pipeline_alerts` and logged. Built-in alerts cover high rejection rate, high duplicate rate, low volume against recent successful runs, rejected input files, and failed runs. Webhook and email delivery are opt-in and channel failures are isolated from the pipeline.

## Data model and idempotency

The principal tables are `stg_healthcare_claims`, `fact_claims`, `rejected_claims`, `pipeline_audit`, `source_files`, `fact_claims_history`, and `pipeline_alerts`. The loader scopes staging/reject cleanup to the run ID, upserts claims on `claim_id`, records history only for inserts or actual changes, and recomputes duplicate flags across the full fact history. A rerun reports unchanged rows instead of creating duplicate facts.

## Testing and verification

```bash
pytest -q
python3 -m compileall -q src dags dashboard
docker compose config
```

The suite covers ingestion, declarative rules, validation, transformation, loading/idempotency, rollback, API contracts, lineage transports, alert thresholds, and end-to-end behavior. Tests marked `db` run when PostgreSQL from `.env` is reachable; otherwise they are skipped with an explicit reason.

Before deployment, run `python3 -m src.database`, verify a sample run in the dashboard/API, inspect `pipeline_audit`, and confirm that input files move only after a successful load.

## Operational safeguards

- Keep credentials and API/notification secrets in `.env` or a deployment secret manager; never commit `.env`.
- Use synthetic data only in development and training.
- Review database constraints and validation rules together when changing financial or status semantics.
- Keep lineage logs and audit tables access-controlled.
- For large volumes, move raw files to object storage, process in chunks, and consider partitioning `fact_claims` by service date.
