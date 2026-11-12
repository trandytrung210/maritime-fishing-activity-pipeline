# Maritime Fishing Activity Pipeline

PySpark medallion (Bronze → Silver → Gold) pipeline for daily vessel fishing activity, orchestrated with Apache Airflow 3, object storage on SeaweedFS (S3 API), and GitHub Actions
CI/CD.

## What this project does

1. Ingest raw daily CSV files into local Bronze storage and/or S3 (`fishing-bronze`).
2. Transform Bronze → Silver with data-quality rules, quarantine for invalid rows, and incremental reprocessing.
3. Detect which source dates changed and which months Gold must rebuild.
4. Build Gold datasets:
   - Daily vessel activity
   - Monthly aggregations
   - Daily vessel-hours DQ
   - Area fishing activity (grid-cell metrics; full refresh when Silver changes)
5. Validate Silver and Gold layers before upload.
6. Upload verified Parquet to S3 (`fishing-silver`, `fishing-gold`) with SHA-256, staging, read-back verification, and audit logs.
7. Finalize pending Gold change tracking only after a successful Gold upload.
8. Generate interactive Gold analytics dashboards (PyArrow + Plotly HTML) for daily, monthly, area, and hours-DQ views.

## Architecture

```text
Raw CSV (Bronze)
    │
    ▼
Silver (Parquet + DQ + quarantine)
    │
    ├─► pending_gold_changes.json
    │         │
    │         ▼
    │   detect_gold_changes → gold_changes.json (per DAG run)
    │         │
    │         ├─► daily / monthly / area Gold (incremental or full refresh)
    │         ├─► validators
    │         ├─► upload_gold
    │         └─► finalize_gold_changes
    │
    └─► upload_silver
              │
              ▼
    Gold dashboards (PyArrow → Plotly HTML)

Object store: SeaweedFS S3 gateway
Orchestration: Airflow 3 (LocalExecutor)
Compute: PySpark 4.x + Java 21
Analytics UI: dashboard/ (Plotly HTML; optional Streamlit)
```

### Medallion layers

| Layer | Location (local) | S3 bucket | Contents |
|---|---|---|---|
| Bronze | `data/raw/` | `fishing-bronze` | Daily CSV (`YYYY-MM-DD.csv`) |
| Silver | `data/processed/silver/` | `fishing-silver` | Cleaned Parquet + quarantine |
| Gold | `data/processed/gold/` | `fishing-gold` | Aggregated analytics tables |

### Key design choices

- Incremental Silver: skip unchanged raw files via fingerprint manifest; optional force reprocess.
- Pending Gold changes: successful Silver dates are upserted; Gold rebuilds only affected months when possible.
- Area Gold: full refresh only when Silver has changes for the run.
- Safe uploads: SHA-256 metadata, idempotent skip, staging → verify → promote, stale cleanup only after success.
- Manifest locking: `portalocker` for concurrent-safe pending/silver/gold JSON updates.
- Dev vs deploy images:
  - Dev: `airflow/Dockerfile` + bind-mount project source
  - Deploy/CD: `Dockerfile.deploy` bakes `src`, `dags`, and `include`

## Repository layout

```text
.
├── airflow/                  # Airflow 3 Docker stack + DAG
│   ├── dags/                 # maritime_fishing_pipeline.py
│   ├── include/run_script.py # Runs src/* scripts with Airflow run id
│   ├── Dockerfile            # Dev image (Java + Python deps)
│   ├── docker-compose.yml    # Local Airflow (source mounted)
│   └── docker-compose.staging.yml
├── infrastructure/           # SeaweedFS Compose (+ CI variant)
├── src/
│   ├── common/               # manifest, spark_io
│   ├── silver/               # ingest, silver, validate, upload
│   └── gold/                 # detect, build, validate, upload, finalize
├── dashboard/                # Gold analytics dashboards (PyArrow + Plotly)
│   ├── generate.py           # Build HTML reports under reports/dashboards/
│   ├── app.py / pages/       # Optional Streamlit UI
│   └── lib/                  # Data loaders + chart helpers
├── test/                     # Unit + SeaweedFS integration tests
├── scripts/deploy_staging.sh
├── Dockerfile.deploy         # Baked image for GHCR / staging
├── .github/workflows/        # ci.yml, cd.yml
├── requirements.txt
└── requirements-dev.txt
```

## Prerequisites

- Python 3.12+
- Java 21 (for local PySpark)
- Docker + Docker Compose
- (Optional) GitHub repo for CI/CD and GHCR

## Quick start (local)

### 1. Python environment

```bash
python -m venv .venv
# Windows: .venv\Scripts\activate
# Linux/macOS: source .venv/bin/activate
pip install -r requirements-dev.txt
```

Copy env templates:

```bash
cp .env.example .env
cp airflow/.env.example airflow/.env
```

Fill SeaweedFS keys and Airflow secrets in those files. Generate Airflow secrets (example):

```bash
python -c "import base64,os; print(base64.urlsafe_b64encode(os.urandom(32)).decode())"
python -c "import secrets; print(secrets.token_urlsafe(64))"
```

### 2. SeaweedFS (S3)

Create the shared network once:

```bash
docker network create maritime-shared
```

Configure `infrastructure/s3.json` (gitignored; mirror `infrastructure/s3.ci.json` for local keys), then:

```bash
cd infrastructure
docker compose up -d
```

S3 endpoint (host): `http://127.0.0.1:8333`  
Create buckets:

```bash
python src/create_bucket.py
```

Buckets: `fishing-bronze`, `fishing-silver`, `fishing-gold`.

### 3. Airflow (dev)

```bash
cd airflow
docker compose build
docker compose up -d
```

UI: [http://localhost:8080](http://localhost:8080)

The DAG `maritime_fishing_pipeline` mounts the repo at `/opt/maritime_project` and runs scripts under `src/` via `include/run_script.py`.

### 4. Gold analytics dashboards

After Gold Parquet exists under `data/processed/gold/`, generate interactive HTML dashboards (PyArrow reads + Plotly charts; no Spark):

```bash
pip install -r dashboard/requirements.txt
python dashboard/generate.py
```

Open [`reports/dashboards/index.html`](reports/dashboards/index.html) in a browser.

| Page | Gold source | What it shows |
|---|---|---|
| Overview | monthly inventory | Fishing hours vs vessels by month; Gold row counts |
| Daily Vessel | `daily_vessel_activity` | Latest month: daily trend + top vessels |
| Monthly Activity | `monthly_vessel_activity` | Month trends + ranked vessels (fishing hours / active days) |
| Area Fishing | `area_fishing_activity` | Hot grid cells (scatter + rankings) |
| Hours DQ | `reports/daily_vessel_hours_dq` | NORMAL / INFO / REVIEW mix from daily hours rules |

**Chart controls (Plotly):** hover for values, drag to zoom, double-click to reset, legend click to hide/show series, camera icon to export PNG.

Re-run `python dashboard/generate.py` whenever Gold (or the DQ summary) is rebuilt. Output under `reports/dashboards/` is gitignored with the rest of `reports/`.

Optional Streamlit UI (`streamlit run dashboard/app.py`) — see [`dashboard/README.md`](dashboard/README.md). Prefer HTML generation if Streamlit conflicts with the pinned pandas version.

### 5. Run the pipeline

Trigger `maritime_fishing_pipeline` manually in the Airflow UI (schedule is `None`).

High-level task order:

1. `ingest_bronze`
2. `build_silver` → `validate_silver` → `upload_silver`
3. `detect_gold_changes`
4. Gold builds (daily / monthly / area) → validators
5. `upload_gold` (only if all required validators pass)
6. `finalize_gold_changes`

## Incremental processing notes

- Silver state: `reports/silver_processing_manifest.json`
- Pending Gold dates: `reports/pending_gold_changes.json`
- Per-run Gold plan: `reports/runs/<run_id>/gold_changes.json` (+ latest pointer `reports/gold_changes.json`)
- Force Silver reprocess via `.env`:

```env
SILVER_FORCE_REPROCESS=          # normal incremental
SILVER_FORCE_REPROCESS=all       # all dates
SILVER_FORCE_REPROCESS=2020-01-01,2020-01-02
```

If Silver has no new raw files, Spark is not started, but Gold may still run from leftover pending dates.

## Testing

```bash
# Unit tests (default; skips live SeaweedFS)
pytest -q -m "not integration"

# Lint
ruff check src airflow/dags test

# SeaweedFS integration (CI compose)
cd infrastructure
docker compose -f docker-compose.ci.yml up -d --wait
cd ..
REQUIRE_SEAWEEDFS=1 pytest -q -m integration test/test_seaweedfs_integration.py
```

`test/test_pipeline.py` is a local investigation script (ignored by pytest).

## CI / CD

### CI (`.github/workflows/ci.yml`)

On pull requests and pushes to `main` / `master`:

1. lint-and-test — Ruff + unit pytest (Java 21 for PySpark tests)
2. docker-build — Build `airflow/Dockerfile`, inspect image, smoke-run Java/PySpark/Airflow
3. seaweedfs-integration — Start CI SeaweedFS stack, run S3 auth/upload/download/checksum/delete tests

### CD (`.github/workflows/cd.yml`)

1. Publish deploy image (`Dockerfile.deploy`) to GHCR when CI succeeds on `main`/`master`  
   Tags: `latest`, `sha-<commit>`
2. Deploy staging (optional, manual `workflow_dispatch`) over SSH using secrets

### Staging deploy (manual on a host)

```bash
export AIRFLOW_IMAGE=ghcr.io/<owner>/maritime-fishing-airflow:latest
./scripts/deploy_staging.sh
```

Or:

```bash
cd airflow
docker compose -f docker-compose.staging.yml pull
docker compose -f docker-compose.staging.yml up -d
```

Staging mounts data/reports/logs only; application code comes from the image.

## Main modules

| Path | Role |
|---|---|
| `src/silver/ingest.py` | Bronze ingest to S3 with verification |
| `src/silver/silver.py` | Incremental Silver build + DQ + pending Gold |
| `src/silver/validate_silver.py` | Silver validation |
| `src/silver/upload_silver.py` | Silver → S3 upload |
| `src/gold/detect_gold_changes.py` | Build per-run `gold_changes.json` |
| `src/gold/build_gold/*` | Daily / monthly / area / hours-DQ builders |
| `src/gold/validate/*` | Gold validators |
| `src/gold/upload_gold.py` | Gold → S3 upload |
| `src/gold/finalize_gold_changes.py` | Clear pending after successful upload |
| `src/common/manifest.py` | Shared manifests, locks, run ids |
| `src/common/spark_io.py` | Safe Parquet writes (tmp stage for Windows mounts) |
| `dashboard/generate.py` | Build Plotly HTML dashboards from Gold Parquet |
| `dashboard/lib/data.py` | PyArrow loaders / aggregations for dashboard pages |
| `dashboard/app.py` | Optional Streamlit entrypoint |

## Configuration reference

| Variable | Where | Purpose |
|---|---|---|
| `S3_ENDPOINT` | `.env` / Airflow | SeaweedFS S3 URL |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | `.env` / Airflow | S3 credentials |
| `SILVER_FORCE_REPROCESS` | `.env` | Force Silver dates |
| `MARITIME_PROJECT_ROOT` | Airflow | Project root inside container |
| `AIRFLOW_IMAGE` | Staging | GHCR image tag to run |
| `REQUIRE_SEAWEEDFS` | Tests/CI | Fail (don’t skip) if S3 is down |

## License / notes

Internal data-engineering project for maritime fishing activity analytics. Do not commit `.env`, `infrastructure/s3.json`, raw `data/`, or secrets.
