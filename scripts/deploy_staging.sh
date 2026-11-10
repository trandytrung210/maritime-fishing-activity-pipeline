#!/usr/bin/env bash
# Deploy baked Airflow image to staging (run on the target host).
#
# Required env:
#   AIRFLOW_IMAGE   e.g. ghcr.io/<owner>/maritime-fishing-airflow:sha-abc1234
# Optional:
#   AIRFLOW_DIR     path to airflow/ folder (default: ./airflow)
#
# Example:
#   export AIRFLOW_IMAGE=ghcr.io/myuser/maritime-fishing-airflow:latest
#   ./scripts/deploy_staging.sh

set -euo pipefail

AIRFLOW_DIR="${AIRFLOW_DIR:-airflow}"
COMPOSE_FILE="${AIRFLOW_DIR}/docker-compose.staging.yml"

if [[ -z "${AIRFLOW_IMAGE:-}" ]]; then
  echo "ERROR: set AIRFLOW_IMAGE to the GHCR tag to deploy" >&2
  exit 1
fi

if [[ ! -f "${COMPOSE_FILE}" ]]; then
  echo "ERROR: compose file not found: ${COMPOSE_FILE}" >&2
  exit 1
fi

if [[ ! -f "${AIRFLOW_DIR}/.env" ]]; then
  echo "ERROR: missing ${AIRFLOW_DIR}/.env (copy from .env.example)" >&2
  exit 1
fi

echo "Ensuring docker network maritime-shared exists..."
docker network create maritime-shared 2>/dev/null || true

echo "Pulling ${AIRFLOW_IMAGE}..."
docker pull "${AIRFLOW_IMAGE}"

echo "Starting staging stack..."
docker compose \
  --project-name maritime-staging \
  -f "${COMPOSE_FILE}" \
  --env-file "${AIRFLOW_DIR}/.env" \
  up -d

echo "Waiting for Airflow API health..."
for i in $(seq 1 60); do
  if curl -sf "http://127.0.0.1:${AIRFLOW_UI_PORT:-8080}/api/v2/monitor/health" >/dev/null; then
    echo "Staging Airflow is healthy."
    echo "UI: http://127.0.0.1:${AIRFLOW_UI_PORT:-8080}"
    exit 0
  fi
  echo "Waiting ($i)..."
  sleep 5
done

echo "ERROR: Airflow did not become healthy in time" >&2
docker compose --project-name maritime-staging -f "${COMPOSE_FILE}" ps
exit 1
