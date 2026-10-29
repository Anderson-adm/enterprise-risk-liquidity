#!/usr/bin/env bash
# Local Airflow helper. Airflow lives in its own venv (airflow/.venv) and calls the pipeline
# through the project CLI (`erl step ...`), so Airflow never imports dbt or project deps.
#   scripts/airflow_local.sh setup                 # create venv + metadata DB (SQLite)
#   scripts/airflow_local.sh test 2026-10-26       # run the DAG once, in-process (airflow dags test)
#   scripts/airflow_local.sh standalone            # UI on http://localhost:8080 (Ctrl+C to stop)
set -euo pipefail
cd "$(dirname "$0")/.."
ROOT=$PWD
AIRFLOW_VERSION=3.3.2
PY=3.12
export AIRFLOW_HOME="$ROOT/.airflow"
export AIRFLOW__CORE__DAGS_FOLDER="$ROOT/airflow/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__PARALLELISM=2
export AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG=1
export AIRFLOW__CORE__EXECUTOR=LocalExecutor
export AIRFLOW__LOGGING__LOGGING_LEVEL=INFO
export ERL_BIN="${ERL_BIN:-$ROOT/.venv/bin/erl}"
export ERL_DATA_DIR="${ERL_DATA_DIR:-$ROOT/data}"
AF="$ROOT/airflow/.venv/bin/airflow"

case "${1:-}" in
  setup)
    uv venv --quiet --python "$PY" airflow/.venv
    uv pip install --quiet --python airflow/.venv/bin/python -r airflow/requirements.txt \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PY}.txt"
    "$AF" db migrate
    ;;
  test)
    "$AF" dags reserialize >/dev/null
    "$AF" dags test erl_risk_liquidity_pipeline --conf "{\"reference_date\": \"${2:?reference date}\"}"
    ;;
  standalone)
    "$AF" standalone
    ;;
  *)
    echo "usage: $0 setup|test <YYYY-MM-DD>|standalone" >&2
    exit 2
    ;;
esac
