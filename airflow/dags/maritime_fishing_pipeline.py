
from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

_AIRFLOW_ROOT = Path(__file__).resolve().parents[1]

if str(_AIRFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(_AIRFLOW_ROOT))

from include.run_script import make_script_callable

DEFAULT_ARGS = {
    "owner": "maritime_fishing",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="maritime_fishing_pipeline",
    description="Bronze -> Silver -> Gold fishing activity pipeline",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    tags=["maritime", "fishing", "etl", "medallion"],
) as dag:

    # ---------- Bronze ----------
    ingest_bronze = PythonOperator(
        task_id="ingest_bronze",
        python_callable=make_script_callable("src/silver/ingest.py"),
    )

    # ---------- Silver ----------
    build_silver = PythonOperator(
        task_id="build_silver",
        python_callable=make_script_callable("src/silver/silver.py"),
    )

    validate_silver = PythonOperator(
        task_id="validate_silver",
        python_callable=make_script_callable("src/silver/validate_silver.py"),
    )

    upload_silver = PythonOperator(
        task_id="upload_silver",
        python_callable=make_script_callable("src/silver/upload_silver.py"),
    )

    # ---------- Gold change detection ----------
    detect_gold_changes = PythonOperator(
        task_id="detect_gold_changes",
        python_callable=make_script_callable(
            "src/gold/detect_gold_changes.py"
        ),
    )

    # ---------- Gold: daily ----------
    build_daily_gold = PythonOperator(
        task_id="build_daily_gold",
        python_callable=make_script_callable(
            "src/gold/build_gold/build_daily_gold.py"
        ),
    )

    validate_daily_gold = PythonOperator(
        task_id="validate_daily_gold",
        python_callable=make_script_callable(
            "src/gold/validate/validate_daily_gold.py"
        ),
    )

    build_daily_hours_dq = PythonOperator(
        task_id="build_daily_hours_dq",
        python_callable=make_script_callable(
            "src/gold/build_gold/build_daily_vessel_hours_dq.py"
        ),
    )

    validate_daily_hours_dq = PythonOperator(
        task_id="validate_daily_hours_dq",
        python_callable=make_script_callable(
            "src/gold/validate/validate_daily_vessel_hours_dq.py"
        ),
    )

    # ---------- Gold: monthly ----------
    build_monthly_gold = PythonOperator(
        task_id="build_monthly_gold",
        python_callable=make_script_callable(
            "src/gold/build_gold/build_monthly_gold.py"
        ),
    )

    validate_monthly_gold = PythonOperator(
        task_id="validate_monthly_gold",
        python_callable=make_script_callable(
            "src/gold/validate/validate_monthly_gold.py"
        ),
    )

    # ---------- Gold: area ----------
    build_area_fishing_activity = PythonOperator(
        task_id="build_area_fishing_activity",
        python_callable=make_script_callable(
            "src/gold/build_gold/build_area_fishing_activity.py"
        ),
    )

    validate_area_fishing_activity = PythonOperator(
        task_id="validate_area_fishing_activity",
        python_callable=make_script_callable(
            "src/gold/validate/validate_area_fishing_activity.py"
        ),
    )

    upload_gold = PythonOperator(
        task_id="upload_gold",
        python_callable=make_script_callable("src/gold/upload_gold.py"),
    )

    # Clear pending only after upload succeeds, using the same run_id
    # gold_changes.json written by detect_gold_changes.
    finalize_gold_changes = PythonOperator(
        task_id="finalize_gold_changes",
        python_callable=make_script_callable(
            "src/gold/finalize_gold_changes.py"
        ),
    )

    # Pipeline graph
    ingest_bronze >> build_silver >> validate_silver

    # Upload silver after validation (does not block gold builds)
    validate_silver >> upload_silver

    # Detect gold changes after silver validation, then build gold
    validate_silver >> detect_gold_changes

    detect_gold_changes >> [
        build_daily_gold,
        build_monthly_gold,
        build_area_fishing_activity,
    ]

    (
        build_daily_gold
        >> validate_daily_gold
        >> build_daily_hours_dq
        >> validate_daily_hours_dq
    )

    build_monthly_gold >> validate_monthly_gold

    build_area_fishing_activity >> validate_area_fishing_activity

    # Upload gold only when every validate task has passed
    [
        validate_silver,
        validate_daily_gold,
        validate_daily_hours_dq,
        validate_monthly_gold,
        validate_area_fishing_activity,
    ] >> upload_gold

    # Finalize pending against detect snapshot for this DAG run
    upload_gold >> finalize_gold_changes
