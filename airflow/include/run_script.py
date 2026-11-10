from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


def get_project_root() -> Path:
    configured = os.getenv("MARITIME_PROJECT_ROOT")
    if configured:
        return Path(configured).resolve()

    # airflow/include/run_script.py -> parents[2] = repo root
    return Path(__file__).resolve().parents[2]


def get_python_executable(project_root: Path) -> str:
    configured = os.getenv("MARITIME_PYTHON")
    if configured:
        return configured

    if os.name == "nt":
        python_venv = project_root / ".venv" / "Scripts" / "python.exe"
    else:
        python_venv = project_root / ".venv" / "bin" / "python"

    if python_venv.exists():
        return str(python_venv)

    return sys.executable


def _context_env(context: dict | None) -> dict[str, str]:
    if not context:
        return {}
    extra: dict[str, str] = {}
    dag = context.get("dag")
    dag_run = context.get("dag_run")
    ti = context.get("ti") or context.get("task_instance")
    if dag is not None and getattr(dag, "dag_id", None):
        extra["AIRFLOW_CTX_DAG_ID"] = str(dag.dag_id)
    if dag_run is not None and getattr(dag_run, "run_id", None):
        extra["AIRFLOW_CTX_DAG_RUN_ID"] = str(dag_run.run_id)
    if ti is not None and getattr(ti, "task_id", None):
        extra["AIRFLOW_CTX_TASK_ID"] = str(ti.task_id)
    return extra


def run_project_script(
    relative_script_path: str,
    extra_env: dict[str, str] | None = None,
) -> None:
    project_root = get_project_root()
    script_path = (project_root / relative_script_path).resolve()

    try:
        script_path.relative_to(project_root)
    except ValueError as exc:
        raise ValueError(
            f"Script path escapes project root: {script_path}"
        ) from exc

    if not script_path.is_file():
        raise FileNotFoundError(f"Script not found: {script_path}")

    python_exe = get_python_executable(project_root)

    env = os.environ.copy()
    env["MARITIME_PROJECT_ROOT"] = str(project_root)
    if extra_env:
        env.update(extra_env)

    existing_pythonpath = env.get("PYTHONPATH")
    env["PYTHONPATH"] = (
        str(project_root)
        if not existing_pythonpath
        else f"{project_root}{os.pathsep}{existing_pythonpath}"
    )

    print(f"Project root: {project_root}")
    print(f"Python: {python_exe}")
    print(f"Running: {script_path}")
    if env.get("AIRFLOW_CTX_DAG_RUN_ID"):
        print(f"Airflow run id: {env['AIRFLOW_CTX_DAG_RUN_ID']}")

    try:
        subprocess.run(
            [python_exe, "-u", str(script_path)],
            cwd=str(project_root),
            env=env,
            check=True,
        )
    except subprocess.CalledProcessError as exc:
        raise RuntimeError(
            f"Script failed (exit {exc.returncode}): {relative_script_path}\n"
            f"python={python_exe}\n"
            f"cwd={project_root}"
        ) from exc

    print(f"Script finished OK: {relative_script_path}")


def make_script_callable(relative_script_path: str):
    def _callable(**context):
        run_project_script(
            relative_script_path,
            extra_env=_context_env(context),
        )

    normalized_path = relative_script_path.replace("\\", "/")
    safe_name = "".join(
        char if char.isalnum() or char == "_" else "_"
        for char in normalized_path
    )
    _callable.__name__ = f"run_{safe_name}"

    return _callable
