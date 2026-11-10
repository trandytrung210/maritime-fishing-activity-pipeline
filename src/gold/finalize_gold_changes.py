from __future__ import annotations

from src.common.manifest import (
    finalize_gold_changes,
    get_run_id,
    gold_changes_path,
    load_gold_changes,
)


def main() -> None:
    print("=" * 50)
    print("FINALIZE GOLD CHANGES")
    print("=" * 50)

    run_id = get_run_id()
    gold = load_gold_changes(run_id)
    path = gold_changes_path(run_id)

    print(f"Run ID: {run_id}")
    print(f"Gold changes file: {path}")
    print(f"Changed dates: {len(gold.get('changed_dates') or [])}")
    print(f"has_silver_changes: {gold.get('has_silver_changes')}")

    removed = finalize_gold_changes(run_id=run_id)
    print(f"Pending gold entries cleared: {removed}")
    print("Finalize complete.")


if __name__ == "__main__":
    main()
