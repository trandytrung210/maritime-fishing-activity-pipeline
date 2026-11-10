from __future__ import annotations

from datetime import UTC, datetime

from src.common.manifest import (
    PENDING_GOLD_CHANGES_PATH,
    build_changes_snapshot,
    dates_to_affected_months,
    get_run_id,
    gold_changes_path,
    load_pending_gold_changes,
    save_gold_changes,
)


def main() -> None:
    print("=" * 50)
    print("DETECT GOLD CHANGES")
    print("=" * 50)

    run_id = get_run_id()
    pending = load_pending_gold_changes()
    changes = pending.get("changes") or {}
    if not isinstance(changes, dict):
        raise TypeError(
            f"pending 'changes' must be a dict, got {type(changes).__name__}"
        )
    changed_dates = sorted(changes.keys())
    affected_months = dates_to_affected_months(changed_dates)
    changes_snapshot = build_changes_snapshot(changes)

    payload = {
        "run_id": run_id,
        "generated_at": datetime.now(UTC).isoformat(),
        "changed_dates": changed_dates,
        "affected_months": affected_months,
        "changes_snapshot": changes_snapshot,
        "has_silver_changes": bool(changed_dates),
        "pending_source": str(PENDING_GOLD_CHANGES_PATH),
    }
    out_path = save_gold_changes(payload, run_id=run_id)

    print(f"Run ID: {run_id}")
    print(f"Pending gold file: {PENDING_GOLD_CHANGES_PATH}")
    print(f"Gold changes (run): {out_path}")
    print(f"Gold changes path helper: {gold_changes_path(run_id)}")
    print(f"Changed dates: {len(changed_dates)}")
    print(f"Affected months: {len(affected_months)}")
    if changed_dates:
        preview = ", ".join(changed_dates[:10])
        more = (
            ""
            if len(changed_dates) <= 10
            else f", ... (+{len(changed_dates) - 10})"
        )
        print(f"Dates preview: {preview}{more}")
        print("has_silver_changes: true")
    else:
        print("has_silver_changes: false")
        print(
            "No silver dates pending. Gold builders may no-op, "
            "but pipeline continues (upload/validate still run)."
        )


if __name__ == "__main__":
    main()
