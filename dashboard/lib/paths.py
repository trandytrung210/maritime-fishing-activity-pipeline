from __future__ import annotations

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
GOLD_ROOT = PROJECT_ROOT / "data" / "processed" / "gold"
DAILY_GOLD = GOLD_ROOT / "daily_vessel_activity"
MONTHLY_GOLD = GOLD_ROOT / "monthly_vessel_activity"
AREA_GOLD = GOLD_ROOT / "area_fishing_activity"
DQ_SUMMARY = PROJECT_ROOT / "reports" / "daily_vessel_hours_dq" / "summary.json"
DQ_REVIEW = PROJECT_ROOT / "reports" / "daily_vessel_hours_dq" / "review_records"
CACHE_ROOT = PROJECT_ROOT / "reports" / "dashboard_cache"
