# Maritime Gold Dashboards

Dashboards over Gold Parquet from `src/gold/build_gold/`.

| View | Source | Questions answered |
|---|---|---|
| Overview | monthly + inventory | Month trends, Gold scale |
| Daily Vessel | `daily_vessel_activity` | Vessel-day effort, top vessels |
| Monthly Activity | `monthly_vessel_activity` | Top fishers / active days |
| Area Fishing | `area_fishing_activity` | Hot cells & intensity |
| Hours DQ | `reports/daily_vessel_hours_dq` | NORMAL / INFO / REVIEW |

Reads with **PyArrow** (partition-aware). No Spark required.

Year **2015** partitions are excluded from dashboard views (local sample only).

## Generate HTML dashboards (recommended)

Works with the project `.venv` (Plotly + existing pandas/pyarrow):

```bash
pip install -r dashboard/requirements.txt
python dashboard/generate.py
```

Open:

`reports/dashboards/index.html`

## Optional Streamlit UI

```bash
streamlit run dashboard/app.py
```

> Streamlit may conflict with very new pandas pins in this repo. Prefer HTML generation if install fails; Streamlit pages under `dashboard/pages/` stay available when deps resolve.
