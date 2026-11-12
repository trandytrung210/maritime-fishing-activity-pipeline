from __future__ import annotations

import streamlit as st

THEME_CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Instrument+Sans:wght@400;500;600;700&family=Sora:wght@500;600;700&display=swap');

:root {
  --sea-ink: #07141c;
  --sea-deep: #0c2430;
  --sea-panel: rgba(14, 48, 62, 0.72);
  --sea-line: rgba(146, 201, 214, 0.18);
  --foam: #dff5f2;
  --teal: #2ec4b6;
  --amber: #e9b44c;
  --coral: #ef6f6c;
}

html, body, [class*="css"] {
  font-family: "Instrument Sans", sans-serif;
}

.stApp {
  background:
    radial-gradient(1200px 600px at 12% -10%, rgba(46, 196, 182, 0.18), transparent 55%),
    radial-gradient(900px 500px at 90% 0%, rgba(233, 180, 76, 0.12), transparent 50%),
    linear-gradient(180deg, #07141c 0%, #0b1f2a 42%, #102a36 100%);
  color: var(--foam);
}

[data-testid="stSidebar"] {
  background: linear-gradient(180deg, #061119 0%, #0c2230 100%);
  border-right: 1px solid var(--sea-line);
}

h1, h2, h3, .brand-title {
  font-family: "Sora", sans-serif !important;
  letter-spacing: -0.02em;
}

.brand-shell {
  padding: 1.1rem 1.25rem 1.35rem;
  border: 1px solid var(--sea-line);
  border-radius: 18px;
  background:
    linear-gradient(135deg, rgba(46, 196, 182, 0.14), rgba(14, 48, 62, 0.35)),
    var(--sea-panel);
  margin-bottom: 1.25rem;
}

.brand-kicker {
  color: var(--teal);
  font-size: 0.78rem;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 0.14em;
  margin-bottom: 0.35rem;
}

.brand-title {
  font-size: 1.85rem;
  font-weight: 700;
  margin: 0;
  color: var(--foam);
}

.brand-copy {
  margin: 0.45rem 0 0;
  color: rgba(223, 245, 242, 0.78);
  max-width: 52rem;
  line-height: 1.45;
}

.metric-strip {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 0.85rem;
  margin: 0.75rem 0 1.4rem;
}

.metric-card {
  border: 1px solid var(--sea-line);
  border-radius: 14px;
  padding: 0.9rem 1rem;
  background: rgba(8, 28, 38, 0.7);
}

.metric-label {
  font-size: 0.75rem;
  color: rgba(223, 245, 242, 0.62);
  text-transform: uppercase;
  letter-spacing: 0.08em;
}

.metric-value {
  font-family: "Sora", sans-serif;
  font-size: 1.45rem;
  font-weight: 650;
  margin-top: 0.25rem;
  color: var(--foam);
}

.metric-value.accent { color: var(--teal); }
.metric-value.warn { color: var(--amber); }
.metric-value.alert { color: var(--coral); }

.section-note {
  color: rgba(223, 245, 242, 0.68);
  margin-top: -0.35rem;
  margin-bottom: 0.9rem;
}

div[data-testid="stMetric"] {
  background: rgba(8, 28, 38, 0.7);
  border: 1px solid var(--sea-line);
  border-radius: 14px;
  padding: 0.75rem 0.9rem;
}

@media (max-width: 900px) {
  .metric-strip { grid-template-columns: repeat(2, minmax(0, 1fr)); }
}
</style>
"""


def inject_theme() -> None:
    st.markdown(THEME_CSS, unsafe_allow_html=True)


def brand_header(title: str, copy: str, kicker: str = "Maritime Gold Analytics") -> None:
    st.markdown(
        f"""
        <div class="brand-shell">
          <div class="brand-kicker">{kicker}</div>
          <h1 class="brand-title">{title}</h1>
          <p class="brand-copy">{copy}</p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def metric_strip(items: list[tuple[str, str, str | None]] ) -> None:
    cards = []
    for label, value, tone in items:
        cls = f"metric-value {tone}" if tone else "metric-value"
        cards.append(
            f'<div class="metric-card"><div class="metric-label">{label}</div>'
            f'<div class="{cls}">{value}</div></div>'
        )
    st.markdown(
        f'<div class="metric-strip">{"".join(cards)}</div>',
        unsafe_allow_html=True,
    )
