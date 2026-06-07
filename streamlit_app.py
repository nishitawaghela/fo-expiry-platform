"""
streamlit_app.py
F&O Expiry Analytics Dashboard
Reads from Supabase PostgreSQL dbt views.
Deploy on Streamlit Cloud — free, no credit card.
"""

import os
import psycopg2
import psycopg2.extras
import pandas as pd
import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="F&O Expiry Analytics",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── Custom CSS ────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:wght@300;400;600&display=swap');

html, body, [class*="css"] {
    font-family: 'IBM Plex Sans', sans-serif;
    background-color: #0D1117;
    color: #E6EDF3;
}

.main { background-color: #0D1117; }
.block-container { padding: 2rem 2rem 2rem 2rem; }

.metric-card {
    background: #161B22;
    border: 1px solid #21262D;
    border-radius: 8px;
    padding: 1.2rem 1.5rem;
    margin-bottom: 0.5rem;
}

.metric-label {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 0.7rem;
    color: #8B949E;
    text-transform: uppercase;
    letter-spacing: 0.1em;
    margin-bottom: 0.3rem;
}

.metric-value {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 1.6rem;
    font-weight: 600;
    color: #E6EDF3;
}

.metric-value.gold { color: #D4A017; }
.metric-value.green { color: #3FB950; }
.metric-value.red { color: #F85149; }
.metric-value.amber { color: #D29922; }

.section-title {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 0.75rem;
    color: #8B949E;
    text-transform: uppercase;
    letter-spacing: 0.15em;
    border-bottom: 1px solid #21262D;
    padding-bottom: 0.5rem;
    margin-bottom: 1.2rem;
    margin-top: 2rem;
}

.header-title {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 1.4rem;
    font-weight: 600;
    color: #E6EDF3;
    letter-spacing: -0.02em;
}

.header-sub {
    font-family: 'IBM Plex Sans', sans-serif;
    font-size: 0.85rem;
    color: #8B949E;
    margin-top: 0.2rem;
}

.anomaly-row {
    background: #1C2128;
    border-left: 3px solid #F85149;
    border-radius: 4px;
    padding: 0.6rem 1rem;
    margin-bottom: 0.4rem;
    font-family: 'IBM Plex Mono', monospace;
    font-size: 0.8rem;
}

.sentiment-pill {
    display: inline-block;
    padding: 0.2rem 0.8rem;
    border-radius: 20px;
    font-family: 'IBM Plex Mono', monospace;
    font-size: 0.75rem;
    font-weight: 600;
    letter-spacing: 0.05em;
}

.pill-bullish { background: #1A3D2B; color: #3FB950; border: 1px solid #3FB950; }
.pill-bearish { background: #3D1A1A; color: #F85149; border: 1px solid #F85149; }
.pill-neutral { background: #2D2A1A; color: #D29922; border: 1px solid #D29922; }

.stButton button {
    background: #21262D;
    color: #E6EDF3;
    border: 1px solid #30363D;
    border-radius: 6px;
    font-family: 'IBM Plex Mono', monospace;
    font-size: 0.8rem;
}

.stButton button:hover {
    background: #30363D;
    border-color: #D4A017;
    color: #D4A017;
}

div[data-testid="stMetric"] { display: none; }
</style>
""", unsafe_allow_html=True)


# ── DB connection ─────────────────────────────────────────────────────────────

@st.cache_resource
def get_connection():
    db_url = st.secrets["DATABASE_URL"]
    return psycopg2.connect(db_url, sslmode="require")


def fetch(query: str) -> list:
    try:
        conn = get_connection()
        cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(query)
        rows = cur.fetchall()
        cur.close()
        return [dict(r) for r in rows]
    except Exception as e:
        st.error(f"Database error: {e}")
        return []


# ── Data fetchers ─────────────────────────────────────────────────────────────
@st.cache_data(ttl=60)
def get_summary():
    rows = fetch("SELECT * FROM mart_expiry_summary LIMIT 1")
    return rows[0] if rows else {}


@st.cache_data(ttl=60)
def get_oi_by_strike():
    return fetch("SELECT * FROM mart_max_pain ORDER BY strike")


@st.cache_data(ttl=60)
def get_pcr_trend():
    return fetch("""
        SELECT batch_id, pcr, spot_price, market_sentiment, computed_at
        FROM int_pcr_trend
        ORDER BY computed_at ASC
        LIMIT 30
    """)


@st.cache_data(ttl=60)
def get_anomalies():
    return fetch("""
        SELECT strike, option_type, open_interest, oi_shift, implied_volatility
        FROM stg_oi_snapshots
        WHERE oi_shift != 0
        ORDER BY ABS(oi_shift) DESC
        LIMIT 15
    """)


# ── Plotly theme ──────────────────────────────────────────────────────────────
PLOT_BG    = "#0D1117"
PAPER_BG   = "#0D1117"
GRID_COLOR = "#21262D"
TEXT_COLOR = "#8B949E"
FONT_FAMILY = "IBM Plex Mono"


def base_layout(title=""):
    return dict(
        title=dict(text=title, font=dict(family=FONT_FAMILY, size=13, color="#E6EDF3")),
        plot_bgcolor=PLOT_BG,
        paper_bgcolor=PAPER_BG,
        font=dict(family=FONT_FAMILY, color=TEXT_COLOR, size=11),
        xaxis=dict(gridcolor=GRID_COLOR, linecolor=GRID_COLOR, tickfont=dict(size=9)),
        yaxis=dict(gridcolor=GRID_COLOR, linecolor=GRID_COLOR, tickfont=dict(size=9)),
        margin=dict(l=40, r=20, t=40, b=40),
        showlegend=True,
        legend=dict(
            font=dict(family=FONT_FAMILY, size=10, color=TEXT_COLOR),
            bgcolor="rgba(0,0,0,0)",
        ),
    )


# ── Main app ──────────────────────────────────────────────────────────────────
def main():

    # Header
    col_title, col_refresh = st.columns([5, 1])
    with col_title:
        st.markdown('<div class="header-title">⚡ F&O Expiry Analytics</div>', unsafe_allow_html=True)
        st.markdown('<div class="header-sub">NSE Nifty Weekly Options — Live Expiry Intelligence</div>', unsafe_allow_html=True)
    with col_refresh:
        st.markdown("<br>", unsafe_allow_html=True)
        if st.button("↻ Refresh"):
            st.cache_data.clear()
            st.rerun()

    st.markdown("<br>", unsafe_allow_html=True)

    # Fetch data
    summary   = get_summary()
    oi_data   = get_oi_by_strike()
    pcr_trend = get_pcr_trend()
    anomalies = get_anomalies()

    if not summary:
        st.warning("No data available. Run the pipeline first.")
        return

    sentiment = summary.get("market_sentiment", "neutral").lower()
    pcr_val   = summary.get("pcr", 0)
    sentiment_color = {"bullish": "green", "bearish": "red"}.get(sentiment, "amber")
    pill_class = f"pill-{sentiment}"

    # ── Section 1: Summary metrics ─────────────────────────────────────────
    st.markdown('<div class="section-title">// Market Summary</div>', unsafe_allow_html=True)

    c1, c2, c3, c4, c5 = st.columns(5)

    with c1:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">Symbol</div>
            <div class="metric-value gold">{summary.get('symbol', 'NIFTY')}</div>
        </div>""", unsafe_allow_html=True)

    with c2:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">Expiry</div>
            <div class="metric-value">{summary.get('expiry', '—')}</div>
        </div>""", unsafe_allow_html=True)

    with c3:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">Spot Price</div>
            <div class="metric-value">₹{summary.get('spot_price', 0):,.2f}</div>
        </div>""", unsafe_allow_html=True)

    with c4:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">PCR</div>
            <div class="metric-value {sentiment_color}">{pcr_val:.4f}</div>
        </div>""", unsafe_allow_html=True)

    with c5:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">Max Pain Strike</div>
            <div class="metric-value gold">₹{summary.get('max_pain', 0):,.0f}</div>
        </div>""", unsafe_allow_html=True)

    # Second row
    c6, c7, c8, _ = st.columns([1, 1, 1, 2])

    with c6:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">Sentiment</div>
            <div style="margin-top:0.4rem">
                <span class="sentiment-pill {pill_class}">{sentiment.upper()}</span>
            </div>
        </div>""", unsafe_allow_html=True)

    with c7:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">Total CE OI</div>
            <div class="metric-value" style="font-size:1.2rem">{summary.get('total_ce_oi', 0):,}</div>
        </div>""", unsafe_allow_html=True)

    with c8:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">Total PE OI</div>
            <div class="metric-value" style="font-size:1.2rem">{summary.get('total_pe_oi', 0):,}</div>
        </div>""", unsafe_allow_html=True)

    # ── Section 2: OI by Strike chart ──────────────────────────────────────
    st.markdown('<div class="section-title">// Open Interest by Strike</div>', unsafe_allow_html=True)

    if oi_data:
        df_oi = pd.DataFrame(oi_data)
        max_pain_strike = summary.get("max_pain", 0)

        fig_oi = go.Figure()

        fig_oi.add_trace(go.Bar(
            x=df_oi["strike"], y=df_oi["ce_oi"],
            name="CE OI", marker_color="#4472C4",
            opacity=0.85,
        ))

        fig_oi.add_trace(go.Bar(
            x=df_oi["strike"], y=df_oi["pe_oi"],
            name="PE OI", marker_color="#ED7D31",
            opacity=0.85,
        ))

        # Max pain vertical line
        fig_oi.add_vline(
            x=max_pain_strike,
            line_dash="dash",
            line_color="#D4A017",
            line_width=2,
            annotation_text=f"Max Pain ₹{max_pain_strike:,.0f}",
            annotation_font=dict(color="#D4A017", family=FONT_FAMILY, size=11),
            annotation_position="top right",
        )

        layout = base_layout()
        layout["barmode"] = "group"
        layout["xaxis"]["title"] = "Strike Price"
        layout["yaxis"]["title"] = "Open Interest"
        fig_oi.update_layout(**layout)

        st.plotly_chart(fig_oi, use_container_width=True)

    # ── Section 3: PCR Trend ───────────────────────────────────────────────
    st.markdown('<div class="section-title">// PCR Trend</div>', unsafe_allow_html=True)

    if pcr_trend:
        df_pcr = pd.DataFrame(pcr_trend)
        df_pcr["computed_at"] = pd.to_datetime(df_pcr["computed_at"])

        fig_pcr = go.Figure()

        fig_pcr.add_trace(go.Scatter(
            x=df_pcr["computed_at"], y=df_pcr["pcr"],
            mode="lines+markers",
            name="PCR",
            line=dict(color="#D4A017", width=2),
            marker=dict(size=5, color="#D4A017"),
        ))

        # Threshold lines
        fig_pcr.add_hline(y=1.2, line_dash="dot", line_color="#F85149",
                          annotation_text="Bearish (1.2)",
                          annotation_font=dict(color="#F85149", size=10, family=FONT_FAMILY))
        fig_pcr.add_hline(y=0.7, line_dash="dot", line_color="#3FB950",
                          annotation_text="Bullish (0.7)",
                          annotation_font=dict(color="#3FB950", size=10, family=FONT_FAMILY))

        layout = base_layout()
        layout["xaxis"]["title"] = "Time"
        layout["yaxis"]["title"] = "Put-Call Ratio"
        fig_pcr.update_layout(**layout)

        st.plotly_chart(fig_pcr, use_container_width=True)

    # ── Section 4: Anomalies ───────────────────────────────────────────────
    st.markdown('<div class="section-title">// Unusual OI Activity — Potential Institutional Positioning</div>', unsafe_allow_html=True)

    if anomalies:
        for a in anomalies:
            shift = a.get("oi_shift", 0)
            shift_str = f"+{shift:,}" if shift > 0 else f"{shift:,}"
            st.markdown(f"""
            <div class="anomaly-row">
                Strike <b style="color:#E6EDF3">₹{a['strike']:,.0f}</b>
                &nbsp;·&nbsp; {a['option_type']}
                &nbsp;·&nbsp; OI <b style="color:#E6EDF3">{a['open_interest']:,}</b>
                &nbsp;·&nbsp; Shift <b style="color:#F85149">{shift_str}</b>
                &nbsp;·&nbsp; IV <b style="color:#D4A017">{a['implied_volatility']:.1f}%</b>
            </div>
            """, unsafe_allow_html=True)
    else:
        st.markdown("""
        <div class="anomaly-row" style="border-left-color: #3FB950; color: #8B949E">
            No unusual OI activity detected in current batch.
            Anomalies appear when the pipeline runs continuously across multiple cycles.
        </div>
        """, unsafe_allow_html=True)

    # Footer
    last_updated = summary.get("computed_at", "")
    st.markdown(f"""
    <div style="margin-top:3rem; padding-top:1rem; border-top:1px solid #21262D;
                font-family:'IBM Plex Mono',monospace; font-size:0.7rem; color:#484F58;">
        Last updated: {str(last_updated)[:19]} UTC &nbsp;·&nbsp;
        Data source: NSE India &nbsp;·&nbsp;
        F&O Expiry Analytics Platform
    </div>
    """, unsafe_allow_html=True)


if __name__ == "__main__":
    main()