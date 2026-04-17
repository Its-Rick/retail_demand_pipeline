"""
forecasting.py
--------------
Streamlit sub-page: Demand Forecasting & Trend Analysis.
Uses simple statistical models (rolling average, linear trend)
to produce demand forecasts when a full ML model is not available.

Import in dashboard.py or run standalone:
    streamlit run streamlit/forecasting.py
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
import os
from datetime import datetime, timedelta


# ─── FORECAST MODELS ──────────────────────────────────────────────────────────

def moving_average_forecast(series: pd.Series, horizon: int, window: int = 7) -> pd.Series:
    """Simple moving average forecast: last window days avg projected forward."""
    last_avg = series.iloc[-window:].mean()
    return pd.Series([last_avg] * horizon)


def linear_trend_forecast(series: pd.Series, horizon: int) -> pd.Series:
    """Linear regression on recent 30 days, extrapolate forward."""
    n = min(30, len(series))
    y = series.iloc[-n:].values
    x = np.arange(n)
    slope, intercept = np.polyfit(x, y, 1)
    future_x = np.arange(n, n + horizon)
    forecast = slope * future_x + intercept
    return pd.Series(np.maximum(forecast, 0))  # clip negatives


def seasonal_naive_forecast(series: pd.Series, horizon: int,
                            period: int = 7) -> pd.Series:
    """Seasonal naive: repeat last full season (week) for horizon days."""
    last_season = series.iloc[-period:].values
    reps = (horizon // period) + 1
    forecast = np.tile(last_season, reps)[:horizon]
    return pd.Series(np.maximum(forecast, 0))


def ensemble_forecast(series: pd.Series, horizon: int) -> pd.DataFrame:
    """Combine three models into an ensemble with confidence band."""
    ma  = moving_average_forecast(series, horizon)
    lt  = linear_trend_forecast(series, horizon)
    sn  = seasonal_naive_forecast(series, horizon)
    avg = (ma + lt + sn) / 3
    std = pd.DataFrame({"ma": ma, "lt": lt, "sn": sn}).std(axis=1)
    return pd.DataFrame({
        "forecast":    avg,
        "lower_80":    (avg - 1.28 * std).clip(lower=0),
        "upper_80":    avg + 1.28 * std,
        "lower_95":    (avg - 1.96 * std).clip(lower=0),
        "upper_95":    avg + 1.96 * std,
        "ma":          ma,
        "linear":      lt,
        "seasonal":    sn,
    })


# ─── LOAD DATA ────────────────────────────────────────────────────────────────

@st.cache_data(ttl=300)
def load_daily_demand(data_lake: str) -> pd.DataFrame:
    """Load daily demand from DB or sample CSV."""
    try:
        import psycopg2
        dsn = os.getenv("DATABASE_URL", "postgresql://airflow:airflow@localhost:5432/retail_dw")
        conn = psycopg2.connect(dsn)
        df = pd.read_sql("""
            SELECT agg_date, product_id, store_id, category, region,
                   total_quantity, total_revenue
            FROM warehouse.agg_daily_demand
            ORDER BY agg_date
        """, conn)
        conn.close()
        df["agg_date"] = pd.to_datetime(df["agg_date"])
        return df
    except Exception:
        # Fallback: build from sample CSV
        pos = pd.read_csv(os.path.join(data_lake, "sample", "pos_sales.csv"),
                          parse_dates=["transaction_date"])
        return (
            pos.groupby(["transaction_date", "product_id", "store_id", "category", "region"])
            .agg(total_quantity=("quantity","sum"), total_revenue=("total_amount","sum"))
            .reset_index()
            .rename(columns={"transaction_date": "agg_date"})
        )


# ─── MAIN PAGE ────────────────────────────────────────────────────────────────

def render():
    st.markdown("## 🔮 Demand Forecasting")
    st.markdown("Statistical demand forecasts using Moving Average, Linear Trend, and Seasonal Naive models.")

    data_lake = os.getenv("DATA_LAKE_PATH", "./data")
    df = load_daily_demand(data_lake)

    # ── CONTROLS ──────────────────────────────────────────────────────────────
    col1, col2, col3 = st.columns(3)
    with col1:
        categories = sorted(df["category"].unique().tolist())
        selected_cat = st.selectbox("Category", categories)
    with col2:
        products = sorted(df[df["category"] == selected_cat]["product_id"].unique().tolist())
        selected_product = st.selectbox("Product", products[:20])
    with col3:
        horizon = st.slider("Forecast horizon (days)", min_value=7, max_value=90, value=30, step=7)

    # ── FILTER TO PRODUCT ─────────────────────────────────────────────────────
    series_df = (
        df[df["product_id"] == selected_product]
        .groupby("agg_date")["total_quantity"]
        .sum()
        .sort_index()
        .reset_index()
    )

    if len(series_df) < 14:
        st.warning("Not enough historical data for this product (need ≥ 14 days).")
        return

    series = series_df.set_index("agg_date")["total_quantity"]

    # ── FORECAST ──────────────────────────────────────────────────────────────
    fc = ensemble_forecast(series, horizon)
    last_date = series.index[-1]
    future_dates = pd.date_range(last_date + timedelta(days=1), periods=horizon)
    fc.index = future_dates

    # ── PLOT ──────────────────────────────────────────────────────────────────
    fig = go.Figure()

    # Historical
    fig.add_trace(go.Scatter(
        x=series.index, y=series.values,
        name="Historical", line=dict(color="#0077b6", width=2),
        mode="lines",
    ))

    # 95% confidence band
    fig.add_trace(go.Scatter(
        x=list(future_dates) + list(future_dates[::-1]),
        y=list(fc["upper_95"]) + list(fc["lower_95"][::-1]),
        fill="toself", fillcolor="rgba(233,69,96,0.08)",
        line=dict(color="rgba(255,255,255,0)"),
        name="95% CI", showlegend=True,
    ))

    # 80% confidence band
    fig.add_trace(go.Scatter(
        x=list(future_dates) + list(future_dates[::-1]),
        y=list(fc["upper_80"]) + list(fc["lower_80"][::-1]),
        fill="toself", fillcolor="rgba(233,69,96,0.18)",
        line=dict(color="rgba(255,255,255,0)"),
        name="80% CI", showlegend=True,
    ))

    # Ensemble forecast
    fig.add_trace(go.Scatter(
        x=future_dates, y=fc["forecast"],
        name="Ensemble Forecast",
        line=dict(color="#e94560", width=2.5, dash="dot"),
        mode="lines",
    ))

    # Individual models
    for model, color in [("ma","#f77f00"), ("linear","#0f9b58"), ("seasonal","#9b59b6")]:
        fig.add_trace(go.Scatter(
            x=future_dates, y=fc[model],
            name={"ma":"Moving Avg","linear":"Linear Trend","seasonal":"Seasonal Naive"}[model],
            line=dict(color=color, width=1, dash="dash"),
            opacity=0.6, visible="legendonly",
        ))

    # Divider line at forecast start
    fig.add_vline(x=str(last_date), line_dash="dot", line_color="gray",
                  annotation_text="Forecast start", annotation_position="top right")

    fig.update_layout(
        template="plotly_dark",
        height=420,
        plot_bgcolor="#16213e",
        paper_bgcolor="#16213e",
        title=f"Demand Forecast — {selected_product} | {horizon}-day horizon",
        xaxis_title="Date",
        yaxis_title="Units Sold",
        legend=dict(orientation="h", y=1.08),
        xaxis=dict(showgrid=False),
        yaxis=dict(showgrid=True, gridcolor="#ffffff15"),
    )
    st.plotly_chart(fig, use_container_width=True)

    # ── FORECAST TABLE ────────────────────────────────────────────────────────
    st.markdown("### 📋 Forecast Detail")
    fc_display = fc[["forecast","lower_80","upper_80"]].copy()
    fc_display.index = future_dates.strftime("%Y-%m-%d")
    fc_display.columns = ["Forecast", "Lower 80%", "Upper 80%"]
    fc_display = fc_display.round(1)

    col_tbl, col_kpi = st.columns([3, 1])
    with col_tbl:
        st.dataframe(fc_display.head(14), use_container_width=True, height=320)
    with col_kpi:
        total_fc = fc["forecast"].sum()
        avg_fc   = fc["forecast"].mean()
        hist_avg = series.iloc[-horizon:].mean() if len(series) >= horizon else series.mean()
        pct_chg  = (avg_fc - hist_avg) / max(hist_avg, 1) * 100

        st.markdown(f"""
        <div style="background:linear-gradient(135deg,#16213e,#1a1a2e);border:1px solid #e9456030;
             border-radius:12px;padding:1rem;margin:0.3rem 0">
            <div style="color:#8892b0;font-size:0.75rem;text-transform:uppercase;letter-spacing:1px">Total Forecast</div>
            <div style="color:#e94560;font-size:1.8rem;font-weight:700">{total_fc:,.0f}</div>
            <div style="color:#0f9b58;font-size:0.8rem">{horizon}-day total units</div>
        </div>
        <div style="background:linear-gradient(135deg,#16213e,#1a1a2e);border:1px solid #e9456030;
             border-radius:12px;padding:1rem;margin:0.3rem 0">
            <div style="color:#8892b0;font-size:0.75rem;text-transform:uppercase;letter-spacing:1px">vs Prior Period</div>
            <div style="color:{'#0f9b58' if pct_chg >= 0 else '#e94560'};font-size:1.8rem;font-weight:700">
                {'▲' if pct_chg >= 0 else '▼'} {abs(pct_chg):.1f}%
            </div>
            <div style="color:#8892b0;font-size:0.8rem">Avg daily: {avg_fc:.1f} units</div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("---")
    st.caption("📌 Models: Moving Average (7d) · Linear Trend (30d) · Seasonal Naive (7d period) · Ensemble = mean of three")


if __name__ == "__main__":
    st.set_page_config(page_title="Demand Forecasting", layout="wide")
    render()
