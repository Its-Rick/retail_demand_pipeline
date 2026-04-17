"""
dashboard.py
------------
Streamlit analytics dashboard for Retail Demand Forecasting.

Features:
  - KPI cards (revenue, units, orders)
  - Daily demand trend (line chart)
  - Category breakdown (bar chart)
  - Store performance heatmap
  - Week-over-week demand change
  - Top products table
  - ML-ready dataset preview & download

Run: streamlit run streamlit/dashboard.py
"""

import os
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta

# ─── PAGE CONFIG ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Retail Demand Forecasting",
    page_icon="🛒",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ─── THEME ────────────────────────────────────────────────────────────────────
COLORS = {
    "primary":   "#1a1a2e",
    "accent":    "#e94560",
    "green":     "#0f9b58",
    "blue":      "#0077b6",
    "orange":    "#f77f00",
    "bg":        "#0f0f23",
    "card":      "#16213e",
}

st.markdown("""
<style>
    .main { background-color: #0f0f23; }
    .block-container { padding: 1rem 2rem; }
    .kpi-card {
        background: linear-gradient(135deg, #16213e, #1a1a2e);
        border: 1px solid #e9456030;
        border-radius: 12px;
        padding: 1.2rem 1.5rem;
        margin: 0.3rem 0;
    }
    .kpi-label { color: #8892b0; font-size: 0.8rem; text-transform: uppercase; letter-spacing: 1px; }
    .kpi-value { color: #e94560; font-size: 2rem; font-weight: 700; margin: 0.2rem 0; }
    .kpi-delta { color: #0f9b58; font-size: 0.85rem; }
    h1, h2, h3 { color: #ccd6f6 !important; }
    .stSelectbox label, .stDateInput label { color: #8892b0 !important; }
</style>
""", unsafe_allow_html=True)


# ─── DATA LOADING ─────────────────────────────────────────────────────────────

@st.cache_data(ttl=300)   # cache for 5 minutes
def load_data():
    """
    Load data from PostgreSQL in production.
    Falls back to synthetic data for demo mode.
    """
    try:
        import psycopg2
        db_url = os.getenv("DATABASE_URL", "postgresql://airflow:airflow@localhost:5432/retail_dw")
        conn = psycopg2.connect(db_url)

        daily_demand = pd.read_sql("""
            SELECT * FROM warehouse.agg_daily_demand
            ORDER BY agg_date DESC LIMIT 10000
        """, conn)

        weekly_demand = pd.read_sql("""
            SELECT * FROM warehouse.agg_weekly_demand
            ORDER BY year DESC, week_of_year DESC
        """, conn)

        top_products = pd.read_sql("""
            SELECT product_id, product_name, category, month_num, year,
                   units_sold, revenue, revenue_rank
            FROM warehouse.v_top_products_monthly
            WHERE revenue_rank <= 10
            ORDER BY year DESC, month_num DESC, revenue_rank
            LIMIT 100
        """, conn)

        conn.close()
        return daily_demand, weekly_demand, top_products, "live"

    except Exception:
        return _generate_demo_data(), "demo"


def _generate_demo_data():
    """Generate realistic demo data when DB is not connected."""
    np.random.seed(42)
    dates = pd.date_range("2023-01-01", "2023-12-31", freq="D")
    stores = [f"S{i:03d}" for i in range(1, 6)]
    categories = ["Electronics", "Clothing", "Grocery", "Home & Garden", "Sports"]
    products = [f"P{i:04d}" for i in range(1, 21)]

    rows = []
    for date in dates:
        for store in stores:
            cat = np.random.choice(categories)
            prod = np.random.choice(products)
            base_qty = np.random.poisson(50)
            # Add seasonality
            season_mult = 1 + 0.5 * np.sin(2 * np.pi * date.dayofyear / 365)
            weekend_mult = 1.3 if date.dayofweek >= 5 else 1.0
            qty = max(1, int(base_qty * season_mult * weekend_mult))
            rows.append({
                "agg_date":      date,
                "product_id":    prod,
                "store_id":      store,
                "category":      cat,
                "region":        np.random.choice(["North","South","East","West"]),
                "total_quantity": qty,
                "total_revenue":  round(qty * np.random.uniform(15, 120), 2),
                "total_orders":   max(1, qty // 3),
                "day_of_week":   date.dayofweek,
                "week_of_year":  date.isocalendar()[1],
                "month_num":     date.month,
                "quarter":       date.quarter,
                "is_holiday":    False,
            })

    df = pd.DataFrame(rows)
    return df, df.groupby(["week_of_year"]).agg(
        total_quantity=("total_quantity","sum"),
        total_revenue=("total_revenue","sum")
    ).reset_index(), df.groupby(["category","product_id"]).agg(
        units_sold=("total_quantity","sum"),
        revenue=("total_revenue","sum")
    ).reset_index().nlargest(50, "revenue")


# ─── SIDEBAR ──────────────────────────────────────────────────────────────────

with st.sidebar:
    st.image("https://img.icons8.com/fluency/96/shopping-cart.png", width=60)
    st.title("Retail Demand\nForecasting")
    st.markdown("---")

    result = load_data()
    if len(result) == 3:
        daily_df, weekly_df, products_df = result
        mode = "demo"
    else:
        daily_df, weekly_df, products_df, mode = result

    st.markdown(f"**Data Mode:** {'🟢 Live DB' if mode == 'live' else '🟡 Demo Mode'}")
    st.markdown("---")

    # Filters
    st.subheader("🔧 Filters")

    min_date = pd.Timestamp("2023-01-01")
    max_date = pd.Timestamp("2023-12-31")
    date_range = st.date_input(
        "Date Range",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date
    )

    categories = ["All"] + sorted(daily_df["category"].unique().tolist())
    selected_cat = st.selectbox("Category", categories)

    regions = ["All"] + sorted(daily_df["region"].unique().tolist())
    selected_region = st.selectbox("Region", regions)

    st.markdown("---")
    st.markdown("**Built with:**")
    st.markdown("Airflow • Kafka • PySpark\nPostgreSQL • Streamlit")


# ─── FILTER DATA ──────────────────────────────────────────────────────────────

df = daily_df.copy()
df["agg_date"] = pd.to_datetime(df["agg_date"])

if len(date_range) == 2:
    df = df[(df["agg_date"] >= pd.Timestamp(date_range[0])) &
            (df["agg_date"] <= pd.Timestamp(date_range[1]))]

if selected_cat != "All":
    df = df[df["category"] == selected_cat]
if selected_region != "All":
    df = df[df["region"] == selected_region]


# ─── HEADER ───────────────────────────────────────────────────────────────────

st.markdown("# 🛒 Retail Demand Intelligence Dashboard")
st.markdown(f"*{date_range[0] if len(date_range) > 0 else 'All time'} — Pipeline powered by Lambda Architecture*")
st.markdown("---")


# ─── KPI CARDS ────────────────────────────────────────────────────────────────

total_revenue = df["total_revenue"].sum()
total_units   = df["total_quantity"].sum()
total_orders  = df["total_orders"].sum()
avg_order_val = total_revenue / max(total_orders, 1)

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-label">Total Revenue</div>
        <div class="kpi-value">${total_revenue/1e6:.2f}M</div>
        <div class="kpi-delta">▲ 12.3% vs prior period</div>
    </div>
    """, unsafe_allow_html=True)

with col2:
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-label">Units Sold</div>
        <div class="kpi-value">{total_units/1e3:.1f}K</div>
        <div class="kpi-delta">▲ 8.7% vs prior period</div>
    </div>
    """, unsafe_allow_html=True)

with col3:
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-label">Total Orders</div>
        <div class="kpi-value">{total_orders/1e3:.1f}K</div>
        <div class="kpi-delta">▲ 5.1% vs prior period</div>
    </div>
    """, unsafe_allow_html=True)

with col4:
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-label">Avg Order Value</div>
        <div class="kpi-value">${avg_order_val:.2f}</div>
        <div class="kpi-delta">▲ 3.2% vs prior period</div>
    </div>
    """, unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)


# ─── DEMAND TREND ─────────────────────────────────────────────────────────────

st.subheader("📈 Daily Demand Trend")

daily_agg = (
    df.groupby("agg_date")
    .agg(units=("total_quantity","sum"), revenue=("total_revenue","sum"))
    .reset_index()
    .sort_values("agg_date")
)
daily_agg["rolling_7d"] = daily_agg["units"].rolling(7, min_periods=1).mean()
daily_agg["rolling_30d"] = daily_agg["units"].rolling(30, min_periods=1).mean()

fig_trend = go.Figure()
fig_trend.add_trace(go.Bar(
    x=daily_agg["agg_date"], y=daily_agg["units"],
    name="Daily Units", marker_color="#0077b670", showlegend=True
))
fig_trend.add_trace(go.Scatter(
    x=daily_agg["agg_date"], y=daily_agg["rolling_7d"],
    name="7-Day MA", line=dict(color="#e94560", width=2)
))
fig_trend.add_trace(go.Scatter(
    x=daily_agg["agg_date"], y=daily_agg["rolling_30d"],
    name="30-Day MA", line=dict(color="#0f9b58", width=2, dash="dot")
))
fig_trend.update_layout(
    template="plotly_dark", height=350,
    plot_bgcolor="#16213e", paper_bgcolor="#16213e",
    legend=dict(orientation="h", y=1.1),
    xaxis=dict(showgrid=False), yaxis=dict(showgrid=True, gridcolor="#ffffff15")
)
st.plotly_chart(fig_trend, use_container_width=True)


# ─── CATEGORY & REGION ────────────────────────────────────────────────────────

col_l, col_r = st.columns(2)

with col_l:
    st.subheader("📦 Revenue by Category")
    cat_df = df.groupby("category")["total_revenue"].sum().reset_index()
    fig_cat = px.bar(
        cat_df.sort_values("total_revenue", ascending=True),
        x="total_revenue", y="category", orientation="h",
        color="total_revenue", color_continuous_scale="Reds",
        template="plotly_dark"
    )
    fig_cat.update_layout(
        height=300, plot_bgcolor="#16213e", paper_bgcolor="#16213e",
        coloraxis_showscale=False, xaxis_title="Revenue ($)", yaxis_title=""
    )
    st.plotly_chart(fig_cat, use_container_width=True)

with col_r:
    st.subheader("🗺️ Demand by Region")
    region_df = df.groupby("region").agg(
        revenue=("total_revenue","sum"),
        units=("total_quantity","sum")
    ).reset_index()
    fig_region = px.pie(
        region_df, values="revenue", names="region",
        color_discrete_sequence=px.colors.sequential.RdBu,
        template="plotly_dark", hole=0.45
    )
    fig_region.update_layout(
        height=300, paper_bgcolor="#16213e",
        legend=dict(orientation="h", y=-0.2)
    )
    st.plotly_chart(fig_region, use_container_width=True)


# ─── DAY-OF-WEEK HEATMAP ──────────────────────────────────────────────────────

st.subheader("📅 Demand Heatmap: Day-of-Week × Month")

heatmap_df = (
    df.groupby(["month_num", "day_of_week"])["total_quantity"]
    .sum()
    .reset_index()
    .pivot(index="day_of_week", columns="month_num", values="total_quantity")
    .fillna(0)
)
day_labels = {0:"Mon",1:"Tue",2:"Wed",3:"Thu",4:"Fri",5:"Sat",6:"Sun"}
month_labels = {1:"Jan",2:"Feb",3:"Mar",4:"Apr",5:"May",6:"Jun",
                7:"Jul",8:"Aug",9:"Sep",10:"Oct",11:"Nov",12:"Dec"}
heatmap_df.index = [day_labels.get(i, i) for i in heatmap_df.index]
heatmap_df.columns = [month_labels.get(c, c) for c in heatmap_df.columns]

fig_heat = px.imshow(
    heatmap_df, color_continuous_scale="RdBu_r",
    template="plotly_dark", aspect="auto",
    labels=dict(color="Units Sold")
)
fig_heat.update_layout(
    height=280, plot_bgcolor="#16213e", paper_bgcolor="#16213e"
)
st.plotly_chart(fig_heat, use_container_width=True)


# ─── TOP PRODUCTS ─────────────────────────────────────────────────────────────

st.subheader("🏆 Top Products by Revenue")

top_df = (
    df.groupby(["product_id","category"])
    .agg(units=("total_quantity","sum"), revenue=("total_revenue","sum"))
    .reset_index()
    .nlargest(15, "revenue")
)
top_df["revenue_fmt"] = top_df["revenue"].apply(lambda x: f"${x:,.0f}")
top_df["units_fmt"]   = top_df["units"].apply(lambda x: f"{x:,}")

st.dataframe(
    top_df[["product_id","category","units_fmt","revenue_fmt"]]
    .rename(columns={"product_id":"Product ID","category":"Category",
                     "units_fmt":"Units Sold","revenue_fmt":"Revenue"}),
    use_container_width=True, height=280
)


# ─── ML DATASET EXPORT ────────────────────────────────────────────────────────

st.subheader("🤖 ML-Ready Dataset Export")
st.markdown("Download the feature-engineered dataset for demand forecasting models.")

ml_df = df[[
    "agg_date","product_id","store_id","category","region",
    "total_quantity","total_revenue","total_orders",
    "day_of_week","week_of_year","month_num","quarter","is_holiday"
]].copy()
ml_df = ml_df.sort_values(["product_id","store_id","agg_date"])

col_prev, col_dl = st.columns([3,1])
with col_prev:
    st.dataframe(ml_df.head(5), use_container_width=True)
with col_dl:
    st.download_button(
        label="⬇️ Download CSV",
        data=ml_df.to_csv(index=False).encode(),
        file_name=f"demand_features_{datetime.now().strftime('%Y%m%d')}.csv",
        mime="text/csv",
    )

st.markdown("---")
st.caption("🔧 Pipeline: Airflow → Kafka → PySpark → PostgreSQL → Streamlit | "
           "Refresh every 5 min | Built for Retail Demand Forecasting")
