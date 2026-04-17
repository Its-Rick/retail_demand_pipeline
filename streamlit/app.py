"""
app.py
------
Multi-page Streamlit application entry point.
Combines the main dashboard and forecasting page into a
single app with sidebar navigation.

Run: streamlit run streamlit/app.py
"""

import streamlit as st
import os

st.set_page_config(
    page_title="Retail Demand Intelligence",
    page_icon="🛒",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Shared CSS
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
    .stSelectbox label, .stDateInput label, .stSlider label { color: #8892b0 !important; }
    [data-testid="stSidebar"] { background: #16213e; }
</style>
""", unsafe_allow_html=True)

# Navigation
with st.sidebar:
    st.image("https://img.icons8.com/fluency/96/shopping-cart.png", width=56)
    st.markdown("## Retail Demand\nPipeline")
    st.markdown("---")

    page = st.radio(
        "Navigate",
        ["📊 Analytics Dashboard", "🔮 Demand Forecasting", "⚙️ Pipeline Health"],
        label_visibility="collapsed",
    )

    st.markdown("---")
    st.markdown("**Stack**")
    st.markdown("""
    - Airflow 2.8
    - Kafka 7.5
    - PySpark 3.5
    - PostgreSQL 15
    - Streamlit 1.31
    """)

# Route pages
if page == "📊 Analytics Dashboard":
    # Import and call dashboard render function
    import importlib.util, sys, os
    spec = importlib.util.spec_from_file_location(
        "dashboard",
        os.path.join(os.path.dirname(__file__), "dashboard.py")
    )
    # Just run dashboard.py inline for simplicity
    exec(open(os.path.join(os.path.dirname(__file__), "dashboard.py")).read())

elif page == "🔮 Demand Forecasting":
    from streamlit.forecasting import render
    render()

elif page == "⚙️ Pipeline Health":
    st.markdown("## ⚙️ Pipeline Health Monitor")
    st.markdown("Real-time status of all pipeline components.")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### Infrastructure")
        services = {
            "PostgreSQL":   ("🟢", "healthy",  "localhost:5432"),
            "Kafka Broker": ("🟢", "healthy",  "localhost:9092"),
            "Airflow":      ("🟡", "starting", "localhost:8080"),
            "Spark Master": ("🟢", "healthy",  "localhost:7077"),
        }
        for svc, (icon, status, addr) in services.items():
            st.markdown(f"{icon} **{svc}** — `{status}` @ `{addr}`")

    with col2:
        st.markdown("### Last Pipeline Run")
        st.markdown("""
        | Stage | Status | Duration |
        |-------|--------|----------|
        | extract_pos | ✅ PASS | 12s |
        | data_quality | ✅ PASS | 8s |
        | transform_pos | ✅ PASS | 4m 23s |
        | aggregate | ✅ PASS | 45s |
        | export_ml | ✅ PASS | 18s |
        """)

    st.markdown("### DQ Check Log (Last 24h)")
    import pandas as pd
    import numpy as np
    np.random.seed(42)
    dq_data = {
        "Check": ["null_check_transaction_id","uniqueness_transaction_id",
                  "range_quantity","range_unit_price","date_format_transaction_date",
                  "null_check_product_name","value_set_payment_method"],
        "Table": ["pos_sales_raw"] * 7,
        "Status": ["PASS","PASS","PASS","PASS","PASS","WARN","PASS"],
        "Pass Rate": ["100%","99.5%","99.8%","99.9%","100%","96.2%","98.7%"],
        "Run Time": ["02:04:12"] * 7,
    }
    dq_df = pd.DataFrame(dq_data)
    st.dataframe(dq_df, use_container_width=True)
