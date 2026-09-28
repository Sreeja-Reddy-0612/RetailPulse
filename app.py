"""
app.py

Streamlit dashboard for the "E-Commerce Transaction Analytics Using Apache
Spark" project.

IMPORTANT: This dashboard reads the ALREADY-PROCESSED output of the PySpark
pipeline (output/cleaned_data.csv, output/summary_kpis.json), which is
produced by running:

    python run_pipeline.py

before launching Streamlit. The heavy Big Data processing (cleaning,
transformation, aggregation, Spark SQL) happens in PySpark inside
run_pipeline.py. The dashboard itself uses pandas + Plotly on the small
(~10K row) cleaned dataset so that sidebar filters feel instant -- starting
a fresh Spark JVM on every filter click would make the UI painfully slow
and is unnecessary for a dataset this size.

Run:
    streamlit run app.py
"""

import json
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

# ---------------------------------------------------------------------------
# Page config (must be the first Streamlit call)
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="RetailPulse: Big Data Insights for Customer and Sales Intelligence",
    page_icon="🛒",
    layout="wide",
)

PROJECT_ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = PROJECT_ROOT / "output"
CLEANED_DATA_PATH = OUTPUT_DIR / "cleaned_data.csv"
KPI_JSON_PATH = OUTPUT_DIR / "summary_kpis.json"


# ---------------------------------------------------------------------------
# Data loading (cached so the CSV is only read once per session)
# ---------------------------------------------------------------------------
@st.cache_data
def load_data():
    if not CLEANED_DATA_PATH.exists() or not KPI_JSON_PATH.exists():
        return None, None

    df = pd.read_csv(CLEANED_DATA_PATH, parse_dates=["transaction_date"])
    with open(KPI_JSON_PATH, "r") as f:
        kpis = json.load(f)
    return df, kpis


df, base_kpis = load_data()

# ---------------------------------------------------------------------------
# Guard: pipeline hasn't been run yet
# ---------------------------------------------------------------------------
if df is None:
    st.title("🛒 RetailPulse: Big Data Insights for Customer and Sales Intelligence")
    st.error(
        "⚠️ No processed data found in the `output/` folder.\n\n"
        "Please run the PySpark pipeline first:\n\n"
        "```\n"
        "python generate_dataset.py\n"
        "python run_pipeline.py\n"
        "```\n\n"
        "Then restart this dashboard with `streamlit run app.py`."
    )
    st.stop()

if df.empty:
    st.error("The processed dataset (output/cleaned_data.csv) is empty. "
              "Please check data/ecommerce_transactions.csv and re-run run_pipeline.py.")
    st.stop()

# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------
st.title("🛒 RetailPulse: Big Data Insights for Customer and Sales Intelligence")
st.markdown(
    "A Big Data Analytics dashboard that processes e-commerce transaction "
    "data with **PySpark** (ingestion, cleaning, transformation, Spark SQL) "
    "and visualizes the results interactively with **Streamlit** and **Plotly**."
)
st.divider()

# ---------------------------------------------------------------------------
# Sidebar filters
# ---------------------------------------------------------------------------
st.sidebar.header("🔍 Filters")

min_date = df["transaction_date"].min().date()
max_date = df["transaction_date"].max().date()

date_range = st.sidebar.date_input(
    "Transaction Date Range",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date,
)

categories = sorted(df["product_category"].dropna().unique().tolist())
selected_categories = st.sidebar.multiselect(
    "Product Category", options=categories, default=categories
)

states = sorted(df["state"].dropna().unique().tolist())
selected_states = st.sidebar.multiselect("State", options=states, default=states)

payment_methods = sorted(df["payment_method"].dropna().unique().tolist())
selected_payments = st.sidebar.multiselect(
    "Payment Method", options=payment_methods, default=payment_methods
)

genders = sorted(df["customer_gender"].dropna().unique().tolist())
selected_genders = st.sidebar.multiselect("Gender", options=genders, default=genders)

if st.sidebar.button("Reset Filters"):
    st.rerun()

# ---------------------------------------------------------------------------
# Apply filters
# ---------------------------------------------------------------------------
if isinstance(date_range, tuple) and len(date_range) == 2:
    start_date, end_date = date_range
else:
    # Streamlit returns a single date while the user is mid-selection
    start_date, end_date = min_date, max_date

mask = (
    (df["transaction_date"].dt.date >= start_date)
    & (df["transaction_date"].dt.date <= end_date)
    & (df["product_category"].isin(selected_categories))
    & (df["state"].isin(selected_states))
    & (df["payment_method"].isin(selected_payments))
    & (df["customer_gender"].isin(selected_genders))
)
filtered_df = df.loc[mask].copy()

if filtered_df.empty:
    st.warning("No transactions match the selected filters. Try widening your filter selection.")
    st.stop()

# ---------------------------------------------------------------------------
# KPI cards (recomputed on the FILTERED data so the dashboard is interactive)
# ---------------------------------------------------------------------------
total_revenue = filtered_df["total_amount"].sum()
total_transactions = len(filtered_df)
avg_order_value = filtered_df["total_amount"].mean()
total_quantity_sold = filtered_df["quantity"].sum()

kpi1, kpi2, kpi3, kpi4 = st.columns(4)
kpi1.metric("💰 Total Revenue", f"₹{total_revenue:,.0f}")
kpi2.metric("🧾 Total Transactions", f"{total_transactions:,}")
kpi3.metric("📊 Avg Order Value", f"₹{avg_order_value:,.2f}")
kpi4.metric("📦 Total Quantity Sold", f"{total_quantity_sold:,}")

st.divider()

# ---------------------------------------------------------------------------
# Charts
# ---------------------------------------------------------------------------
row1_col1, row1_col2 = st.columns(2)

with row1_col1:
    st.subheader("Revenue by Category")
    cat_revenue = (
        filtered_df.groupby("product_category")["total_amount"]
        .sum()
        .sort_values(ascending=False)
        .reset_index()
    )
    fig = px.bar(
        cat_revenue, x="product_category", y="total_amount",
        labels={"product_category": "Category", "total_amount": "Revenue (₹)"},
        color="total_amount", color_continuous_scale="Blues",
    )
    st.plotly_chart(fig, use_container_width=True)

with row1_col2:
    st.subheader("Monthly Revenue Trend")
    monthly = (
        filtered_df.assign(year_month=filtered_df["transaction_date"].dt.to_period("M").astype(str))
        .groupby("year_month")["total_amount"]
        .sum()
        .reset_index()
        .sort_values("year_month")
    )
    fig = px.line(
        monthly, x="year_month", y="total_amount", markers=True,
        labels={"year_month": "Month", "total_amount": "Revenue (₹)"},
    )
    st.plotly_chart(fig, use_container_width=True)

row2_col1, row2_col2 = st.columns(2)

with row2_col1:
    st.subheader("Revenue by State (Top 10)")
    state_revenue = (
        filtered_df.groupby("state")["total_amount"]
        .sum()
        .sort_values(ascending=False)
        .head(10)
        .reset_index()
    )
    fig = px.bar(
        state_revenue, x="total_amount", y="state", orientation="h",
        labels={"state": "State", "total_amount": "Revenue (₹)"},
        color="total_amount", color_continuous_scale="Greens",
    )
    fig.update_layout(yaxis={"categoryorder": "total ascending"})
    st.plotly_chart(fig, use_container_width=True)

with row2_col2:
    st.subheader("Top 10 Products by Revenue")
    top_products = (
        filtered_df.groupby(["product_id", "product_category"])["total_amount"]
        .sum()
        .sort_values(ascending=False)
        .head(10)
        .reset_index()
    )
    fig = px.bar(
        top_products, x="total_amount", y="product_id", orientation="h",
        color="product_category",
        labels={"product_id": "Product", "total_amount": "Revenue (₹)"},
    )
    fig.update_layout(yaxis={"categoryorder": "total ascending"})
    st.plotly_chart(fig, use_container_width=True)

row3_col1, row3_col2 = st.columns(2)

with row3_col1:
    st.subheader("Payment Method Distribution")
    payment_dist = filtered_df["payment_method"].value_counts().reset_index()
    payment_dist.columns = ["payment_method", "count"]
    fig = px.pie(payment_dist, names="payment_method", values="count", hole=0.4)
    st.plotly_chart(fig, use_container_width=True)

with row3_col2:
    st.subheader("Customer Spending Analysis (Gender-wise)")
    gender_spending = (
        filtered_df.groupby("customer_gender")["total_amount"]
        .agg(["sum", "mean", "count"])
        .reset_index()
        .rename(columns={"sum": "total_spent", "mean": "avg_order_value", "count": "num_transactions"})
    )
    fig = px.bar(
        gender_spending, x="customer_gender", y="total_spent",
        color="customer_gender",
        labels={"customer_gender": "Gender", "total_spent": "Total Spend (₹)"},
    )
    st.plotly_chart(fig, use_container_width=True)

st.divider()

# ---------------------------------------------------------------------------
# Extra: Age-group spending + Top customers table
# ---------------------------------------------------------------------------
row4_col1, row4_col2 = st.columns(2)

with row4_col1:
    st.subheader("Age-Group-wise Spending")
    bins = [17, 24, 34, 44, 54, 100]
    labels = ["18-24", "25-34", "35-44", "45-54", "55+"]
    filtered_df["age_group"] = pd.cut(filtered_df["customer_age"], bins=bins, labels=labels)
    age_spending = filtered_df.groupby("age_group", observed=True)["total_amount"].sum().reset_index()
    fig = px.bar(
        age_spending, x="age_group", y="total_amount",
        labels={"age_group": "Age Group", "total_amount": "Total Spend (₹)"},
    )
    st.plotly_chart(fig, use_container_width=True)

with row4_col2:
    st.subheader("Top 10 Customers by Spending")
    top_customers = (
        filtered_df.groupby("customer_id")["total_amount"]
        .sum()
        .sort_values(ascending=False)
        .head(10)
        .reset_index()
        .rename(columns={"total_amount": "total_spent"})
    )
    st.dataframe(top_customers, use_container_width=True, hide_index=True)

st.divider()
st.caption(
    "Data processed with Apache Spark (PySpark) · Visualized with Streamlit & Plotly · "
    "B.Tech Big Data Analytics Academic Project"
)
