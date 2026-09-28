"""
src/analytics.py

Implements all Big Data analytics required for the project using PySpark
DataFrame operations (groupBy, agg, orderBy, window-style top-N, etc.).

Every function takes the analytics-ready Spark DataFrame (output of
data_processing.process_pipeline) and returns a small pandas DataFrame
(or a plain Python scalar/dict for single-value KPIs) -- this is the
"Processed Data" stage of the architecture, ready to be handed to the
Streamlit dashboard or saved to output/ as CSV.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


# ---------------------------------------------------------------------------
# A-D, Q: Top-level KPIs
# ---------------------------------------------------------------------------
def get_summary_kpis(df: DataFrame) -> dict:
    """
    A. Total Revenue
    B. Total Transactions
    C. Average Transaction Value (= Average Order Value, Q)
    D. Total Quantity Sold
    """
    result = df.agg(
        F.round(F.sum("total_amount"), 2).alias("total_revenue"),
        F.count("transaction_id").alias("total_transactions"),
        F.round(F.avg("total_amount"), 2).alias("avg_order_value"),
        F.sum("quantity").alias("total_quantity_sold"),
    ).collect()[0]

    return {
        "total_revenue": float(result["total_revenue"] or 0),
        "total_transactions": int(result["total_transactions"] or 0),
        "avg_order_value": float(result["avg_order_value"] or 0),
        "total_quantity_sold": int(result["total_quantity_sold"] or 0),
    }


# ---------------------------------------------------------------------------
# E. Revenue by Product Category  (also feeds O. Best-selling categories)
# ---------------------------------------------------------------------------
def revenue_by_category(df: DataFrame):
    return (
        df.groupBy("product_category")
        .agg(
            F.round(F.sum("total_amount"), 2).alias("revenue"),
            F.sum("quantity").alias("units_sold"),
            F.count("transaction_id").alias("num_transactions"),
        )
        .orderBy(F.desc("revenue"))
        .toPandas()
    )


# ---------------------------------------------------------------------------
# F. Revenue by State
# ---------------------------------------------------------------------------
def revenue_by_state(df: DataFrame):
    return (
        df.groupBy("state")
        .agg(F.round(F.sum("total_amount"), 2).alias("revenue"),
             F.count("transaction_id").alias("num_transactions"))
        .orderBy(F.desc("revenue"))
        .toPandas()
    )


# ---------------------------------------------------------------------------
# G. Revenue by City
# ---------------------------------------------------------------------------
def revenue_by_city(df: DataFrame):
    return (
        df.groupBy("city")
        .agg(F.round(F.sum("total_amount"), 2).alias("revenue"),
             F.count("transaction_id").alias("num_transactions"))
        .orderBy(F.desc("revenue"))
        .toPandas()
    )


# ---------------------------------------------------------------------------
# H. Monthly Revenue Trend  (also feeds P. Highest Revenue Month)
# ---------------------------------------------------------------------------
def monthly_revenue_trend(df: DataFrame):
    return (
        df.groupBy("year_month", "month_name")
        .agg(F.round(F.sum("total_amount"), 2).alias("revenue"))
        .orderBy("year_month")
        .toPandas()
    )


# ---------------------------------------------------------------------------
# I. Daily Transaction Trend
# ---------------------------------------------------------------------------
def daily_transaction_trend(df: DataFrame):
    return (
        df.groupBy("transaction_date")
        .agg(
            F.count("transaction_id").alias("num_transactions"),
            F.round(F.sum("total_amount"), 2).alias("revenue"),
        )
        .orderBy("transaction_date")
        .toPandas()
    )


# ---------------------------------------------------------------------------
# J. Top 10 Products by Revenue
# ---------------------------------------------------------------------------
def top_10_products(df: DataFrame):
    return (
        df.groupBy("product_id", "product_category")
        .agg(
            F.round(F.sum("total_amount"), 2).alias("revenue"),
            F.sum("quantity").alias("units_sold"),
        )
        .orderBy(F.desc("revenue"))
        .limit(10)
        .toPandas()
    )


# ---------------------------------------------------------------------------
# K. Top 10 Customers by Spending
# ---------------------------------------------------------------------------
def top_10_customers(df: DataFrame):
    return (
        df.groupBy("customer_id")
        .agg(
            F.round(F.sum("total_amount"), 2).alias("total_spent"),
            F.count("transaction_id").alias("num_orders"),
        )
        .orderBy(F.desc("total_spent"))
        .limit(10)
        .toPandas()
    )


# ---------------------------------------------------------------------------
# L. Payment Method Distribution
# ---------------------------------------------------------------------------
def payment_method_distribution(df: DataFrame):
    return (
        df.groupBy("payment_method")
        .agg(
            F.count("transaction_id").alias("num_transactions"),
            F.round(F.sum("total_amount"), 2).alias("revenue"),
        )
        .orderBy(F.desc("num_transactions"))
        .toPandas()
    )


# ---------------------------------------------------------------------------
# M. Gender-wise Spending
# ---------------------------------------------------------------------------
def gender_wise_spending(df: DataFrame):
    return (
        df.groupBy("customer_gender")
        .agg(
            F.round(F.sum("total_amount"), 2).alias("total_spent"),
            F.count("transaction_id").alias("num_transactions"),
            F.round(F.avg("total_amount"), 2).alias("avg_order_value"),
        )
        .orderBy(F.desc("total_spent"))
        .toPandas()
    )


# ---------------------------------------------------------------------------
# N. Age-group-wise Spending
# ---------------------------------------------------------------------------
def age_group_spending(df: DataFrame):
    return (
        df.groupBy("age_group")
        .agg(
            F.round(F.sum("total_amount"), 2).alias("total_spent"),
            F.count("transaction_id").alias("num_transactions"),
        )
        .orderBy("age_group")
        .toPandas()
    )


# ---------------------------------------------------------------------------
# P. Highest Revenue Month (derived from monthly_revenue_trend)
# ---------------------------------------------------------------------------
def highest_revenue_month(df: DataFrame) -> dict:
    row = (
        df.groupBy("year_month", "month_name")
        .agg(F.round(F.sum("total_amount"), 2).alias("revenue"))
        .orderBy(F.desc("revenue"))
        .limit(1)
        .collect()
    )
    if not row:
        return {"month_name": "N/A", "revenue": 0.0}
    return {"month_name": row[0]["month_name"], "revenue": float(row[0]["revenue"])}


def run_all_analytics(df: DataFrame) -> dict:
    """
    Convenience function used by run_pipeline.py: runs every analysis and
    returns a dictionary of {name: result} so the caller can save each
    result to output/ in one loop.
    """
    print("[analytics] Running all Big Data analytics on the processed DataFrame...")
    results = {
        "summary_kpis": get_summary_kpis(df),
        "revenue_by_category": revenue_by_category(df),
        "revenue_by_state": revenue_by_state(df),
        "revenue_by_city": revenue_by_city(df),
        "monthly_revenue_trend": monthly_revenue_trend(df),
        "daily_transaction_trend": daily_transaction_trend(df),
        "top_10_products": top_10_products(df),
        "top_10_customers": top_10_customers(df),
        "payment_method_distribution": payment_method_distribution(df),
        "gender_wise_spending": gender_wise_spending(df),
        "age_group_spending": age_group_spending(df),
        "highest_revenue_month": highest_revenue_month(df),
    }
    print("[analytics] All analytics computed successfully.")
    return results
