"""
src/spark_sql_queries.py

Demonstrates Spark SQL (as opposed to the DataFrame API used in
analytics.py) by registering the processed DataFrame as a temporary SQL
view and running plain SQL queries against it.

This module can be:
  1. Imported and called from run_pipeline.py (see run_spark_sql_demo), OR
  2. Run standalone for a classroom/viva demo:
        python src/spark_sql_queries.py

Both paths load data/ecommerce_transactions.csv, clean it, process it,
and then run the SQL queries -- so this file works independently too.
"""

import os
import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

# Allow running this file directly (python src/spark_sql_queries.py)
# as well as importing it as a package module (from src import spark_sql_queries)
CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.data_ingestion import get_spark_session, load_raw_data
from src.data_cleaning import clean_pipeline
from src.data_processing import process_pipeline

VIEW_NAME = "transactions"

# ---------------------------------------------------------------------------
# The 5+ required Spark SQL queries, kept as named, readable SQL strings.
# ---------------------------------------------------------------------------
SQL_QUERIES = {
    "top_10_customers_by_revenue": f"""
        SELECT customer_id,
               ROUND(SUM(total_amount), 2) AS total_spent,
               COUNT(transaction_id)       AS num_orders
        FROM {VIEW_NAME}
        GROUP BY customer_id
        ORDER BY total_spent DESC
        LIMIT 10
    """,
    "top_10_products_by_revenue": f"""
        SELECT product_id,
               product_category,
               ROUND(SUM(total_amount), 2) AS revenue,
               SUM(quantity)                AS units_sold
        FROM {VIEW_NAME}
        GROUP BY product_id, product_category
        ORDER BY revenue DESC
        LIMIT 10
    """,
    "monthly_revenue": f"""
        SELECT year_month,
               month_name,
               ROUND(SUM(total_amount), 2) AS revenue
        FROM {VIEW_NAME}
        GROUP BY year_month, month_name
        ORDER BY year_month
    """,
    "category_wise_revenue": f"""
        SELECT product_category,
               ROUND(SUM(total_amount), 2) AS revenue,
               SUM(quantity)                AS units_sold
        FROM {VIEW_NAME}
        GROUP BY product_category
        ORDER BY revenue DESC
    """,
    "state_wise_revenue": f"""
        SELECT state,
               ROUND(SUM(total_amount), 2) AS revenue,
               COUNT(transaction_id)        AS num_transactions
        FROM {VIEW_NAME}
        GROUP BY state
        ORDER BY revenue DESC
    """,
    "payment_method_summary": f"""
        SELECT payment_method,
               COUNT(transaction_id)        AS num_transactions,
               ROUND(SUM(total_amount), 2)  AS revenue,
               ROUND(AVG(total_amount), 2)  AS avg_order_value
        FROM {VIEW_NAME}
        GROUP BY payment_method
        ORDER BY revenue DESC
    """,
    "high_value_orders_above_avg": f"""
        SELECT transaction_id, customer_id, product_category, total_amount
        FROM {VIEW_NAME}
        WHERE total_amount > (SELECT AVG(total_amount) FROM {VIEW_NAME})
        ORDER BY total_amount DESC
        LIMIT 10
    """,
}


def register_view(spark: SparkSession, df: DataFrame, view_name: str = VIEW_NAME) -> None:
    """Register the processed DataFrame as a temporary SQL view."""
    df.createOrReplaceTempView(view_name)
    print(f"[spark_sql_queries] Registered temp view '{view_name}'")


def run_query(spark: SparkSession, query_name: str):
    """Run a single named query from SQL_QUERIES and return a pandas DataFrame."""
    if query_name not in SQL_QUERIES:
        raise ValueError(f"Unknown query '{query_name}'. Available: {list(SQL_QUERIES.keys())}")
    result_df = spark.sql(SQL_QUERIES[query_name])
    return result_df.toPandas()


def run_all_sql_queries(spark: SparkSession, df: DataFrame) -> dict:
    """Register the view and run every query in SQL_QUERIES. Returns dict of pandas DataFrames."""
    register_view(spark, df)
    results = {}
    for name, query in SQL_QUERIES.items():
        print(f"[spark_sql_queries] Running query: {name}")
        results[name] = spark.sql(query).toPandas()
    return results


def _standalone_demo():
    """Run this file directly for a quick classroom/viva demo of Spark SQL."""
    project_root = PROJECT_ROOT
    csv_path = project_root / "data" / "ecommerce_transactions.csv"

    spark = get_spark_session(app_name="EcommerceBDA-SparkSQLDemo")
    try:
        raw_df = load_raw_data(spark, str(csv_path))
        clean_df = clean_pipeline(raw_df)
        processed_df = process_pipeline(clean_df)

        register_view(spark, processed_df)

        for name, query in SQL_QUERIES.items():
            print("\n" + "=" * 80)
            print(f"QUERY: {name}")
            print("=" * 80)
            spark.sql(query).show(10, truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    _standalone_demo()
