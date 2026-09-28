"""
run_pipeline.py

Orchestrates the full Big Data pipeline:

    CSV Dataset
        |
    PySpark Data Ingestion      (src/data_ingestion.py)
        |
    Data Cleaning                (src/data_cleaning.py)
        |
    Data Transformation          (src/data_processing.py)
        |
    Spark SQL / DataFrame Analytics  (src/analytics.py, src/spark_sql_queries.py)
        |
    Processed Data -> output/*.csv + output/summary_kpis.json
        |
    (Streamlit dashboard reads output/ in app.py)

Run:
    python run_pipeline.py

This script must be run ONCE (or whenever the dataset changes) BEFORE
launching the Streamlit dashboard, since app.py reads its input from
the output/ folder rather than re-running Spark on every page interaction
(this keeps the dashboard fast and avoids repeated JVM startup costs).
"""

import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.data_ingestion import get_spark_session, load_raw_data, DataIngestionError
from src.data_cleaning import clean_pipeline
from src.data_processing import process_pipeline
from src.analytics import run_all_analytics
from src.spark_sql_queries import run_all_sql_queries

DATA_PATH = PROJECT_ROOT / "data" / "ecommerce_transactions.csv"
OUTPUT_DIR = PROJECT_ROOT / "output"


def save_pandas_df(pdf, name: str) -> None:
    """Save a pandas DataFrame to output/<name>.csv"""
    out_path = OUTPUT_DIR / f"{name}.csv"
    pdf.to_csv(out_path, index=False)
    print(f"[run_pipeline] Saved {out_path.relative_to(PROJECT_ROOT)} ({len(pdf)} rows)")


def main():
    print("=" * 80)
    print("E-COMMERCE TRANSACTION ANALYTICS -- PYSPARK PIPELINE")
    print("=" * 80)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    spark = get_spark_session()
    try:
        # 1. INGESTION -------------------------------------------------------
        try:
            raw_df = load_raw_data(spark, str(DATA_PATH))
        except DataIngestionError as e:
            print(f"\n[ERROR] Data ingestion failed:\n{e}\n")
            sys.exit(1)

        # 2. CLEANING ----------------------------------------------------------
        clean_df = clean_pipeline(raw_df)

        if clean_df.count() == 0:
            print("\n[ERROR] No rows survived the cleaning stage. "
                  "Check the raw CSV for excessive bad data.\n")
            sys.exit(1)

        # 3. TRANSFORMATION ------------------------------------------------------
        processed_df = process_pipeline(clean_df)

        # Cache since it will be reused across many aggregations + SQL queries
        processed_df.cache()
        processed_df.count()  # materialize the cache

        # 4a. ANALYTICS (DataFrame API) ---------------------------------------
        results = run_all_analytics(processed_df)

        # Save each analytic result to output/ as CSV
        summary_kpis = results.pop("summary_kpis")
        highest_month = results.pop("highest_revenue_month")

        for name, pdf in results.items():
            save_pandas_df(pdf, name)

        # Save scalar KPIs + highest revenue month as JSON
        summary_kpis["highest_revenue_month"] = highest_month["month_name"]
        summary_kpis["highest_revenue_month_amount"] = highest_month["revenue"]
        with open(OUTPUT_DIR / "summary_kpis.json", "w") as f:
            json.dump(summary_kpis, f, indent=2)
        print(f"[run_pipeline] Saved output/summary_kpis.json -> {summary_kpis}")

        # 4b. SPARK SQL DEMO ---------------------------------------------------
        sql_results = run_all_sql_queries(spark, processed_df)
        for name, pdf in sql_results.items():
            save_pandas_df(pdf, f"sql_{name}")

        # 5. SAVE CLEANED DATA FOR THE DASHBOARD (interactive filtering) -------
        # The dashboard (app.py) uses pandas for fast, interactive filtering
        # since the cleaned dataset is small (~10K rows). The heavy lifting
        # (cleaning + transformation + aggregation) was already done in Spark
        # above -- this is standard practice for small-to-medium BI dashboards.
        cleaned_pdf = processed_df.toPandas()
        cleaned_pdf.to_csv(OUTPUT_DIR / "cleaned_data.csv", index=False)
        print(f"[run_pipeline] Saved output/cleaned_data.csv ({len(cleaned_pdf)} rows)")

        print("\n" + "=" * 80)
        print("PIPELINE COMPLETED SUCCESSFULLY")
        print(f"All outputs saved to: {OUTPUT_DIR}")
        print("Next step: run  streamlit run app.py")
        print("=" * 80)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
