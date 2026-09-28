"""
src/data_processing.py

Transforms the CLEANED PySpark DataFrame into an ANALYTICS-READY DataFrame:
    - Calculates total_amount = quantity * unit_price
    - Extracts year / month / month_name / day_of_week from transaction_date
    - Creates an age_group bucket for age-wise analysis
    - Ensures final column typing is correct for downstream aggregations

This module sits between data_cleaning.py and analytics.py in the pipeline.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


def add_total_amount(df: DataFrame) -> DataFrame:
    """Add the calculated column total_amount = quantity * unit_price."""
    return df.withColumn("total_amount", F.round(F.col("quantity") * F.col("unit_price"), 2))


def add_date_parts(df: DataFrame) -> DataFrame:
    """
    Break transaction_date into useful parts for time-based analytics:
        year, month (1-12), month_name (e.g. 'Jan-2024'), day_of_week
    """
    df = df.withColumn("year", F.year(F.col("transaction_date")))
    df = df.withColumn("month", F.month(F.col("transaction_date")))
    df = df.withColumn(
        "year_month", F.date_format(F.col("transaction_date"), "yyyy-MM")
    )
    df = df.withColumn(
        "month_name", F.date_format(F.col("transaction_date"), "MMM-yyyy")
    )
    df = df.withColumn("day_of_week", F.date_format(F.col("transaction_date"), "EEEE"))
    return df


def add_age_group(df: DataFrame) -> DataFrame:
    """
    Bucket customer_age into readable age groups for the
    'age-group-wise spending' analysis.
    """
    df = df.withColumn(
        "age_group",
        F.when(F.col("customer_age") < 25, "18-24")
        .when((F.col("customer_age") >= 25) & (F.col("customer_age") < 35), "25-34")
        .when((F.col("customer_age") >= 35) & (F.col("customer_age") < 45), "35-44")
        .when((F.col("customer_age") >= 45) & (F.col("customer_age") < 55), "45-54")
        .otherwise("55+")
        .cast(StringType()),
    )
    return df


def process_pipeline(df: DataFrame) -> DataFrame:
    """
    Run the full transformation pipeline in order:
        1. Add total_amount
        2. Add date parts (year, month, year_month, month_name, day_of_week)
        3. Add age_group bucket
    """
    print("[data_processing] Adding calculated columns...")
    df = add_total_amount(df)
    df = add_date_parts(df)
    df = add_age_group(df)
    print("[data_processing] Transformation complete. Columns:", df.columns)
    return df
