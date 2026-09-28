"""
src/data_ingestion.py

Responsible for:
    1. Creating the Spark session
    2. Defining an explicit schema for the raw CSV
    3. Loading the raw CSV into a PySpark DataFrame
    4. Basic sanity checks (file exists, not empty)

Keeping ingestion in its own module makes the pipeline easy to follow:
CSV -> (this module) -> raw Spark DataFrame -> data_cleaning.py -> ...
"""

import os
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)


class DataIngestionError(Exception):
    """Raised when the raw dataset cannot be located or loaded."""
    pass


def get_spark_session(app_name: str = "EcommerceBDA") -> SparkSession:
    """
    Create (or reuse) a local SparkSession.

    We use local[*] so the project runs on a single Windows/Mac/Linux
    laptop without needing a real Hadoop/Spark cluster.
    """
    spark = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")   # small dataset -> few partitions is enough
        .config("spark.driver.memory", "2g")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")  # keep console output clean for students
    return spark


def get_raw_schema() -> StructType:
    """
    Explicit schema for the raw CSV.

    NOTE: We intentionally load unit_price / quantity / customer_age as
    StringType at ingestion time. Some rows contain missing or malformed
    numeric values (by design, see generate_dataset.py) and letting Spark
    infer/enforce numeric types too early would silently turn bad values
    into nulls before we get a chance to *count and report* them in the
    cleaning stage. Proper numeric casting happens explicitly in
    data_cleaning.py / data_processing.py.
    """
    return StructType(
        [
            StructField("transaction_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("product_category", StringType(), True),
            StructField("quantity", StringType(), True),
            StructField("unit_price", StringType(), True),
            StructField("transaction_date", StringType(), True),
            StructField("payment_method", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
            StructField("customer_age", StringType(), True),
            StructField("customer_gender", StringType(), True),
        ]
    )


def load_raw_data(spark: SparkSession, csv_path: str) -> DataFrame:
    """
    Load the raw e-commerce transactions CSV into a Spark DataFrame.

    Raises
    ------
    DataIngestionError
        If the file does not exist, or the CSV loads with zero rows.
    """
    path = Path(csv_path)

    if not path.exists():
        raise DataIngestionError(
            f"CSV file not found at '{csv_path}'.\n"
            f"-> Fix: run 'python generate_dataset.py' first to create the dataset."
        )

    if path.stat().st_size == 0:
        raise DataIngestionError(
            f"CSV file at '{csv_path}' is empty (0 bytes).\n"
            f"-> Fix: delete it and re-run 'python generate_dataset.py'."
        )

    schema = get_raw_schema()

    df = (
        spark.read.option("header", "true")
        .option("mode", "PERMISSIVE")   # don't crash on malformed rows; keep them for cleaning stage
        .schema(schema)
        .csv(str(path))
    )

    row_count = df.count()
    if row_count == 0:
        raise DataIngestionError(
            f"CSV file at '{csv_path}' loaded but contains 0 rows.\n"
            f"-> Fix: regenerate the dataset with 'python generate_dataset.py'."
        )

    print(f"[data_ingestion] Loaded {row_count} raw rows from {path.name}")
    return df
