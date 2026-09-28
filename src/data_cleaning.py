"""
src/data_cleaning.py

Responsible for cleaning the raw PySpark DataFrame produced by
data_ingestion.py:
    - Removing exact duplicate transactions
    - Handling missing values (drop or impute, depending on the column)
    - Validating / fixing invalid dates
    - Validating numeric columns (quantity, unit_price, customer_age)
    - Reporting how many bad rows were found and removed (for transparency)

Every function returns a NEW DataFrame (Spark DataFrames are immutable) and
prints a short summary so the console output doubles as a data-quality report.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def remove_duplicates(df: DataFrame) -> DataFrame:
    """Remove exact duplicate transaction rows based on transaction_id."""
    before = df.count()
    deduped = df.dropDuplicates(["transaction_id"])
    after = deduped.count()
    removed = before - after
    print(f"[data_cleaning] Removed {removed} duplicate transaction(s) "
          f"({before} -> {after} rows)")
    return deduped


def handle_missing_values(df: DataFrame) -> DataFrame:
    """
    Handle missing/null values column by column:
      - customer_gender, city, payment_method  -> fill with 'Unknown'
      - customer_age                           -> fill with dataset median
      - unit_price                             -> DROP the row (price is
                                                   essential for revenue
                                                   calculations, we can't
                                                   safely guess it)
    """
    before = df.count()

    # Categorical columns: fill missing with 'Unknown' rather than dropping,
    # since we still want the revenue/quantity info from that row.
    df = df.fillna(
        {
            "customer_gender": "Unknown",
            "city": "Unknown",
            "state": "Unknown",
            "payment_method": "Unknown",
        }
    )

    # customer_age: cast to double first (raw column is StringType), then
    # impute missing/invalid ages with the median age.
    df = df.withColumn("customer_age_numeric", F.col("customer_age").cast("double"))
    median_age_row = df.approxQuantile("customer_age_numeric", [0.5], 0.01)
    median_age = median_age_row[0] if median_age_row and median_age_row[0] is not None else 30.0

    df = df.withColumn(
        "customer_age",
        F.when(
            F.col("customer_age_numeric").isNull(), F.lit(median_age)
        ).otherwise(F.col("customer_age_numeric")),
    ).drop("customer_age_numeric")

    # unit_price is essential -- drop rows where it is null/blank.
    df = df.filter(F.col("unit_price").isNotNull() & (F.trim(F.col("unit_price")) != ""))

    after = df.count()
    print(f"[data_cleaning] Handled missing values "
          f"(dropped {before - after} rows with missing unit_price; "
          f"filled categorical nulls with 'Unknown'; "
          f"imputed missing ages with median={median_age:.1f})")
    return df


def validate_numeric_columns(df: DataFrame) -> DataFrame:
    """
    Cast quantity / unit_price to numeric types and remove rows with
    invalid values (non-numeric, negative, or zero) -- these represent
    corrupted / erroneous transactions that should not be counted as
    real sales.
    """
    before = df.count()

    df = df.withColumn("quantity", F.col("quantity").cast("integer"))
    df = df.withColumn("unit_price", F.col("unit_price").cast("double"))

    df = df.filter(
        F.col("quantity").isNotNull()
        & (F.col("quantity") > 0)
        & F.col("unit_price").isNotNull()
        & (F.col("unit_price") > 0)
    )

    after = df.count()
    print(f"[data_cleaning] Removed {before - after} row(s) with invalid/negative/zero "
          f"quantity or unit_price ({before} -> {after} rows)")
    return df


def validate_dates(df: DataFrame) -> DataFrame:
    """
    Parse transaction_date (string) into a proper DateType column.
    Rows with malformed dates (e.g. '31-02-2023', 'not_a_date') will
    produce a null after to_date() and are removed, since a transaction
    without a valid date cannot be placed in any time-based analysis
    (monthly trend, daily trend, etc.).
    """
    before = df.count()

    df = df.withColumn(
        "transaction_date_parsed", F.to_date(F.col("transaction_date"), "yyyy-MM-dd")
    )
    df = df.filter(F.col("transaction_date_parsed").isNotNull())
    df = df.drop("transaction_date").withColumnRenamed(
        "transaction_date_parsed", "transaction_date"
    )

    after = df.count()
    print(f"[data_cleaning] Removed {before - after} row(s) with invalid transaction_date "
          f"({before} -> {after} rows)")
    return df


def clean_pipeline(df: DataFrame) -> DataFrame:
    """
    Run the full cleaning pipeline in the correct order:
        1. Remove duplicates
        2. Handle missing values
        3. Validate numeric columns
        4. Validate dates
    """
    print("[data_cleaning] Starting data cleaning pipeline...")
    df = remove_duplicates(df)
    df = handle_missing_values(df)
    df = validate_numeric_columns(df)
    df = validate_dates(df)
    print(f"[data_cleaning] Cleaning complete. Final clean row count: {df.count()}")
    return df
