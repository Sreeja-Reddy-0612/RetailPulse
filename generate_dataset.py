"""
generate_dataset.py

Generates a realistic synthetic E-Commerce transaction dataset (CSV) with
Indian cities, states, payment methods and product categories.

Run:
    python generate_dataset.py

Output:
    data/ecommerce_transactions.csv   (>= 10,000 rows)

NOTE: A small percentage of "dirty" records (missing values, duplicate
transaction_ids, invalid dates, invalid numeric values) are intentionally
injected so that the data-cleaning stage of the pipeline (src/data_cleaning.py)
has real problems to fix -- this mirrors real-world Big Data pipelines.
"""

import os
import random
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Reproducibility
# ---------------------------------------------------------------------------
random.seed(42)
np.random.seed(42)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
NUM_RECORDS = 10500          # slightly more than 10,000 so that after we
                              # remove some rows during cleaning we still
                              # comfortably exceed the 10,000-record target
OUTPUT_DIR = Path(__file__).resolve().parent / "data"
OUTPUT_FILE = OUTPUT_DIR / "ecommerce_transactions.csv"

# Realistic Indian reference data -------------------------------------------------
CITY_STATE_MAP = {
    "Hyderabad": "Telangana",
    "Bangalore": "Karnataka",
    "Mumbai": "Maharashtra",
    "Delhi": "Delhi",
    "Chennai": "Tamil Nadu",
    "Pune": "Maharashtra",
    "Kolkata": "West Bengal",
    "Ahmedabad": "Gujarat",
    "Jaipur": "Rajasthan",
    "Kochi": "Kerala",
    "Lucknow": "Uttar Pradesh",
    "Chandigarh": "Chandigarh",
    "Indore": "Madhya Pradesh",
    "Nagpur": "Maharashtra",
    "Coimbatore": "Tamil Nadu",
    "Visakhapatnam": "Andhra Pradesh",
    "Bhopal": "Madhya Pradesh",
    "Patna": "Bihar",
    "Surat": "Gujarat",
    "Guwahati": "Assam",
}
CITIES = list(CITY_STATE_MAP.keys())

PAYMENT_METHODS = ["UPI", "Credit Card", "Debit Card", "Net Banking", "Cash on Delivery", "Wallet"]
PAYMENT_WEIGHTS = [0.35, 0.18, 0.15, 0.10, 0.15, 0.07]

PRODUCT_CATEGORIES = {
    "Electronics": ["Smartphone", "Laptop", "Headphones", "Smartwatch", "Bluetooth Speaker", "Power Bank"],
    "Fashion": ["T-Shirt", "Jeans", "Saree", "Kurta", "Sneakers", "Handbag"],
    "Home & Kitchen": ["Mixer Grinder", "Cookware Set", "Bedsheet", "Water Bottle", "Air Fryer", "Curtains"],
    "Beauty & Personal Care": ["Face Wash", "Shampoo", "Perfume", "Lipstick", "Sunscreen", "Trimmer"],
    "Books": ["Novel", "Textbook", "Comic Book", "Biography", "Self-Help Book", "Cookbook"],
    "Sports & Fitness": ["Yoga Mat", "Dumbbells", "Cricket Bat", "Running Shoes", "Football", "Resistance Band"],
    "Groceries": ["Rice Pack", "Cooking Oil", "Spices Combo", "Snacks Pack", "Tea Powder", "Atta Pack"],
    "Toys & Baby Products": ["Building Blocks", "Remote Car", "Baby Diapers", "Stroller", "Soft Toy", "Feeding Bottle"],
}
CATEGORY_LIST = list(PRODUCT_CATEGORIES.keys())

# Approx typical unit-price ranges (INR) per category, used to keep amounts realistic
CATEGORY_PRICE_RANGE = {
    "Electronics": (799, 65000),
    "Fashion": (299, 4500),
    "Home & Kitchen": (199, 12000),
    "Beauty & Personal Care": (99, 2500),
    "Books": (99, 1200),
    "Sports & Fitness": (199, 8000),
    "Groceries": (49, 1500),
    "Toys & Baby Products": (149, 5000),
}

GENDERS = ["Male", "Female", "Other"]
GENDER_WEIGHTS = [0.48, 0.48, 0.04]

START_DATE = datetime(2023, 1, 1)
END_DATE = datetime(2024, 12, 31)
DATE_RANGE_DAYS = (END_DATE - START_DATE).days


def random_date():
    """Return a random datetime between START_DATE and END_DATE."""
    offset = random.randint(0, DATE_RANGE_DAYS)
    return START_DATE + timedelta(days=offset)


def build_product_catalog():
    """Create a fixed catalog of product_id -> (category, product_name, base_price)."""
    catalog = []
    product_id = 1000
    for category, products in PRODUCT_CATEGORIES.items():
        low, high = CATEGORY_PRICE_RANGE[category]
        for product_name in products:
            product_id += 1
            base_price = round(random.uniform(low, high), 2)
            catalog.append(
                {
                    "product_id": f"P{product_id}",
                    "product_category": category,
                    "product_name": product_name,
                    "base_price": base_price,
                }
            )
    return catalog


def generate_records(num_records: int, catalog: list) -> pd.DataFrame:
    """Generate the core (clean) transaction records."""
    num_customers = max(500, num_records // 8)  # repeat customers, like real e-commerce
    customer_ids = [f"CUST{str(i).zfill(5)}" for i in range(1, num_customers + 1)]

    rows = []
    for i in range(1, num_records + 1):
        product = random.choice(catalog)
        city = random.choice(CITIES)
        state = CITY_STATE_MAP[city]
        quantity = np.random.choice([1, 2, 3, 4, 5], p=[0.55, 0.25, 0.10, 0.06, 0.04])
        # small +/- variation around the base price to simulate discounts/markups
        unit_price = round(product["base_price"] * random.uniform(0.85, 1.10), 2)

        rows.append(
            {
                "transaction_id": f"TXN{str(i).zfill(7)}",
                "customer_id": random.choice(customer_ids),
                "product_id": product["product_id"],
                "product_category": product["product_category"],
                "quantity": int(quantity),
                "unit_price": unit_price,
                "transaction_date": random_date().strftime("%Y-%m-%d"),
                "payment_method": np.random.choice(PAYMENT_METHODS, p=PAYMENT_WEIGHTS),
                "city": city,
                "state": state,
                "customer_age": int(np.clip(np.random.normal(32, 9), 18, 70)),
                "customer_gender": np.random.choice(GENDERS, p=GENDER_WEIGHTS),
            }
        )
    return pd.DataFrame(rows)


def inject_dirty_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Intentionally introduce realistic data-quality problems so the
    cleaning stage (src/data_cleaning.py) has real work to do:
      - missing values in a few columns
      - a handful of duplicate transaction_ids (full duplicate rows)
      - a few invalid / malformed dates
      - a few invalid numeric values (negative / zero / non-numeric-like)
    """
    df = df.copy()
    n = len(df)
    rng = np.random.default_rng(42)

    # 1. Missing values (~1.5% of rows across a few columns)
    for col in ["unit_price", "customer_age", "customer_gender", "city", "payment_method"]:
        missing_idx = rng.choice(n, size=int(n * 0.005), replace=False)
        df.loc[missing_idx, col] = np.nan

    # 2. Duplicate rows (~1% duplicated transactions -- same transaction_id re-appended)
    dup_rows = df.sample(n=int(n * 0.01), random_state=42)
    df = pd.concat([df, dup_rows], ignore_index=True)

    # 3. Invalid / malformed dates (~0.5%)
    n_after = len(df)
    bad_date_idx = rng.choice(n_after, size=int(n_after * 0.005), replace=False)
    bad_dates = ["31-02-2023", "2023/13/45", "not_a_date", "0000-00-00", ""]
    for idx in bad_date_idx:
        df.loc[idx, "transaction_date"] = random.choice(bad_dates)

    # 4. Invalid numeric values (~0.5%) -- negative or zero quantity/price
    bad_num_idx = rng.choice(n_after, size=int(n_after * 0.005), replace=False)
    for idx in bad_num_idx:
        if random.random() < 0.5:
            df.loc[idx, "quantity"] = random.choice([-1, 0])
        else:
            df.loc[idx, "unit_price"] = random.choice([-100.0, 0.0])

    return df.sample(frac=1, random_state=42).reset_index(drop=True)  # shuffle rows


def main():
    print("Generating synthetic e-commerce transaction dataset...")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    catalog = build_product_catalog()
    clean_df = generate_records(NUM_RECORDS, catalog)
    final_df = inject_dirty_data(clean_df)

    final_df.to_csv(OUTPUT_FILE, index=False)

    print(f"Dataset generated successfully -> {OUTPUT_FILE}")
    print(f"Total rows written (including intentional dirty/duplicate rows): {len(final_df)}")
    print("Columns:", list(final_df.columns))
    print("\nSample rows:")
    print(final_df.head(5).to_string(index=False))


if __name__ == "__main__":
    main()
