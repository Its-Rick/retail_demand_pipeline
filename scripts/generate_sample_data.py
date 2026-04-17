"""
generate_sample_data.py
-----------------------
Generates realistic synthetic retail data for local development and testing.
Outputs:
  - data/sample/pos_sales.csv        → POS transactions
  - data/sample/ecommerce_events.json → E-commerce clickstream events
  - data/sample/weather.csv          → External weather data
  - data/sample/holidays.csv         → Public holidays
"""

import os
import json
import random
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from faker import Faker

fake = Faker()
random.seed(42)
np.random.seed(42)

# ─── CONFIG ───────────────────────────────────────────────────────────────────
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "sample")
os.makedirs(OUTPUT_DIR, exist_ok=True)

START_DATE = datetime(2023, 1, 1)
END_DATE   = datetime(2024, 1, 1)
NUM_STORES   = 10
NUM_PRODUCTS = 50
NUM_POS_ROWS = 50_000
NUM_ECOM_EVENTS = 20_000

# ─── MASTER DIMENSIONS ────────────────────────────────────────────────────────
STORES = [
    {"store_id": f"S{i:03d}", "store_name": fake.company(), "city": fake.city(),
     "state": fake.state_abbr(), "region": random.choice(["North","South","East","West"])}
    for i in range(1, NUM_STORES + 1)
]

CATEGORIES = ["Electronics", "Clothing", "Grocery", "Home & Garden", "Sports", "Toys"]
PRODUCTS = [
    {"product_id": f"P{i:04d}",
     "product_name": fake.catch_phrase(),
     "category": random.choice(CATEGORIES),
     "unit_price": round(random.uniform(5.0, 500.0), 2),
     "brand": fake.company()}
    for i in range(1, NUM_PRODUCTS + 1)
]

# ─── GENERATE POS SALES DATA ──────────────────────────────────────────────────
def generate_pos_sales():
    """Generates POS transaction records with realistic seasonality."""
    print("Generating POS sales data...")
    rows = []
    date_range = pd.date_range(START_DATE, END_DATE - timedelta(days=1), freq="D")

    for _ in range(NUM_POS_ROWS):
        txn_date = random.choice(date_range)
        store    = random.choice(STORES)
        product  = random.choice(PRODUCTS)

        # Weekend boost (+30%), holiday-season boost (Dec +50%)
        qty_multiplier = 1.0
        if txn_date.dayofweek >= 5:   qty_multiplier *= 1.3
        if txn_date.month == 12:      qty_multiplier *= 1.5

        quantity  = max(1, int(np.random.poisson(3) * qty_multiplier))
        discount  = round(random.choice([0, 0, 0, 0.05, 0.10, 0.15]), 2)
        unit_price = product["unit_price"]
        total_amt  = round(quantity * unit_price * (1 - discount), 2)

        # Inject ~3% missing values for realism
        rows.append({
            "transaction_id": fake.uuid4(),
            "transaction_date": txn_date.strftime("%Y-%m-%d"),
            "transaction_time": fake.time(),
            "store_id":   store["store_id"],
            "store_name": store["store_name"],
            "city":       store["city"],
            "state":      store["state"],
            "region":     store["region"],
            "product_id": product["product_id"],
            "product_name": product["product_name"] if random.random() > 0.03 else None,
            "category":   product["category"],
            "brand":      product["brand"],
            "unit_price": unit_price,
            "quantity":   quantity,
            "discount":   discount,
            "total_amount": total_amt,
            "payment_method": random.choice(["Cash","Credit","Debit","UPI"]),
            "customer_id": f"C{random.randint(1000,9999)}" if random.random() > 0.2 else None,
        })

    df = pd.DataFrame(rows)

    # Inject duplicate rows (~1%)
    dupe_rows = df.sample(frac=0.01, random_state=1)
    df = pd.concat([df, dupe_rows], ignore_index=True).sample(frac=1, random_state=42)

    out_path = os.path.join(OUTPUT_DIR, "pos_sales.csv")
    df.to_csv(out_path, index=False)
    print(f"  ✓ POS data saved → {out_path}  ({len(df):,} rows)")
    return df


# ─── GENERATE E-COMMERCE EVENTS ───────────────────────────────────────────────
def generate_ecommerce_events():
    """Generates JSON clickstream events: view, add_to_cart, purchase."""
    print("Generating e-commerce events...")
    events = []
    event_types = ["product_view", "add_to_cart", "remove_from_cart", "purchase", "wishlist_add"]
    date_range = pd.date_range(START_DATE, END_DATE - timedelta(days=1), freq="D")

    for _ in range(NUM_ECOM_EVENTS):
        product = random.choice(PRODUCTS)
        txn_date = random.choice(date_range)
        event = {
            "event_id":        fake.uuid4(),
            "event_type":      random.choice(event_types),
            "event_timestamp": f"{txn_date.strftime('%Y-%m-%d')}T{fake.time()}Z",
            "session_id":      fake.uuid4(),
            "user_id":         f"U{random.randint(10000, 99999)}",
            "product_id":      product["product_id"],
            "product_name":    product["product_name"],
            "category":        product["category"],
            "price":           product["unit_price"],
            "quantity":        random.randint(1, 5) if random.choice(event_types) == "purchase" else None,
            "device":          random.choice(["mobile","desktop","tablet"]),
            "platform":        random.choice(["web","ios","android"]),
            "referrer":        random.choice(["google","direct","email","social","affiliate"]),
            "geo": {
                "country": "US",
                "state": random.choice(["CA","TX","NY","FL","IL"])
            }
        }
        events.append(event)

    out_path = os.path.join(OUTPUT_DIR, "ecommerce_events.json")
    with open(out_path, "w") as f:
        json.dump(events, f, indent=2)
    print(f"  ✓ E-commerce events saved → {out_path}  ({len(events):,} records)")
    return events


# ─── GENERATE WEATHER DATA ────────────────────────────────────────────────────
def generate_weather():
    """Generates daily weather data per state (external enrichment source)."""
    print("Generating weather data...")
    states = list(set(s["state"] for s in STORES))
    rows = []
    for single_date in pd.date_range(START_DATE, END_DATE - timedelta(days=1)):
        for state in states:
            rows.append({
                "date":  single_date.strftime("%Y-%m-%d"),
                "state": state,
                "temp_high_f": round(random.gauss(70, 20), 1),
                "temp_low_f":  round(random.gauss(50, 20), 1),
                "precipitation_in": round(max(0, random.gauss(0.1, 0.3)), 2),
                "snow_in": round(max(0, random.gauss(0, 0.5)), 2) if single_date.month in [12,1,2] else 0.0,
                "condition": random.choice(["sunny","cloudy","rainy","snowy","partly_cloudy"])
            })

    df = pd.DataFrame(rows)
    out_path = os.path.join(OUTPUT_DIR, "weather.csv")
    df.to_csv(out_path, index=False)
    print(f"  ✓ Weather data saved → {out_path}  ({len(df):,} rows)")


# ─── GENERATE HOLIDAYS DATA ───────────────────────────────────────────────────
def generate_holidays():
    """Generates US public/retail holidays for the pipeline period."""
    print("Generating holidays data...")
    holidays = [
        {"date": "2023-01-01", "holiday_name": "New Year's Day",         "is_federal": True},
        {"date": "2023-01-16", "holiday_name": "Martin Luther King Day",  "is_federal": True},
        {"date": "2023-02-14", "holiday_name": "Valentine's Day",         "is_federal": False},
        {"date": "2023-02-20", "holiday_name": "Presidents' Day",         "is_federal": True},
        {"date": "2023-03-17", "holiday_name": "St. Patrick's Day",       "is_federal": False},
        {"date": "2023-05-14", "holiday_name": "Mother's Day",            "is_federal": False},
        {"date": "2023-05-29", "holiday_name": "Memorial Day",            "is_federal": True},
        {"date": "2023-06-18", "holiday_name": "Father's Day",            "is_federal": False},
        {"date": "2023-07-04", "holiday_name": "Independence Day",        "is_federal": True},
        {"date": "2023-09-04", "holiday_name": "Labor Day",               "is_federal": True},
        {"date": "2023-10-09", "holiday_name": "Columbus Day",            "is_federal": True},
        {"date": "2023-10-31", "holiday_name": "Halloween",               "is_federal": False},
        {"date": "2023-11-11", "holiday_name": "Veterans Day",            "is_federal": True},
        {"date": "2023-11-23", "holiday_name": "Thanksgiving",            "is_federal": True},
        {"date": "2023-11-24", "holiday_name": "Black Friday",            "is_federal": False},
        {"date": "2023-11-27", "holiday_name": "Cyber Monday",            "is_federal": False},
        {"date": "2023-12-24", "holiday_name": "Christmas Eve",           "is_federal": False},
        {"date": "2023-12-25", "holiday_name": "Christmas Day",           "is_federal": True},
        {"date": "2023-12-31", "holiday_name": "New Year's Eve",          "is_federal": False},
    ]
    df = pd.DataFrame(holidays)
    out_path = os.path.join(OUTPUT_DIR, "holidays.csv")
    df.to_csv(out_path, index=False)
    print(f"  ✓ Holidays saved → {out_path}  ({len(df)} rows)")


# ─── MAIN ─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 60)
    print("  Retail Demand Pipeline — Sample Data Generator")
    print("=" * 60)
    generate_pos_sales()
    generate_ecommerce_events()
    generate_weather()
    generate_holidays()
    print("\n✅ All sample data generated successfully!")
    print(f"   Output directory: {os.path.abspath(OUTPUT_DIR)}")
