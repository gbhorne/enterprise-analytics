#!/usr/bin/env python3
"""
Generate realistic retail sample data for the analytics platform.

Produces CSV files that can be loaded into BigQuery bronze layer tables.
Supports three scale levels:
  - small:  100K transactions, 5K customers, 500 products, 50 stores
  - medium: 1M transactions, 50K customers, 2K products, 200 stores
  - large:  10M transactions, 500K customers, 10K products, 1200 stores

Usage:
  python generate_retail_data.py --scale small --output-dir ./data
"""

import argparse
import csv
import os
import random
import uuid
from datetime import datetime, timedelta, date
from pathlib import Path

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SCALES = {
    "small":  {"transactions": 100_000, "customers": 5_000,   "products": 500,   "stores": 50},
    "medium": {"transactions": 1_000_000, "customers": 50_000, "products": 2_000, "stores": 200},
    "large":  {"transactions": 10_000_000, "customers": 500_000, "products": 10_000, "stores": 1_200},
}

REGIONS = {
    "AMERICAS": {"countries": ["US", "CA", "MX", "BR"], "cities": {
        "US": ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia"],
        "CA": ["Toronto", "Vancouver", "Montreal", "Calgary"],
        "MX": ["Mexico City", "Guadalajara", "Monterrey"],
        "BR": ["Sao Paulo", "Rio de Janeiro", "Brasilia"],
    }, "tz": "America/Chicago"},
    "EMEA": {"countries": ["GB", "DE", "FR", "NL", "ES"], "cities": {
        "GB": ["London", "Manchester", "Birmingham", "Edinburgh"],
        "DE": ["Berlin", "Munich", "Hamburg", "Frankfurt"],
        "FR": ["Paris", "Lyon", "Marseille"],
        "NL": ["Amsterdam", "Rotterdam"],
        "ES": ["Madrid", "Barcelona"],
    }, "tz": "Europe/London"},
    "APAC": {"countries": ["AU", "JP", "SG", "IN"], "cities": {
        "AU": ["Sydney", "Melbourne", "Brisbane", "Perth"],
        "JP": ["Tokyo", "Osaka", "Yokohama"],
        "SG": ["Singapore"],
        "IN": ["Mumbai", "Delhi", "Bangalore"],
    }, "tz": "Australia/Sydney"},
}

CATEGORIES = {
    "electronics": {
        "sub": ["smartphones", "laptops", "tablets", "accessories", "audio"],
        "brands": ["TechPro", "NovaTech", "DigitalEdge", "SmartCore", "PrimeElec"],
        "price_range": (29.99, 2499.99), "cost_ratio": (0.45, 0.65),
    },
    "clothing": {
        "sub": ["mens", "womens", "kids", "shoes", "accessories"],
        "brands": ["UrbanWear", "ClassicFit", "SportLine", "EcoThread", "LuxLabel"],
        "price_range": (9.99, 499.99), "cost_ratio": (0.25, 0.50),
    },
    "home": {
        "sub": ["furniture", "kitchen", "bedding", "decor", "garden"],
        "brands": ["HomeEssentials", "ModernLiving", "CozyNest", "GreenHome", "DesignCraft"],
        "price_range": (4.99, 1999.99), "cost_ratio": (0.35, 0.60),
    },
    "grocery": {
        "sub": ["fresh", "pantry", "beverages", "snacks", "frozen"],
        "brands": ["FarmFresh", "OrganicChoice", "DailyBasics", "GourmetSelect", "NaturePure"],
        "price_range": (0.99, 79.99), "cost_ratio": (0.55, 0.80),
    },
    "beauty": {
        "sub": ["skincare", "makeup", "haircare", "fragrance", "wellness"],
        "brands": ["GlowUp", "PureSkin", "LuxeBeauty", "NaturalGlow", "VitalCare"],
        "price_range": (4.99, 299.99), "cost_ratio": (0.15, 0.40),
    },
}

PAYMENT_METHODS = ["credit", "debit", "cash", "mobile", "gift_card"]
CHANNELS = ["in_store", "online", "mobile_app"]
STORE_TYPES = ["flagship", "standard", "outlet", "warehouse"]
LOYALTY_TIERS = ["bronze", "silver", "gold", "platinum", None]
GENDERS = ["male", "female", "non_binary", "prefer_not_to_say", None]

FIRST_NAMES = [
    "James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda",
    "William", "Elizabeth", "David", "Barbara", "Richard", "Susan", "Joseph", "Jessica",
    "Thomas", "Sarah", "Charles", "Karen", "Kenji", "Yuki", "Hans", "Marie",
    "Carlos", "Ana", "Wei", "Li", "Raj", "Priya", "Oliver", "Emma", "Liam", "Sophia",
]
LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
    "Rodriguez", "Martinez", "Anderson", "Taylor", "Thomas", "Hernandez", "Moore",
    "Tanaka", "Mueller", "Dubois", "Santos", "Kumar", "Chen", "Kim", "Patel", "Singh",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def random_date(start: date, end: date) -> date:
    delta = (end - start).days
    if delta <= 0:
        return start
    return start + timedelta(days=random.randint(0, delta))


def random_timestamp(d: date) -> str:
    hour = random.choices(range(24), weights=[
        1, 1, 1, 1, 1, 2, 3, 5, 8, 10, 12, 12,
        10, 9, 8, 8, 9, 11, 12, 10, 7, 4, 2, 1,
    ])[0]
    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    return f"{d}T{hour:02d}:{minute:02d}:{second:02d}Z"


def generate_email(first, last):
    domains = ["gmail.com", "yahoo.com", "outlook.com", "email.com", "icloud.com"]
    sep = random.choice([".", "_", ""])
    num = random.choice(["", str(random.randint(1, 99))])
    return f"{first.lower()}{sep}{last.lower()}{num}@{random.choice(domains)}"


def generate_phone(country):
    if country == "US":
        return f"+1{random.randint(200, 999)}{random.randint(1000000, 9999999)}"
    elif country == "GB":
        return f"+44{random.randint(7000, 7999)}{random.randint(100000, 999999)}"
    else:
        return f"+{random.randint(1, 99)}{random.randint(1000000000, 9999999999)}"


def write_csv(data: list[dict], filepath: str):
    """Write list of dicts to CSV."""
    if not data:
        return
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)
    size_mb = os.path.getsize(filepath) / (1024 * 1024)
    print(f"  ✓ Wrote {len(data):,} rows → {filepath} ({size_mb:.1f} MB)")


# ---------------------------------------------------------------------------
# Generators
# ---------------------------------------------------------------------------

def generate_stores(n: int) -> list[dict]:
    stores = []
    store_id = 1
    region_weights = {"AMERICAS": 0.4, "EMEA": 0.35, "APAC": 0.25}

    for region_name, region_data in REGIONS.items():
        region_count = max(1, int(n * region_weights[region_name]))
        for _ in range(region_count):
            if store_id > n:
                break
            country = random.choice(region_data["countries"])
            city = random.choice(region_data["cities"][country])
            stores.append({
                "store_id": f"STORE-{store_id:04d}",
                "store_name": f"{city} {random.choice(['Main', 'Central', 'Park', 'Market', 'Plaza'])} Store",
                "store_type": random.choice(STORE_TYPES),
                "address": f"{random.randint(1, 9999)} {random.choice(['Main', 'Oak', 'Market', 'High', 'King'])} Street",
                "city": city,
                "state_province": "",
                "country_code": country,
                "region": region_name,
                "timezone": region_data["tz"],
                "square_footage": random.randint(5000, 80000),
                "open_date": str(random_date(date(2010, 1, 1), date(2023, 12, 31))),
                "close_date": "",
                "is_active": True,
                "manager_name": f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}",
                "_ingested_at": datetime.utcnow().isoformat() + "Z",
                "_source_system": "store-ops",
            })
            store_id += 1
    return stores[:n]


def generate_products(n: int) -> list[dict]:
    products = []
    product_id = 1
    per_cat = n // len(CATEGORIES)

    for cat_name, cat_data in CATEGORIES.items():
        for _ in range(per_cat):
            if product_id > n:
                break
            sub = random.choice(cat_data["sub"])
            brand = random.choice(cat_data["brands"])
            price = round(random.uniform(*cat_data["price_range"]), 2)
            cost_ratio = random.uniform(*cat_data["cost_ratio"])
            products.append({
                "product_id": f"SKU-{product_id:06d}",
                "product_name": f"{brand} {sub.title()} {random.choice(['Pro', 'Plus', 'Essential', 'Classic', 'Deluxe'])} {random.randint(100, 999)}",
                "category_l1": cat_name,
                "category_l2": sub,
                "category_l3": random.choice(["basic", "premium", "limited_edition"]),
                "brand": brand,
                "supplier_id": f"SUP-{random.randint(1, 200):04d}",
                "unit_cost": round(price * cost_ratio, 2),
                "list_price": price,
                "weight_kg": round(random.uniform(0.1, 25.0), 2),
                "is_active": random.random() > 0.05,
                "launch_date": str(random_date(date(2018, 1, 1), date(2024, 6, 1))),
                "discontinue_date": "",
                "_ingested_at": datetime.utcnow().isoformat() + "Z",
                "_source_system": "product-catalog",
            })
            product_id += 1
    return products[:n]


def generate_customers(n: int) -> list[dict]:
    customers = []
    for i in range(1, n + 1):
        country = random.choice(["US", "CA", "GB", "DE", "FR", "AU", "JP", "SG", "BR", "IN"])
        first = random.choice(FIRST_NAMES)
        last = random.choice(LAST_NAMES)
        signup = random_date(date(2018, 1, 1), date(2024, 12, 31))
        last_activity = random_date(signup, date(2025, 2, 15))
        loyalty = random.choice(LOYALTY_TIERS)
        gender = random.choice(GENDERS)
        customers.append({
            "customer_id": f"CUST-{i:07d}",
            "first_name": first,
            "last_name": last,
            "email": generate_email(first, last),
            "phone": generate_phone(country),
            "date_of_birth": str(random_date(date(1950, 1, 1), date(2006, 1, 1))),
            "gender": gender if gender else "",
            "address_line1": f"{random.randint(1, 9999)} {random.choice(['Oak', 'Elm', 'Main', 'Park'])} Ave",
            "city": random.choice(["Springfield", "Portland", "London", "Sydney", "Berlin", "Tokyo"]),
            "state_province": "",
            "postal_code": f"{random.randint(10000, 99999)}",
            "country_code": country,
            "loyalty_tier": loyalty if loyalty else "",
            "signup_date": str(signup),
            "last_activity_date": str(last_activity),
            "is_active": random.random() > 0.1,
            "marketing_opt_in": random.random() > 0.3,
            "_ingested_at": datetime.utcnow().isoformat() + "Z",
            "_source_system": "crm",
        })
    return customers


def generate_transactions(n: int, customers: list, products: list, stores: list) -> list[dict]:
    """Generate transactions with realistic patterns."""
    transactions = []
    customer_ids = [c["customer_id"] for c in customers]
    start = date(2024, 1, 1)
    end = date(2025, 2, 15)
    total_days = (end - start).days

    print(f"  Generating {n:,} transactions...")
    for i in range(1, n + 1):
        # Weighted toward recent dates + holiday spikes
        day_offset = int(random.triangular(0, total_days, total_days * 0.85))
        txn_date = start + timedelta(days=day_offset)

        # More items during holidays
        if txn_date.month in (11, 12):
            items_in_txn = random.choices([1, 2, 3, 4, 5], weights=[30, 30, 20, 12, 8])[0]
        else:
            items_in_txn = random.choices([1, 2, 3, 4, 5], weights=[45, 30, 15, 7, 3])[0]

        txn_id = f"TXN-{uuid.uuid4().hex[:12].upper()}"
        store = random.choice(stores)
        customer_id = random.choice(customer_ids) if random.random() > 0.15 else ""
        channel = random.choices(CHANNELS, weights=[55, 30, 15])[0]
        payment = random.choice(PAYMENT_METHODS)

        for _ in range(items_in_txn):
            product = random.choice(products)
            qty = random.choices([1, 2, 3, 5, 10], weights=[60, 20, 10, 7, 3])[0]
            unit_price = float(product["list_price"])
            discount = round(unit_price * qty * random.choice([0, 0, 0, 0.05, 0.10, 0.15, 0.20, 0.25]), 2)
            subtotal = round(unit_price * qty - discount, 2)
            tax = round(subtotal * random.uniform(0.05, 0.12), 2)
            total = round(subtotal + tax, 2)

            transactions.append({
                "transaction_id": txn_id,
                "transaction_date": str(txn_date),
                "transaction_timestamp": random_timestamp(txn_date),
                "store_id": store["store_id"],
                "customer_id": customer_id,
                "product_id": product["product_id"],
                "quantity": qty,
                "unit_price": unit_price,
                "discount_amount": discount,
                "tax_amount": tax,
                "total_amount": total,
                "payment_method": payment,
                "channel": channel,
                "currency_code": "USD",
                "_ingested_at": datetime.utcnow().isoformat() + "Z",
                "_source_system": "pos-v3",
                "_source_file": "",
            })

        if i % 50_000 == 0:
            print(f"    {i:,} / {n:,} transactions generated...")

    return transactions


def generate_inventory(products: list, stores: list, days: int = 30) -> list[dict]:
    """Generate daily inventory snapshots for the last N days."""
    inventory = []
    today = date(2025, 2, 15)

    sample_products = random.sample(products, min(len(products), 200))
    sample_stores = random.sample(stores, min(len(stores), 30))

    print(f"  Generating inventory for {len(sample_stores)} stores × {len(sample_products)} products × {days} days...")

    for day_offset in range(days):
        snap_date = today - timedelta(days=day_offset)
        for store in sample_stores:
            for product in sample_products:
                stock = max(0, int(random.gauss(50, 30)))
                reorder = random.randint(10, 30)
                last_sold = str(random_date(snap_date - timedelta(days=7), snap_date)) if stock < 80 else ""
                inventory.append({
                    "snapshot_date": str(snap_date),
                    "store_id": store["store_id"],
                    "product_id": product["product_id"],
                    "stock_on_hand": stock,
                    "stock_on_order": random.randint(0, 100) if stock < reorder else 0,
                    "reorder_point": reorder,
                    "last_received_date": str(random_date(snap_date - timedelta(days=14), snap_date)),
                    "last_sold_date": last_sold,
                    "_ingested_at": datetime.utcnow().isoformat() + "Z",
                    "_source_system": "inventory-system",
                })
    return inventory


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Generate retail sample data")
    parser.add_argument("--scale", choices=["small", "medium", "large"], default="small",
                        help="Dataset scale (default: small)")
    parser.add_argument("--output-dir", default="./data",
                        help="Output directory for CSV files (default: ./data)")
    parser.add_argument("--seed", type=int, default=42,
                        help="Random seed for reproducibility (default: 42)")
    args = parser.parse_args()

    random.seed(args.seed)
    cfg = SCALES[args.scale]
    out = Path(args.output_dir)

    print(f"\n{'=' * 60}")
    print(f"  Retail Data Generator — {args.scale.upper()} scale")
    print(f"{'=' * 60}")
    print(f"  Transactions : {cfg['transactions']:>12,}")
    print(f"  Customers    : {cfg['customers']:>12,}")
    print(f"  Products     : {cfg['products']:>12,}")
    print(f"  Stores       : {cfg['stores']:>12,}")
    print(f"  Output       : {out.resolve()}")
    print(f"  Seed         : {args.seed}")
    print(f"{'=' * 60}\n")

    # --- Stores ---
    print("[1/5] Generating stores...")
    stores = generate_stores(cfg["stores"])
    write_csv(stores, str(out / "raw_stores.csv"))

    # --- Products ---
    print("[2/5] Generating products...")
    products = generate_products(cfg["products"])
    write_csv(products, str(out / "raw_products.csv"))

    # --- Customers ---
    print("[3/5] Generating customers...")
    customers = generate_customers(cfg["customers"])
    write_csv(customers, str(out / "raw_customers.csv"))

    # --- Transactions ---
    print("[4/5] Generating transactions...")
    transactions = generate_transactions(cfg["transactions"], customers, products, stores)
    write_csv(transactions, str(out / "raw_transactions.csv"))

    # --- Inventory ---
    print("[5/5] Generating inventory snapshots...")
    inventory = generate_inventory(products, stores, days=30)
    write_csv(inventory, str(out / "raw_inventory.csv"))

    # --- Summary ---
    print(f"\n{'=' * 60}")
    print(f"  ✅ Data generation complete!")
    print(f"{'=' * 60}")
    total_size = sum(
        os.path.getsize(str(out / f))
        for f in ["raw_stores.csv", "raw_products.csv", "raw_customers.csv",
                   "raw_transactions.csv", "raw_inventory.csv"]
    ) / (1024 * 1024)
    print(f"  Total size: {total_size:.1f} MB")
    print(f"\n  Next steps:")
    print(f"  1. Load to BigQuery:  python load_bronze.py --data-dir {out}")
    print(f"  2. Run dbt:           cd ../dbt && dbt run")
    print(f"  3. Run tests:         cd ../dbt && dbt test")
    print()


if __name__ == "__main__":
    main()
