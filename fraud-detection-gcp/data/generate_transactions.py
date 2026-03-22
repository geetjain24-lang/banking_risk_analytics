"""
Synthetic Transaction Data Generator for Fraud Detection Pipeline.

Generates realistic credit card transaction data with embedded fraud patterns:
- Normal transactions: typical amounts, local merchants, business hours
- Fraudulent transactions: unusual amounts, foreign merchants, odd hours, velocity spikes
"""

import csv
import random
import os
from datetime import datetime, timedelta

random.seed(42)

# --- Configuration ---
NUM_TRANSACTIONS = 500_000
FRAUD_RATE = 0.035
OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_DIR = os.path.join(OUTPUT_DIR, "raw")

# --- Reference Data ---
COUNTRIES = ["US", "US", "US", "US", "UK", "UK", "CA", "DE", "IN", "BR"]  # weighted toward US
HIGH_RISK_COUNTRIES = ["NG", "RU", "CN", "PH", "UA", "RO"]

MERCHANT_CATEGORIES = [
    "grocery", "gas_station", "restaurant", "online_retail",
    "electronics", "travel", "healthcare", "entertainment",
    "clothing", "utilities"
]
HIGH_RISK_CATEGORIES = ["online_gambling", "crypto_exchange", "wire_transfer", "jewelry"]

CHANNELS = ["chip", "contactless", "online", "swipe"]

CITIES = {
    "US": ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Dallas"],
    "UK": ["London", "Manchester", "Birmingham"],
    "CA": ["Toronto", "Vancouver", "Montreal"],
    "DE": ["Berlin", "Munich", "Frankfurt"],
    "IN": ["Mumbai", "Delhi", "Bangalore"],
    "BR": ["Sao Paulo", "Rio de Janeiro"],
    "NG": ["Lagos", "Abuja"],
    "RU": ["Moscow", "St Petersburg"],
    "CN": ["Shanghai", "Beijing"],
    "PH": ["Manila", "Cebu"],
    "UA": ["Kyiv", "Odessa"],
    "RO": ["Bucharest", "Cluj"],
}


def generate_customer_id():
    return random.randint(1000, 50000)


def generate_normal_transaction(txn_id, timestamp):
    """Generate a legitimate transaction with typical patterns."""
    country = random.choice(COUNTRIES)
    city = random.choice(CITIES[country])
    category = random.choice(MERCHANT_CATEGORIES)

    # Normal amounts: mostly small, occasionally medium
    if category == "grocery":
        amount = round(random.uniform(5, 150), 2)
    elif category == "gas_station":
        amount = round(random.uniform(15, 80), 2)
    elif category == "restaurant":
        amount = round(random.uniform(8, 120), 2)
    elif category == "travel":
        amount = round(random.uniform(50, 2000), 2)
    elif category == "electronics":
        amount = round(random.uniform(20, 1500), 2)
    else:
        amount = round(random.uniform(5, 300), 2)

    return {
        "transaction_id": f"TXN-{txn_id:07d}",
        "customer_id": generate_customer_id(),
        "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
        "amount": amount,
        "currency": "USD",
        "merchant_category": category,
        "merchant_id": f"MER-{random.randint(1, 5000):05d}",
        "channel": random.choice(CHANNELS),
        "country": country,
        "city": city,
        "is_international": country != "US",
        "is_fraud": 0,
    }


def generate_fraudulent_transaction(txn_id, timestamp):
    """Generate a fraudulent transaction with suspicious patterns."""
    fraud_type = random.choice(["high_amount", "foreign", "odd_hours", "rapid_fire", "category"])

    if fraud_type == "high_amount":
        country = random.choice(COUNTRIES)
        city = random.choice(CITIES[country])
        category = random.choice(MERCHANT_CATEGORIES)
        amount = round(random.uniform(2000, 25000), 2)
        channel = random.choice(["online", "swipe"])

    elif fraud_type == "foreign":
        country = random.choice(HIGH_RISK_COUNTRIES)
        city = random.choice(CITIES[country])
        category = random.choice(MERCHANT_CATEGORIES + HIGH_RISK_CATEGORIES)
        amount = round(random.uniform(100, 8000), 2)
        channel = random.choice(["online", "swipe"])

    elif fraud_type == "odd_hours":
        # Override timestamp to be between 1 AM - 5 AM
        timestamp = timestamp.replace(hour=random.randint(1, 4), minute=random.randint(0, 59))
        country = random.choice(COUNTRIES)
        city = random.choice(CITIES[country])
        category = random.choice(MERCHANT_CATEGORIES)
        amount = round(random.uniform(200, 5000), 2)
        channel = "online"

    elif fraud_type == "rapid_fire":
        country = random.choice(COUNTRIES)
        city = random.choice(CITIES[country])
        category = "online_retail"
        amount = round(random.uniform(50, 500), 2)
        channel = "online"

    else:  # high-risk category
        country = random.choice(COUNTRIES + HIGH_RISK_COUNTRIES)
        city = random.choice(CITIES[country])
        category = random.choice(HIGH_RISK_CATEGORIES)
        amount = round(random.uniform(500, 15000), 2)
        channel = "online"

    return {
        "transaction_id": f"TXN-{txn_id:07d}",
        "customer_id": generate_customer_id(),
        "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
        "amount": amount,
        "currency": "USD",
        "merchant_category": category,
        "merchant_id": f"MER-{random.randint(1, 5000):05d}",
        "channel": channel,
        "country": country,
        "city": city,
        "is_international": country != "US",
        "is_fraud": 1,
    }


def generate_merchant_data():
    """Generate merchant enrichment data."""
    merchants = []
    all_categories = MERCHANT_CATEGORIES + HIGH_RISK_CATEGORIES
    for i in range(1, 5001):
        category = random.choice(all_categories)
        country = random.choice(COUNTRIES + HIGH_RISK_COUNTRIES)
        city = random.choice(CITIES[country])
        merchants.append({
            "merchant_id": f"MER-{i:05d}",
            "merchant_name": f"Merchant_{i}",
            "category": category,
            "country": country,
            "city": city,
            "risk_tier": "high" if category in HIGH_RISK_CATEGORIES or country in HIGH_RISK_COUNTRIES else "low",
            "registered_since": f"{random.randint(2010, 2024)}-{random.randint(1,12):02d}-01",
        })
    return merchants


def random_timestamp(start_date, end_date):
    delta = end_date - start_date
    random_seconds = random.randint(0, int(delta.total_seconds()))
    return start_date + timedelta(seconds=random_seconds)


def main():
    os.makedirs(RAW_DIR, exist_ok=True)

    start_date = datetime(2024, 1, 1)
    end_date = datetime(2026, 3, 1)
    num_fraud = int(NUM_TRANSACTIONS * FRAUD_RATE)
    num_normal = NUM_TRANSACTIONS - num_fraud

    print(f"Generating {NUM_TRANSACTIONS:,} transactions ({num_fraud:,} fraudulent, {num_normal:,} normal)...")

    # -- Generate transactions --
    fieldnames = [
        "transaction_id", "customer_id", "timestamp", "amount", "currency",
        "merchant_category", "merchant_id", "channel", "country", "city",
        "is_international", "is_fraud",
    ]

    txn_file = os.path.join(RAW_DIR, "transactions.csv")
    with open(txn_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        txn_id = 1
        for _ in range(num_normal):
            ts = random_timestamp(start_date, end_date)
            writer.writerow(generate_normal_transaction(txn_id, ts))
            txn_id += 1

        for _ in range(num_fraud):
            ts = random_timestamp(start_date, end_date)
            writer.writerow(generate_fraudulent_transaction(txn_id, ts))
            txn_id += 1

    print(f"  Transactions saved: {txn_file}")

    # -- Generate merchant enrichment data --
    merchants = generate_merchant_data()
    merchant_file = os.path.join(RAW_DIR, "merchants.csv")
    merchant_fields = ["merchant_id", "merchant_name", "category", "country", "city", "risk_tier", "registered_since"]

    with open(merchant_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=merchant_fields)
        writer.writeheader()
        writer.writerows(merchants)

    print(f"  Merchants saved:    {merchant_file}")
    print("Done.")


if __name__ == "__main__":
    main()
