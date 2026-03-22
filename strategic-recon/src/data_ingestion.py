import logging

import pandas as pd

from src.db_loader import query_source_system

logger = logging.getLogger(__name__)

UNIFIED_COLUMNS = ["customer_id", "source_system", "full_name", "dob", "nationality", "address"]


def normalize_housing_loan(df):
    df = df.copy()
    df["source_system"] = "housing_loan"
    return df[["customer_id", "source_system", "full_name", "dob", "nationality", "address"]]


def normalize_student_loan(df):
    df = df.copy()
    df["source_system"] = "student_loan"
    df["address"] = df.get("university", "")
    return df[["customer_id", "source_system", "full_name", "dob", "nationality", "address"]]


def normalize_credit_card(df):
    df = df.copy()
    df["source_system"] = "credit_card"
    return df[["customer_id", "source_system", "full_name", "dob", "nationality", "address"]]


NORMALIZERS = {
    "housing_loan": normalize_housing_loan,
    "student_loan": normalize_student_loan,
    "credit_card": normalize_credit_card,
}


def ingest_all_sources(config):
    """Pull data from all source systems, normalize, and combine into one DataFrame."""
    frames = []

    for system_name in config["source_systems"]:
        df = query_source_system(config, system_name)
        normalizer = NORMALIZERS.get(system_name)
        if normalizer is None:
            logger.warning(f"No normalizer for source system: {system_name}")
            continue
        normalized = normalizer(df)
        frames.append(normalized)
        logger.info(f"Ingested {len(normalized)} records from {system_name}")

    combined = pd.concat(frames, ignore_index=True)
    combined["full_name"] = combined["full_name"].str.strip()
    combined["dob"] = combined["dob"].astype(str).str.strip()
    combined["nationality"] = combined["nationality"].str.strip().str.upper()

    logger.info(f"Total combined records: {len(combined)}")
    return combined
