import logging

import pandas as pd
from fuzzywuzzy import fuzz

from src.db_loader import query_world_check

logger = logging.getLogger(__name__)


def screen_customer(customer_row, world_check_df, threshold):
    """Screen a single customer against all World Check entities.

    Returns the best match details (entity_id, entity_name, match_score, category)
    or None values if no match meets the threshold.
    """
    best_score = 0
    best_match = None

    customer_name = str(customer_row["full_name"]).lower().strip()
    customer_dob = str(customer_row["dob"]).strip()
    customer_nat = str(customer_row["nationality"]).upper().strip()

    for _, wc_row in world_check_df.iterrows():
        entity_name = str(wc_row["entity_name"]).lower().strip()

        # Fuzzy name matching
        name_score = fuzz.token_sort_ratio(customer_name, entity_name)

        # Boost score if DOB or nationality also match
        dob_match = str(wc_row["dob"]).strip() == customer_dob
        nat_match = str(wc_row["nationality"]).upper().strip() == customer_nat

        effective_score = name_score
        if dob_match and name_score >= (threshold - 15):
            effective_score = min(100, name_score + 5)
        if nat_match and name_score >= (threshold - 15):
            effective_score = min(100, name_score + 5)

        if effective_score > best_score:
            best_score = effective_score
            best_match = {
                "matched_entity_id": wc_row["entity_id"],
                "matched_entity_name": wc_row["entity_name"],
                "match_score": effective_score,
                "category": wc_row["category"],
                "dob_match": dob_match,
                "nationality_match": nat_match,
            }

    return best_match if best_score >= threshold else None


def screen_all_customers(config, customers_df):
    """Screen all customers against World Check data.

    Returns the customers DataFrame with screening result columns appended.
    """
    threshold = config["screening"]["fuzzy_match_threshold"]
    world_check_df = query_world_check(config)

    logger.info(
        f"Screening {len(customers_df)} customers against "
        f"{len(world_check_df)} World Check entities (threshold={threshold})"
    )

    results = []
    for idx, row in customers_df.iterrows():
        match = screen_customer(row, world_check_df, threshold)
        if match:
            results.append(match)
        else:
            results.append({
                "matched_entity_id": None,
                "matched_entity_name": None,
                "match_score": 0,
                "category": None,
                "dob_match": False,
                "nationality_match": False,
            })

    results_df = pd.DataFrame(results)
    screened_df = pd.concat([customers_df.reset_index(drop=True), results_df], axis=1)

    hits = screened_df[screened_df["matched_entity_id"].notna()]
    logger.info(f"Screening complete: {len(hits)} hits out of {len(screened_df)} customers")

    return screened_df
