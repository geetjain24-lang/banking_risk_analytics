import logging
import os
from datetime import datetime, timezone

import pandas as pd

logger = logging.getLogger(__name__)

REPORT_COLUMNS = [
    "customer_id",
    "source_system",
    "full_name",
    "dob",
    "nationality",
    "screening_status",
    "matched_entity_name",
    "match_score",
    "category",
    "dob_match",
    "nationality_match",
    "recon_timestamp",
]


def generate_report(config, reconciled_df):
    """Generate a CSV reconciliation report and return the file path."""
    output_dir = config["output"]["directory"]
    os.makedirs(output_dir, exist_ok=True)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filename = f"recon_report_{timestamp}.csv"
    filepath = os.path.join(output_dir, filename)

    report_df = reconciled_df[[c for c in REPORT_COLUMNS if c in reconciled_df.columns]]
    report_df.to_csv(filepath, index=False)

    logger.info(f"Report generated: {filepath} ({len(report_df)} rows)")
    return filepath
