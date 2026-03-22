import logging
from datetime import datetime, timezone

import pandas as pd

logger = logging.getLogger(__name__)


def reconcile(config, screened_df):
    """Classify each customer as HIT or NO HIT and add reconciliation metadata."""
    threshold = config["screening"]["fuzzy_match_threshold"]

    df = screened_df.copy()
    df["screening_status"] = df["match_score"].apply(
        lambda score: "HIT" if score >= threshold else "NO HIT"
    )
    df["recon_timestamp"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    _log_summary(df)

    return df


def _log_summary(df):
    total = len(df)
    hits = df[df["screening_status"] == "HIT"]
    no_hits = df[df["screening_status"] == "NO HIT"]

    logger.info("=" * 50)
    logger.info("RECONCILIATION SUMMARY")
    logger.info("=" * 50)
    logger.info(f"Total customers screened: {total}")
    logger.info(f"HITs: {len(hits)} ({len(hits)/total*100:.1f}%)")
    logger.info(f"NO HITs: {len(no_hits)} ({len(no_hits)/total*100:.1f}%)")

    if not hits.empty:
        logger.info("\nHITs by source system:")
        for source, group in hits.groupby("source_system"):
            logger.info(f"  {source}: {len(group)}")

        logger.info("\nHITs by category:")
        for cat, group in hits.groupby("category"):
            logger.info(f"  {cat}: {len(group)}")
