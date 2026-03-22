import logging
import sys
import os

# Set working directory to this script's folder so relative paths in config work from anywhere
PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
os.chdir(PROJECT_DIR)
sys.path.insert(0, PROJECT_DIR)

from src.db_loader import load_config, seed_database
from src.data_ingestion import ingest_all_sources
from src.screening import screen_all_customers
from src.reconciliation import reconcile
from src.report_generator import generate_report
from src.s3_uploader import upload_to_s3

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)


def main():
    logger.info("=== Strategic Recon Pipeline Started ===")

    # Step 1: Load configuration
    config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config", "config.yaml")
    config = load_config(config_path)
    logger.info("Configuration loaded")

    # Step 2: Seed database from sample CSVs
    logger.info("Seeding database from sample data...")
    seed_database(config)


    # Step 3: Ingest and normalize data from all source systems
    logger.info("Ingesting data from all source systems...")
    customers_df = ingest_all_sources(config)

    # Step 4: Screen customers against World Check
    logger.info("Screening customers against World Check...")
    screened_df = screen_all_customers(config, customers_df)

    # Step 5: Reconcile results
    logger.info("Reconciling screening results...")
    reconciled_df = reconcile(config, screened_df)

    # Step 6: Generate CSV report
    logger.info("Generating reconciliation report...")
    report_path = generate_report(config, reconciled_df)

    # Step 7: Upload to S3
    logger.info("Uploading report to S3...")
    s3_uri = upload_to_s3(config, report_path)

    if s3_uri:
        logger.info(f"Report available at: {s3_uri}")
    else:
        logger.info(f"Report available locally at: {report_path}")

    logger.info("=== Strategic Recon Pipeline Complete ===")


if __name__ == "__main__":
    main()
