"""
Create BigQuery views optimized for Looker Studio dashboards.
"""

import os
import yaml
from google.cloud import bigquery

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def load_config():
    config_path = os.path.join(BASE_DIR, "config", "config.yaml")
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def run_query(client, sql, description):
    print(f"{description}...")
    client.query(sql).result()
    print("  Done.")


def main():
    config = load_config()
    project_id = config["project"]["gcp_project_id"]
    dataset = config["bigquery"]["dataset"]
    t = f"{project_id}.{dataset}"

    client = bigquery.Client(project=project_id)

    # View 1: Fraud Overview (KPIs and summary)
    run_query(client, f"""
    CREATE OR REPLACE VIEW `{t}.v_fraud_overview` AS
    SELECT
        COUNT(*) AS total_transactions,
        COUNTIF(actual_fraud = 1) AS total_frauds,
        COUNTIF(predicted_fraud = 1) AS total_flagged,
        ROUND(COUNTIF(actual_fraud = 1) * 100.0 / COUNT(*), 2) AS fraud_rate_pct,
        ROUND(SUM(CASE WHEN actual_fraud = 1 THEN amount ELSE 0 END), 2) AS fraud_amount,
        ROUND(SUM(amount), 2) AS total_amount,
        COUNTIF(actual_fraud = 1 AND predicted_fraud = 1) AS true_positives,
        COUNTIF(actual_fraud = 0 AND predicted_fraud = 1) AS false_positives,
        COUNTIF(actual_fraud = 1 AND predicted_fraud = 0) AS missed_frauds
    FROM `{t}.fraud_predictions`
    """, "Creating fraud overview view")

    # View 2: Fraud by Country (geo heatmap)
    run_query(client, f"""
    CREATE OR REPLACE VIEW `{t}.v_fraud_by_country` AS
    SELECT
        country,
        COUNT(*) AS total_transactions,
        COUNTIF(actual_fraud = 1) AS fraud_count,
        ROUND(COUNTIF(actual_fraud = 1) * 100.0 / COUNT(*), 2) AS fraud_rate_pct,
        ROUND(AVG(fraud_probability), 4) AS avg_fraud_probability,
        ROUND(SUM(CASE WHEN actual_fraud = 1 THEN amount ELSE 0 END), 2) AS fraud_amount
    FROM `{t}.fraud_predictions`
    GROUP BY country
    ORDER BY fraud_rate_pct DESC
    """, "Creating fraud by country view")

    # View 3: Fraud by Merchant Category
    run_query(client, f"""
    CREATE OR REPLACE VIEW `{t}.v_fraud_by_category` AS
    SELECT
        merchant_category,
        COUNT(*) AS total_transactions,
        COUNTIF(actual_fraud = 1) AS fraud_count,
        ROUND(COUNTIF(actual_fraud = 1) * 100.0 / COUNT(*), 2) AS fraud_rate_pct,
        ROUND(AVG(amount), 2) AS avg_amount,
        ROUND(AVG(fraud_probability), 4) AS avg_fraud_probability
    FROM `{t}.fraud_predictions`
    GROUP BY merchant_category
    ORDER BY fraud_rate_pct DESC
    """, "Creating fraud by merchant category view")

    # View 4: Risk Flag Distribution
    run_query(client, f"""
    CREATE OR REPLACE VIEW `{t}.v_risk_distribution` AS
    SELECT
        risk_flag_count,
        COUNT(*) AS transaction_count,
        COUNTIF(actual_fraud = 1) AS fraud_count,
        ROUND(COUNTIF(actual_fraud = 1) * 100.0 / COUNT(*), 2) AS fraud_rate_pct,
        COUNTIF(predicted_fraud = 1) AS predicted_fraud_count,
        ROUND(AVG(fraud_probability), 4) AS avg_fraud_probability
    FROM `{t}.fraud_predictions`
    GROUP BY risk_flag_count
    ORDER BY risk_flag_count DESC
    """, "Creating risk distribution view")

    # View 5: Monthly Fraud Trends
    run_query(client, f"""
    CREATE OR REPLACE VIEW `{t}.v_fraud_trends` AS
    SELECT
        e.transaction_id,
        DATE_TRUNC(e.timestamp, MONTH) AS month,
        e.amount,
        e.channel,
        e.country,
        e.merchant_category,
        e.is_international,
        e.is_high_risk_country,
        e.is_high_risk_merchant,
        e.risk_flag_count,
        p.actual_fraud,
        p.predicted_fraud,
        p.fraud_probability
    FROM `{t}.transactions_enriched` e
    JOIN `{t}.fraud_predictions` p
        ON e.transaction_id = p.transaction_id
    """, "Creating fraud trends view")

    print()
    print("All dashboard views created:")
    print("  - v_fraud_overview        (KPI scorecards)")
    print("  - v_fraud_by_country      (geo heatmap)")
    print("  - v_fraud_by_category     (bar chart)")
    print("  - v_risk_distribution     (risk breakdown)")
    print("  - v_fraud_trends          (time series + detail)")


if __name__ == "__main__":
    main()