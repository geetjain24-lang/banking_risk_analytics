"""
Run fraud scoring SQL on BigQuery and save results to a fraud_scores table.
"""

import os
import yaml
from google.cloud import bigquery

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def load_config():
    config_path = os.path.join(BASE_DIR, "config", "config.yaml")
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def main():
    config = load_config()
    project_id = config["project"]["gcp_project_id"]
    dataset = config["bigquery"]["dataset"]

    client = bigquery.Client(project=project_id)

    # Read SQL template and fill in project/dataset references
    sql_path = os.path.join(BASE_DIR, "sql", "fraud_scoring.sql")
    with open(sql_path, "r") as f:
        sql = f.read()

    sql = sql.replace("{project_id}", project_id).replace("{dataset}", dataset)

    # Save results into fraud_scores table
    destination_table = f"{project_id}.{dataset}.fraud_scores"

    job_config = bigquery.QueryJobConfig(
        destination=destination_table,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    print("Running fraud scoring query...")
    query_job = client.query(sql, job_config=job_config)
    results = query_job.result()  # wait for completion

    print(f"Scored {results.total_rows:,} transactions -> {destination_table}")

    # Quick summary
    summary_sql = f"""
    SELECT
        risk_category,
        COUNT(*) AS transaction_count,
        ROUND(AVG(fraud_score), 1) AS avg_score,
        COUNTIF(actual_fraud_label = 1) AS actual_frauds
    FROM `{destination_table}`
    GROUP BY risk_category
    ORDER BY avg_score DESC
    """
    print("\n--- Risk Distribution ---")
    for row in client.query(summary_sql).result():
        print(f"  {row.risk_category:10s} | {row.transaction_count:>7,} txns | avg score: {row.avg_score:>5} | actual frauds: {row.actual_frauds:>5,}")

    print("\nScoring complete.")


if __name__ == "__main__":
    main()
