"""
Create BigQuery dataset and load data from Cloud Storage.
"""

import os
import yaml
from google.cloud import bigquery

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def load_config():
    config_path = os.path.join(BASE_DIR, "config", "config.yaml")
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def create_dataset(client, dataset_id, region):
    """Create BigQuery dataset if it doesn't exist."""
    dataset_ref = bigquery.Dataset(dataset_id)
    dataset_ref.location = region
    try:
        client.get_dataset(dataset_id)
        print(f"Dataset '{dataset_id}' already exists.")
    except Exception:
        client.create_dataset(dataset_ref)
        print(f"Dataset '{dataset_id}' created.")


def load_csv_from_gcs(client, dataset_id, table_name, gcs_uri, schema):
    """Load a CSV file from GCS into a BigQuery table."""
    table_id = f"{dataset_id}.{table_name}"

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        skip_leading_rows=1,  # skip CSV header
        source_format=bigquery.SourceFormat.CSV,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # overwrite if exists
    )

    load_job = client.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
    print(f"Loading {table_name}...")
    load_job.result()  # wait for completion

    table = client.get_table(table_id)
    print(f"  Loaded {table.num_rows:,} rows into {table_id}")


def main():
    config = load_config()
    project_id = config["project"]["gcp_project_id"]
    region = config["project"]["region"]
    bucket_name = config["cloud_storage"]["bucket_name"]
    dataset_name = config["bigquery"]["dataset"]
    dataset_id = f"{project_id}.{dataset_name}"

    client = bigquery.Client(project=project_id)

    # Step 1: Create dataset
    create_dataset(client, dataset_id, region)

    # Step 2: Load transactions from GCS
    transactions_schema = [
        bigquery.SchemaField("transaction_id", "STRING"),
        bigquery.SchemaField("customer_id", "INTEGER"),
        bigquery.SchemaField("timestamp", "TIMESTAMP"),
        bigquery.SchemaField("amount", "FLOAT"),
        bigquery.SchemaField("currency", "STRING"),
        bigquery.SchemaField("merchant_category", "STRING"),
        bigquery.SchemaField("merchant_id", "STRING"),
        bigquery.SchemaField("channel", "STRING"),
        bigquery.SchemaField("country", "STRING"),
        bigquery.SchemaField("city", "STRING"),
        bigquery.SchemaField("is_international", "BOOLEAN"),
        bigquery.SchemaField("is_fraud", "INTEGER"),
    ]

    transactions_uri = f"gs://{bucket_name}/{config['cloud_storage']['raw_path']}transactions.csv"
    load_csv_from_gcs(client, dataset_id, "transactions", transactions_uri, transactions_schema)

    # Step 3: Load merchants from GCS
    merchants_schema = [
        bigquery.SchemaField("merchant_id", "STRING"),
        bigquery.SchemaField("merchant_name", "STRING"),
        bigquery.SchemaField("category", "STRING"),
        bigquery.SchemaField("country", "STRING"),
        bigquery.SchemaField("city", "STRING"),
        bigquery.SchemaField("risk_tier", "STRING"),
        bigquery.SchemaField("registered_since", "DATE"),
    ]

    merchants_uri = f"gs://{bucket_name}/{config['cloud_storage']['enrichment_path']}merchants.csv"
    load_csv_from_gcs(client, dataset_id, "merchants", merchants_uri, merchants_schema)

    print("\nBigQuery setup complete.")


if __name__ == "__main__":
    main()
