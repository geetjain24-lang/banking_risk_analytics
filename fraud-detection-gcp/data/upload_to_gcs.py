 """
Upload raw transaction and merchant data to Google Cloud Storage.
"""

import os
import yaml
from google.cloud import storage

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def load_config():
    config_path = os.path.join(BASE_DIR, "config", "config.yaml")
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def create_bucket_if_not_exists(client, bucket_name, region):
    """Create GCS bucket if it doesn't already exist."""
    bucket = client.bucket(bucket_name)
    if not bucket.exists():
        bucket = client.create_bucket(bucket_name, location=region)
        print(f"Bucket '{bucket_name}' created in {region}.")
    else:
        print(f"Bucket '{bucket_name}' already exists.")
    return bucket


def upload_file(bucket, local_path, gcs_path):
    """Upload a local file to GCS."""
    blob = bucket.blob(gcs_path)
    blob.upload_from_filename(local_path)
    print(f"  Uploaded: gs://{bucket.name}/{gcs_path} ({os.path.getsize(local_path) / (1024*1024):.1f} MB)")


def main():
    config = load_config()
    project_id = config["project"]["gcp_project_id"]
    region = config["project"]["region"]
    bucket_name = config["cloud_storage"]["bucket_name"]

    # Auto-generate bucket name if empty
    if not bucket_name:
        bucket_name = f"{project_id}-fraud-detection"
        print(f"Bucket name not set in config. Using: {bucket_name}")

    client = storage.Client(project=project_id)
    bucket = create_bucket_if_not_exists(client, bucket_name, region)

    raw_dir = os.path.join(BASE_DIR, "data", "raw")

    # Upload transactions
    upload_file(
        bucket,
        os.path.join(raw_dir, "transactions.csv"),
        config["cloud_storage"]["raw_path"] + "transactions.csv",
    )

    # Upload merchants
    upload_file(
        bucket,
        os.path.join(raw_dir, "merchants.csv"),
        config["cloud_storage"]["enrichment_path"] + "merchants.csv",
    )

    # Update config with bucket name
    config["cloud_storage"]["bucket_name"] = bucket_name
    config_path = os.path.join(BASE_DIR, "config", "config.yaml")
    with open(config_path, "w") as f:
        yaml.dump(config, f, default_flow_style=False)
    print(f"\nConfig updated with bucket_name: {bucket_name}")
    print("Done. Data is now in Cloud Storage.")


if __name__ == "__main__":
    main()