"""
Deploy the fraud detection pipeline to Google Cloud Dataflow.
Submits the pipeline to run on GCP infrastructure (Python 3.11).
"""

import os
import subprocess
import yaml

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def load_config():
    config_path = os.path.join(BASE_DIR, "config", "config.yaml")
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def main():
    config = load_config()
    project_id = config["project"]["gcp_project_id"]
    region = config["project"]["region"]
    bucket_name = config["cloud_storage"]["bucket_name"]
    dataset = config["bigquery"]["dataset"]

    pipeline_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "pipeline.py")

    # GCS paths
    txn_input = f"gs://{bucket_name}/{config['cloud_storage']['raw_path']}transactions.csv"
    merchant_input = f"gs://{bucket_name}/{config['cloud_storage']['enrichment_path']}merchants.csv"
    output_table = f"{project_id}:{dataset}.transactions_enriched"
    temp_location = f"gs://{bucket_name}/dataflow/temp"
    staging_location = f"gs://{bucket_name}/dataflow/staging"

    cmd = [
        "gcloud", "dataflow", "jobs", "run", "fraud-detection-etl",
        f"--gcs-location=gs://dataflow-templates/latest/Word_Count",  # placeholder
        f"--region={region}",
    ]

    # Use direct Beam submission instead
    beam_cmd = [
        "python", pipeline_path,
        f"--input_transactions={txn_input}",
        f"--input_merchants={merchant_input}",
        f"--output_table={output_table}",
        f"--runner=DataflowRunner",
        f"--project={project_id}",
        f"--region={region}",
        f"--temp_location={temp_location}",
        f"--staging_location={staging_location}",
        f"--setup_file={os.path.join(os.path.dirname(os.path.abspath(__file__)), 'setup.py')}",
    ]

    print("Submitting pipeline to Dataflow...")
    print(f"  Transactions: {txn_input}")
    print(f"  Merchants:    {merchant_input}")
    print(f"  Output:       {output_table}")
    print(f"  Region:       {region}")
    print()

    # Print the command for reference
    print("Command:")
    print("  " + " \\\n    ".join(beam_cmd))
    print()

    confirm = input("Run this on Dataflow? (y/n): ").strip().lower()
    if confirm == "y":
        subprocess.run(beam_cmd, check=True)
        print("Pipeline submitted. Check progress at:")
        print(f"  https://console.cloud.google.com/dataflow/jobs?project={project_id}")
    else:
        print("Cancelled.")


if __name__ == "__main__":
    main()
