#!/bin/bash
# Deploy fraud detection pipeline to Google Cloud Dataflow
# This runs on GCP infrastructure with Python 3.11

PROJECT_ID="hallowed-scene-490801-i7"
REGION="us-central1"
BUCKET="hallowed-scene-490801-i7-fraud-detection"

python pipeline.py \
  --input_transactions="gs://${BUCKET}/raw/transactions/transactions.csv" \
  --input_merchants="gs://${BUCKET}/raw/enrichment/merchants.csv" \
  --output_table="${PROJECT_ID}:fraud_detection.transactions_enriched" \
  --runner=DataflowRunner \
  --project="${PROJECT_ID}" \
  --region="${REGION}" \
  --temp_location="gs://${BUCKET}/dataflow/temp" \
  --staging_location="gs://${BUCKET}/dataflow/staging" \
  --experiments=use_runner_v2 \
  --sdk_container_image=apache/beam_python3.11_sdk:latest