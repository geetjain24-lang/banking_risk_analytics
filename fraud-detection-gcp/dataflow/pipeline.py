"""
Fraud Detection ETL Pipeline - Apache Beam / Google Cloud Dataflow

Pipeline steps:
1. Read raw transactions CSV from Cloud Storage
2. Parse and validate records
3. Enrich with merchant risk data (side input)
4. Compute fraud features (amount anomaly, velocity flags)
5. Write enriched data to BigQuery
"""

import argparse
import csv
import io
import json
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions, SetupOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition


# --- Schema ---
BIGQUERY_SCHEMA = {
    "fields": [
        {"name": "transaction_id", "type": "STRING"},
        {"name": "customer_id", "type": "INTEGER"},
        {"name": "timestamp", "type": "TIMESTAMP"},
        {"name": "amount", "type": "FLOAT"},
        {"name": "currency", "type": "STRING"},
        {"name": "merchant_category", "type": "STRING"},
        {"name": "merchant_id", "type": "STRING"},
        {"name": "channel", "type": "STRING"},
        {"name": "country", "type": "STRING"},
        {"name": "city", "type": "STRING"},
        {"name": "is_international", "type": "BOOLEAN"},
        {"name": "is_fraud", "type": "INTEGER"},
        {"name": "merchant_risk_tier", "type": "STRING"},
        {"name": "amount_bucket", "type": "STRING"},
        {"name": "is_high_risk_country", "type": "BOOLEAN"},
        {"name": "is_high_risk_merchant", "type": "BOOLEAN"},
        {"name": "risk_flag_count", "type": "INTEGER"},
    ]
}

HIGH_RISK_COUNTRIES = {"NG", "RU", "CN", "PH", "UA", "RO"}


class ParseTransaction(beam.DoFn):
    """Parse a CSV line into a transaction dictionary."""

    def __init__(self):
        self.parse_errors = beam.metrics.Metrics.counter("pipeline", "parse_errors")
        self.valid_records = beam.metrics.Metrics.counter("pipeline", "valid_records")

    def process(self, line):
        try:
            reader = csv.DictReader(io.StringIO(line), fieldnames=[
                "transaction_id", "customer_id", "timestamp", "amount", "currency",
                "merchant_category", "merchant_id", "channel", "country", "city",
                "is_international", "is_fraud",
            ])
            record = next(reader)

            # Skip header row
            if record["transaction_id"] == "transaction_id":
                return

            # Type conversions
            record["customer_id"] = int(record["customer_id"])
            record["amount"] = float(record["amount"])
            record["is_international"] = record["is_international"] == "True"
            record["is_fraud"] = int(record["is_fraud"])

            self.valid_records.inc()
            yield record

        except Exception as e:
            self.parse_errors.inc()
            logging.warning(f"Failed to parse line: {e}")


class ParseMerchant(beam.DoFn):
    """Parse merchant CSV into (merchant_id, risk_tier) tuples."""

    def process(self, line):
        try:
            reader = csv.DictReader(io.StringIO(line), fieldnames=[
                "merchant_id", "merchant_name", "category", "country", "city",
                "risk_tier", "registered_since",
            ])
            record = next(reader)
            if record["merchant_id"] == "merchant_id":
                return
            yield (record["merchant_id"], record["risk_tier"])
        except Exception:
            pass


class EnrichTransaction(beam.DoFn):
    """Enrich transaction with merchant risk tier using side input."""

    def process(self, txn, merchant_lookup):
        merchant_id = txn["merchant_id"]
        risk_tier = merchant_lookup.get(merchant_id, "unknown")
        txn["merchant_risk_tier"] = risk_tier
        yield txn


class ComputeFeatures(beam.DoFn):
    """Compute fraud detection features for each transaction."""

    def process(self, txn):
        amount = txn["amount"]

        # Amount bucket
        if amount > 5000:
            txn["amount_bucket"] = "very_high"
        elif amount > 2000:
            txn["amount_bucket"] = "high"
        elif amount > 500:
            txn["amount_bucket"] = "medium"
        else:
            txn["amount_bucket"] = "low"

        # High-risk country flag
        txn["is_high_risk_country"] = txn["country"] in HIGH_RISK_COUNTRIES

        # High-risk merchant flag
        txn["is_high_risk_merchant"] = txn.get("merchant_risk_tier") == "high"

        # Count total risk flags
        risk_flags = sum([
            txn["is_high_risk_country"],
            txn["is_high_risk_merchant"],
            txn["is_international"],
            txn["channel"] == "online",
            amount > 2000,
        ])
        txn["risk_flag_count"] = risk_flags

        yield txn


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_transactions",
                        required=True,
                        help="GCS path to transactions CSV")
    parser.add_argument("--input_merchants",
                        required=True,
                        help="GCS path to merchants CSV")
    parser.add_argument("--output_table",
                        required=True,
                        help="BigQuery output table (project:dataset.table)")
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=pipeline_options) as p:

        # Step 1: Read and parse merchant data (used as side input)
        merchant_lookup = (
            p
            | "ReadMerchants" >> beam.io.ReadFromText(known_args.input_merchants)
            | "ParseMerchants" >> beam.ParDo(ParseMerchant())
        )
        merchant_dict = beam.pvalue.AsDict(merchant_lookup)

        # Step 2: Read and parse transactions
        transactions = (
            p
            | "ReadTransactions" >> beam.io.ReadFromText(known_args.input_transactions)
            | "ParseTransactions" >> beam.ParDo(ParseTransaction())
        )

        # Step 3: Enrich with merchant data
        enriched = (
            transactions
            | "EnrichWithMerchant" >> beam.ParDo(EnrichTransaction(), merchant_dict)
        )

        # Step 4: Compute fraud features
        featured = (
            enriched
            | "ComputeFeatures" >> beam.ParDo(ComputeFeatures())
        )

        # Step 5: Write to BigQuery
        featured | "WriteToBigQuery" >> WriteToBigQuery(
            known_args.output_table,
            schema=BIGQUERY_SCHEMA,
            create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=BigQueryDisposition.WRITE_TRUNCATE,
        )

    logging.info("Pipeline complete.")


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
