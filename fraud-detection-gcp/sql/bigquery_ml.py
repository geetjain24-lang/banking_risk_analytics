"""
BigQuery ML - Train and evaluate a fraud detection model.

Steps:
1. Create training dataset with features
2. Train a Logistic Regression classifier
3. Evaluate model performance
4. Run predictions on all transactions
"""

import os
import yaml
from google.cloud import bigquery

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def load_config():
    config_path = os.path.join(BASE_DIR, "config", "config.yaml")
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def run_query(client, sql, description, wait=True):
    print(f"{description}...")
    job = client.query(sql)
    if wait:
        job.result()
        print(f"  Done.")
    return job


def main():
    config = load_config()
    project_id = config["project"]["gcp_project_id"]
    dataset = config["bigquery"]["dataset"]
    table = f"{project_id}.{dataset}"

    client = bigquery.Client(project=project_id)

    # Step 1: Create feature table for ML training
    print("=" * 50)
    print("STEP 1: Creating feature table")
    print("=" * 50)
    run_query(client, f"""
    CREATE OR REPLACE TABLE `{table}.ml_features` AS
    SELECT
        transaction_id,
        amount,
        CASE channel
            WHEN 'online' THEN 1
            WHEN 'swipe' THEN 2
            WHEN 'chip' THEN 3
            WHEN 'contactless' THEN 4
            ELSE 0
        END AS channel_encoded,
        merchant_category,
        country,
        is_international,
        is_high_risk_country,
        is_high_risk_merchant,
        risk_flag_count,
        CASE amount_bucket
            WHEN 'low' THEN 0
            WHEN 'medium' THEN 1
            WHEN 'high' THEN 2
            WHEN 'very_high' THEN 3
            ELSE 0
        END AS amount_bucket_encoded,
        EXTRACT(HOUR FROM timestamp) AS txn_hour,
        EXTRACT(DAYOFWEEK FROM timestamp) AS txn_day_of_week,
        is_fraud AS label
    FROM `{table}.transactions_enriched`
    """, "Creating feature table from enriched transactions")

    features_table = client.get_table(f"{table}.ml_features")
    print(f"  Feature table: {features_table.num_rows:,} rows, {len(features_table.schema)} columns")

    # Step 2: Train Logistic Regression model
    print()
    print("=" * 50)
    print("STEP 2: Training fraud detection model")
    print("=" * 50)
    run_query(client, f"""
    CREATE OR REPLACE MODEL `{table}.fraud_model`
    OPTIONS(
        model_type='LOGISTIC_REG',
        input_label_cols=['label'],
        auto_class_weights=TRUE,
        max_iterations=20,
        data_split_method='AUTO_SPLIT'
    ) AS
    SELECT
        amount,
        channel_encoded,
        merchant_category,
        country,
        is_international,
        is_high_risk_country,
        is_high_risk_merchant,
        risk_flag_count,
        amount_bucket_encoded,
        txn_hour,
        txn_day_of_week,
        label
    FROM `{table}.ml_features`
    """, "Training logistic regression model (this may take a few minutes)")

    # Step 3: Evaluate model
    print()
    print("=" * 50)
    print("STEP 3: Model evaluation")
    print("=" * 50)
    eval_sql = f"""
    SELECT
        ROUND(precision, 4) AS precision,
        ROUND(recall, 4) AS recall,
        ROUND(accuracy, 4) AS accuracy,
        ROUND(f1_score, 4) AS f1_score,
        ROUND(log_loss, 4) AS log_loss,
        ROUND(roc_auc, 4) AS roc_auc
    FROM ML.EVALUATE(MODEL `{table}.fraud_model`)
    """
    print("Evaluating model...")
    for row in client.query(eval_sql).result():
        print(f"  Precision:  {row.precision}")
        print(f"  Recall:     {row.recall}")
        print(f"  Accuracy:   {row.accuracy}")
        print(f"  F1 Score:   {row.f1_score}")
        print(f"  Log Loss:   {row.log_loss}")
        print(f"  ROC AUC:    {row.roc_auc}")

    # Step 4: Run predictions and save to table
    print()
    print("=" * 50)
    print("STEP 4: Running predictions")
    print("=" * 50)
    run_query(client, f"""
    CREATE OR REPLACE TABLE `{table}.fraud_predictions` AS
    SELECT
        f.transaction_id,
        f.amount,
        f.country,
        f.merchant_category,
        f.risk_flag_count,
        f.label AS actual_fraud,
        p.predicted_label AS predicted_fraud,
        ROUND(p.predicted_label_probs[OFFSET(0)].prob, 4) AS fraud_probability
    FROM ML.PREDICT(MODEL `{table}.fraud_model`,
        TABLE `{table}.ml_features`) p
    JOIN `{table}.ml_features` f
        ON f.transaction_id = p.transaction_id
    """, "Generating fraud predictions for all transactions")

    pred_table = client.get_table(f"{table}.fraud_predictions")
    print(f"  Predictions table: {pred_table.num_rows:,} rows")

    # Step 5: Confusion matrix summary
    print()
    print("=" * 50)
    print("STEP 5: Prediction summary")
    print("=" * 50)
    summary_sql = f"""
    SELECT
        CASE
            WHEN actual_fraud = 1 AND predicted_fraud = 1 THEN 'True Positive'
            WHEN actual_fraud = 0 AND predicted_fraud = 0 THEN 'True Negative'
            WHEN actual_fraud = 0 AND predicted_fraud = 1 THEN 'False Positive'
            WHEN actual_fraud = 1 AND predicted_fraud = 0 THEN 'False Negative'
        END AS category,
        COUNT(*) AS count
    FROM `{table}.fraud_predictions`
    GROUP BY category
    ORDER BY category
    """
    print("Confusion matrix:")
    for row in client.query(summary_sql).result():
        print(f"  {row.category:20s} | {row.count:>7,}")

    print()
    print("BigQuery ML pipeline complete.")


if __name__ == "__main__":
    main()