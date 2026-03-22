-- Fraud Risk Scoring Query
-- Enriches each transaction with risk signals and computes a composite fraud score

WITH customer_stats AS (
    -- Per-customer aggregations over last 30 days
    SELECT
        customer_id,
        COUNT(*) AS txn_count_30d,
        SUM(amount) AS total_spend_30d,
        AVG(amount) AS avg_amount_30d,
        MAX(amount) AS max_amount_30d,
        COUNT(DISTINCT country) AS distinct_countries_30d,
        COUNTIF(is_international) AS intl_txn_count_30d
    FROM `{project_id}.{dataset}.transactions`
    WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
    GROUP BY customer_id
),

customer_velocity AS (
    -- Transactions in last 1 hour per customer (velocity check)
    SELECT
        customer_id,
        COUNT(*) AS txn_count_1h
    FROM `{project_id}.{dataset}.transactions`
    WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
    GROUP BY customer_id
)

SELECT
    t.transaction_id,
    t.customer_id,
    t.timestamp,
    t.amount,
    t.merchant_category,
    t.channel,
    t.country,
    t.city,
    t.is_international,
    m.risk_tier AS merchant_risk_tier,

    -- Risk Score Calculation (0-100)
    (
        -- High amount relative to customer average
        CASE
            WHEN cs.avg_amount_30d > 0 AND t.amount > cs.avg_amount_30d * 3 THEN 20
            WHEN cs.avg_amount_30d > 0 AND t.amount > cs.avg_amount_30d * 2 THEN 10
            ELSE 0
        END
        +
        -- High-risk merchant
        CASE WHEN m.risk_tier = 'high' THEN 15 ELSE 0 END
        +
        -- International transaction
        CASE WHEN t.is_international THEN 10 ELSE 0 END
        +
        -- High-risk country
        CASE
            WHEN t.country IN ('NG', 'RU', 'UA', 'RO', 'PH', 'CN') THEN 20
            ELSE 0
        END
        +
        -- Online channel (higher risk than chip/contactless)
        CASE WHEN t.channel = 'online' THEN 10 ELSE 0 END
        +
        -- Velocity spike (many txns in short window)
        CASE
            WHEN cv.txn_count_1h >= 5 THEN 15
            WHEN cv.txn_count_1h >= 3 THEN 8
            ELSE 0
        END
        +
        -- Large absolute amount
        CASE
            WHEN t.amount > 5000 THEN 10
            WHEN t.amount > 2000 THEN 5
            ELSE 0
        END
    ) AS fraud_score,

    -- Risk Category
    CASE
        WHEN m.risk_tier = 'high' AND t.country IN ('NG', 'RU', 'UA', 'RO', 'PH', 'CN') THEN 'Critical'
        WHEN m.risk_tier = 'high' OR t.country IN ('NG', 'RU', 'UA', 'RO', 'PH', 'CN') THEN 'High'
        WHEN t.is_international AND t.channel = 'online' THEN 'Medium'
        WHEN t.amount > 2000 THEN 'Medium'
        ELSE 'Low'
    END AS risk_category,

    t.is_fraud AS actual_fraud_label

FROM `{project_id}.{dataset}.transactions` t
LEFT JOIN `{project_id}.{dataset}.merchants` m
    ON t.merchant_id = m.merchant_id
LEFT JOIN customer_stats cs
    ON t.customer_id = cs.customer_id
LEFT JOIN customer_velocity cv
    ON t.customer_id = cv.customer_id
ORDER BY fraud_score DESC