-- Silver Layer: stg_transactions
-- Deduplicates, type-casts, standardizes, and derives transaction_tier.
-- Materialized as VIEW ($0 storage cost).

CREATE OR REPLACE VIEW `${PROJECT_ID}.retail_silver.stg_transactions` AS
WITH deduplicated AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY transaction_id, product_id ORDER BY _ingested_at DESC) AS _rn
    FROM `${PROJECT_ID}.retail_bronze.raw_transactions`
)
SELECT
    transaction_id, transaction_date, transaction_timestamp,
    store_id, customer_id, product_id,
    CAST(quantity AS INT64) AS quantity,
    CAST(unit_price AS NUMERIC) AS unit_price,
    COALESCE(CAST(discount_amount AS NUMERIC), 0) AS discount_amount,
    COALESCE(CAST(tax_amount AS NUMERIC), 0) AS tax_amount,
    CAST(total_amount AS NUMERIC) AS total_amount,
    LOWER(TRIM(COALESCE(payment_method, 'unknown'))) AS payment_method,
    LOWER(TRIM(channel)) AS channel,
    UPPER(TRIM(currency_code)) AS currency_code,
    CASE
        WHEN CAST(total_amount AS NUMERIC) > 500 THEN 'high'
        WHEN CAST(total_amount AS NUMERIC) > 100 THEN 'medium'
        ELSE 'low'
    END AS transaction_tier,
    _ingested_at, _source_system
FROM deduplicated
WHERE _rn = 1 AND transaction_id IS NOT NULL
  AND CAST(total_amount AS NUMERIC) >= 0 AND CAST(quantity AS INT64) > 0;
