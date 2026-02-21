-- Gold Layer: fct_sales
-- Central sales fact. Grain: one row per transaction line item.
-- Partitioned by transaction_date, clustered by store_id + product_id.
-- Pre-joined with product and store attributes for dashboard performance.

CREATE OR REPLACE TABLE `${PROJECT_ID}.retail_gold.fct_sales`
PARTITION BY transaction_date
CLUSTER BY store_id, product_id AS
SELECT
    FARM_FINGERPRINT(CONCAT(t.transaction_id, '|', t.product_id)) AS sale_key,
    t.transaction_id, t.transaction_date, t.transaction_timestamp,
    t.store_id, t.customer_id, t.product_id,
    t.quantity, t.unit_price, t.discount_amount, t.tax_amount, t.total_amount,
    t.payment_method, t.channel, t.currency_code, t.transaction_tier,
    p.product_name, p.category_l1, p.category_l2, p.brand, p.unit_cost, p.price_tier,
    t.total_amount - (p.unit_cost * t.quantity) AS gross_profit,
    ROUND(SAFE_DIVIDE(t.total_amount - (p.unit_cost * t.quantity), t.total_amount) * 100, 2) AS margin_pct,
    s.store_name, s.store_type, s.city AS store_city,
    s.country_code AS store_country, s.region AS store_region,
    EXTRACT(YEAR FROM t.transaction_date) AS txn_year,
    EXTRACT(MONTH FROM t.transaction_date) AS txn_month,
    EXTRACT(DAYOFWEEK FROM t.transaction_date) AS day_of_week,
    EXTRACT(HOUR FROM t.transaction_timestamp) AS hour_of_day,
    CASE WHEN EXTRACT(DAYOFWEEK FROM t.transaction_date) IN (1,7) THEN TRUE ELSE FALSE END AS is_weekend
FROM `${PROJECT_ID}.retail_silver.stg_transactions` t
LEFT JOIN `${PROJECT_ID}.retail_silver.stg_products` p ON t.product_id = p.product_id
LEFT JOIN `${PROJECT_ID}.retail_silver.stg_stores` s ON t.store_id = s.store_id;
