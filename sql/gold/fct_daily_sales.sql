-- Gold Layer: fct_daily_sales
-- Pre-aggregated daily metrics for fast dashboard queries.
-- Grain: one row per store x channel x date.

CREATE OR REPLACE TABLE `${PROJECT_ID}.retail_gold.fct_daily_sales`
PARTITION BY transaction_date
CLUSTER BY store_region, store_id AS
SELECT
    transaction_date, store_id, store_region, channel,
    COUNT(DISTINCT transaction_id) AS transaction_count,
    SUM(quantity) AS units_sold,
    COUNT(DISTINCT customer_id) AS unique_customers,
    ROUND(SUM(total_amount), 2) AS gross_revenue,
    ROUND(SUM(discount_amount), 2) AS total_discounts,
    ROUND(SUM(gross_profit), 2) AS gross_profit,
    ROUND(AVG(total_amount), 2) AS avg_transaction_value,
    ROUND(SAFE_DIVIDE(SUM(gross_profit), SUM(total_amount)) * 100, 2) AS margin_pct,
    CURRENT_TIMESTAMP() AS _updated_at
FROM `${PROJECT_ID}.retail_gold.fct_sales`
GROUP BY 1, 2, 3, 4;
