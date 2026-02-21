-- Gold Layer: dim_customer
-- Customer dimension with lifetime metrics and RFM-based segmentation.

CREATE OR REPLACE TABLE `${PROJECT_ID}.retail_gold.dim_customer`
CLUSTER BY customer_id, loyalty_tier AS
WITH metrics AS (
    SELECT
        customer_id,
        COUNT(DISTINCT transaction_id) AS lifetime_orders,
        SUM(total_amount) AS lifetime_revenue,
        AVG(total_amount) AS avg_order_value,
        MIN(transaction_date) AS first_purchase_date,
        MAX(transaction_date) AS last_purchase_date,
        DATE_DIFF(CURRENT_DATE(), MAX(transaction_date), DAY) AS days_since_last_purchase,
        ROUND(SAFE_DIVIDE(COUNTIF(channel = 'online'), COUNT(*)) * 100, 1) AS online_pct
    FROM `${PROJECT_ID}.retail_silver.stg_transactions`
    WHERE customer_id IS NOT NULL AND customer_id != ''
    GROUP BY 1
)
SELECT
    FARM_FINGERPRINT(c.customer_id) AS customer_key,
    c.customer_id, c.first_name, c.last_name, c.email,
    c.city, c.country_code, c.loyalty_tier, c.signup_date,
    c.is_active, c.activity_status, c.days_since_signup,
    COALESCE(m.lifetime_orders, 0) AS lifetime_orders,
    COALESCE(m.lifetime_revenue, 0) AS lifetime_revenue,
    ROUND(COALESCE(m.avg_order_value, 0), 2) AS avg_order_value,
    m.first_purchase_date, m.last_purchase_date,
    COALESCE(m.days_since_last_purchase, 9999) AS days_since_last_purchase,
    COALESCE(m.online_pct, 0) AS online_pct,
    CASE
        WHEN COALESCE(m.lifetime_revenue,0) >= 5000 AND COALESCE(m.days_since_last_purchase,9999) <= 30 THEN 'vip_active'
        WHEN COALESCE(m.lifetime_revenue,0) >= 5000 THEN 'vip_at_risk'
        WHEN COALESCE(m.lifetime_revenue,0) >= 1000 AND COALESCE(m.days_since_last_purchase,9999) <= 60 THEN 'loyal'
        WHEN COALESCE(m.lifetime_orders,0) >= 3 AND COALESCE(m.days_since_last_purchase,9999) <= 90 THEN 'regular'
        WHEN COALESCE(m.lifetime_orders,0) >= 1 THEN 'occasional'
        ELSE 'prospect'
    END AS customer_segment,
    CURRENT_TIMESTAMP() AS _updated_at
FROM `${PROJECT_ID}.retail_silver.stg_customers` c
LEFT JOIN metrics m ON c.customer_id = m.customer_id;
