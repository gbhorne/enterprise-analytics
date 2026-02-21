-- Silver Layer: stg_customers
-- Deduplicates, standardizes names/emails, derives activity_status.

CREATE OR REPLACE VIEW `${PROJECT_ID}.retail_silver.stg_customers` AS
WITH deduplicated AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY _ingested_at DESC) AS _rn
    FROM `${PROJECT_ID}.retail_bronze.raw_customers`
)
SELECT
    customer_id,
    INITCAP(TRIM(first_name)) AS first_name,
    INITCAP(TRIM(last_name)) AS last_name,
    LOWER(TRIM(email)) AS email,
    phone, date_of_birth,
    LOWER(TRIM(gender)) AS gender,
    TRIM(address_line1) AS address_line1,
    INITCAP(TRIM(city)) AS city,
    UPPER(TRIM(state_province)) AS state_province,
    UPPER(TRIM(postal_code)) AS postal_code,
    UPPER(TRIM(country_code)) AS country_code,
    LOWER(TRIM(COALESCE(NULLIF(loyalty_tier, ''), 'none'))) AS loyalty_tier,
    signup_date, last_activity_date, is_active, marketing_opt_in,
    DATE_DIFF(CURRENT_DATE(), signup_date, DAY) AS days_since_signup,
    DATE_DIFF(CURRENT_DATE(), last_activity_date, DAY) AS days_since_last_activity,
    CASE
        WHEN DATE_DIFF(CURRENT_DATE(), last_activity_date, DAY) <= 30 THEN 'active'
        WHEN DATE_DIFF(CURRENT_DATE(), last_activity_date, DAY) <= 90 THEN 'at_risk'
        WHEN DATE_DIFF(CURRENT_DATE(), last_activity_date, DAY) <= 365 THEN 'lapsed'
        ELSE 'dormant'
    END AS activity_status,
    _ingested_at, _source_system
FROM deduplicated
WHERE _rn = 1 AND customer_id IS NOT NULL;
