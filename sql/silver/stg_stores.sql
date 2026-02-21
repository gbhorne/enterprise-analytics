-- Silver Layer: stg_stores
-- Standardizes fields, derives years_open.

CREATE OR REPLACE VIEW `${PROJECT_ID}.retail_silver.stg_stores` AS
SELECT
    store_id, TRIM(store_name) AS store_name,
    LOWER(TRIM(store_type)) AS store_type,
    TRIM(address) AS address, INITCAP(TRIM(city)) AS city,
    UPPER(TRIM(state_province)) AS state_province,
    UPPER(TRIM(country_code)) AS country_code,
    UPPER(TRIM(region)) AS region, timezone,
    square_footage, open_date, close_date, is_active,
    TRIM(manager_name) AS manager_name,
    DATE_DIFF(CURRENT_DATE(), open_date, YEAR) AS years_open,
    _ingested_at, _source_system
FROM `${PROJECT_ID}.retail_bronze.raw_stores`
WHERE store_id IS NOT NULL;
