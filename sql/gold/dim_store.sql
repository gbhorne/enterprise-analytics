-- Gold Layer: dim_store
-- Store dimension with geographic hierarchy.

CREATE OR REPLACE TABLE `${PROJECT_ID}.retail_gold.dim_store`
CLUSTER BY region, country_code AS
SELECT
    FARM_FINGERPRINT(store_id) AS store_key,
    store_id, store_name, store_type, city, country_code, region,
    square_footage, open_date, is_active, years_open,
    CURRENT_TIMESTAMP() AS _updated_at
FROM `${PROJECT_ID}.retail_silver.stg_stores`;
