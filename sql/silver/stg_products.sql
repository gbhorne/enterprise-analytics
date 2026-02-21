-- Silver Layer: stg_products
-- Deduplicates, standardizes, derives margin_pct and price_tier.

CREATE OR REPLACE VIEW `${PROJECT_ID}.retail_silver.stg_products` AS
WITH deduplicated AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY _ingested_at DESC) AS _rn
    FROM `${PROJECT_ID}.retail_bronze.raw_products`
)
SELECT
    product_id, TRIM(product_name) AS product_name,
    LOWER(TRIM(category_l1)) AS category_l1,
    LOWER(TRIM(COALESCE(NULLIF(category_l2,''), 'uncategorized'))) AS category_l2,
    LOWER(TRIM(COALESCE(NULLIF(category_l3,''), 'uncategorized'))) AS category_l3,
    LOWER(TRIM(COALESCE(NULLIF(brand,''), 'unbranded'))) AS brand,
    supplier_id,
    CAST(unit_cost AS NUMERIC) AS unit_cost,
    CAST(list_price AS NUMERIC) AS list_price,
    weight_kg, is_active, launch_date,
    ROUND(SAFE_DIVIDE(CAST(list_price AS NUMERIC) - CAST(unit_cost AS NUMERIC), CAST(list_price AS NUMERIC)) * 100, 2) AS margin_pct,
    CASE
        WHEN CAST(list_price AS NUMERIC) >= 500 THEN 'premium'
        WHEN CAST(list_price AS NUMERIC) >= 100 THEN 'mid_range'
        ELSE 'value'
    END AS price_tier,
    _ingested_at, _source_system
FROM deduplicated
WHERE _rn = 1 AND product_id IS NOT NULL;
