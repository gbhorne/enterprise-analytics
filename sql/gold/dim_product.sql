-- Gold Layer: dim_product
-- Product dimension with 3-level category hierarchy and margin analysis.

CREATE OR REPLACE TABLE `${PROJECT_ID}.retail_gold.dim_product`
CLUSTER BY category_l1, brand AS
SELECT
    FARM_FINGERPRINT(product_id) AS product_key,
    product_id, product_name, category_l1, category_l2, category_l3,
    CONCAT(category_l1, ' > ', category_l2, ' > ', category_l3) AS category_path,
    brand, supplier_id, unit_cost, list_price, margin_pct, price_tier,
    is_active, launch_date,
    CURRENT_TIMESTAMP() AS _updated_at
FROM `${PROJECT_ID}.retail_silver.stg_products`;
