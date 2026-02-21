-- Gold Layer: fct_inventory
-- Daily inventory snapshots with status classification.
-- Grain: one row per store x product x date.

CREATE OR REPLACE TABLE `${PROJECT_ID}.retail_gold.fct_inventory`
PARTITION BY snapshot_date
CLUSTER BY store_id, product_id AS
SELECT
    FARM_FINGERPRINT(CONCAT(CAST(snapshot_date AS STRING), '|', store_id, '|', product_id)) AS inventory_key,
    snapshot_date, store_id, product_id,
    stock_on_hand, stock_on_order, reorder_point,
    is_below_reorder_point, is_stockout, days_since_last_sale,
    CASE
        WHEN is_stockout THEN 'stockout'
        WHEN is_below_reorder_point THEN 'low_stock'
        WHEN stock_on_hand > reorder_point * 5 THEN 'overstock'
        ELSE 'healthy'
    END AS inventory_status,
    CURRENT_TIMESTAMP() AS _updated_at
FROM `${PROJECT_ID}.retail_silver.stg_inventory`;
