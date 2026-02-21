-- Silver Layer: stg_inventory
-- Deduplicates, derives stockout flags.

CREATE OR REPLACE VIEW `${PROJECT_ID}.retail_silver.stg_inventory` AS
WITH deduplicated AS (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY snapshot_date, store_id, product_id ORDER BY _ingested_at DESC
    ) AS _rn
    FROM `${PROJECT_ID}.retail_bronze.raw_inventory`
)
SELECT
    snapshot_date, store_id, product_id,
    CAST(stock_on_hand AS INT64) AS stock_on_hand,
    CAST(COALESCE(stock_on_order, 0) AS INT64) AS stock_on_order,
    CAST(COALESCE(reorder_point, 0) AS INT64) AS reorder_point,
    last_received_date, last_sold_date,
    CAST(stock_on_hand AS INT64) <= CAST(COALESCE(reorder_point, 0) AS INT64) AS is_below_reorder_point,
    CAST(stock_on_hand AS INT64) = 0 AS is_stockout,
    DATE_DIFF(snapshot_date, last_sold_date, DAY) AS days_since_last_sale,
    _ingested_at, _source_system
FROM deduplicated
WHERE _rn = 1 AND store_id IS NOT NULL AND product_id IS NOT NULL;
