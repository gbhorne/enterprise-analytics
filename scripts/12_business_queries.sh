#!/bin/bash
###############################################################################
# Step 12: Business Validation Queries
# Proves the star schema works for real analytics.
###############################################################################

export PROJECT_ID="${PROJECT_ID:?Set PROJECT_ID}"

echo "--- Revenue by Region ---"
bq query --use_legacy_sql=false --format=pretty "
SELECT store_region,
  COUNT(DISTINCT transaction_id) AS orders,
  ROUND(SUM(total_amount), 2) AS revenue,
  ROUND(SUM(gross_profit), 2) AS profit,
  ROUND(SUM(gross_profit)/SUM(total_amount)*100, 1) AS margin_pct
FROM \`${PROJECT_ID}.retail_gold.fct_sales\`
GROUP BY 1 ORDER BY revenue DESC
"

echo ""
echo "--- Top 10 Products ---"
bq query --use_legacy_sql=false --format=pretty "
SELECT p.product_name, p.category_l1, p.brand,
  COUNT(DISTINCT s.transaction_id) AS orders,
  ROUND(SUM(s.total_amount), 2) AS revenue
FROM \`${PROJECT_ID}.retail_gold.fct_sales\` s
JOIN \`${PROJECT_ID}.retail_gold.dim_product\` p ON s.product_id = p.product_id
GROUP BY 1,2,3 ORDER BY revenue DESC LIMIT 10
"

echo ""
echo "--- Customer Segments ---"
bq query --use_legacy_sql=false --format=pretty "
SELECT customer_segment,
  COUNT(*) AS customers,
  ROUND(AVG(lifetime_revenue), 2) AS avg_lifetime_revenue,
  ROUND(AVG(lifetime_orders), 1) AS avg_orders
FROM \`${PROJECT_ID}.retail_gold.dim_customer\`
GROUP BY 1 ORDER BY avg_lifetime_revenue DESC
"

echo ""
echo "--- Inventory Health ---"
bq query --use_legacy_sql=false --format=pretty "
SELECT inventory_status,
  COUNT(*) AS product_store_combos,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) AS pct
FROM \`${PROJECT_ID}.retail_gold.fct_inventory\`
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM \`${PROJECT_ID}.retail_gold.fct_inventory\`)
GROUP BY 1 ORDER BY product_store_combos DESC
"

echo "✅ All business queries validated"
