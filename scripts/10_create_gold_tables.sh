#!/bin/bash
###############################################################################
# Step 10: Create Gold Layer Star Schema
# Dimensions first, then facts (facts depend on silver views).
# Order matters: dim_date, dim_store, dim_product, dim_customer,
# then fct_sales, fct_daily_sales, fct_inventory.
###############################################################################

export PROJECT_ID="${PROJECT_ID:?Set PROJECT_ID}"
DIR="$(cd "$(dirname "$0")/../sql/gold" && pwd)"

# Run in dependency order
for sql_file in dim_date dim_store dim_product dim_customer fct_sales fct_daily_sales fct_inventory; do
  echo "Creating ${sql_file}..."
  sed "s/\${PROJECT_ID}/${PROJECT_ID}/g" ${DIR}/${sql_file}.sql | bq query --use_legacy_sql=false
done

echo "✅ Gold layer created"
bq ls ${PROJECT_ID}:retail_gold
