#!/bin/bash
###############################################################################
# Step 8: Load CSVs into BigQuery Bronze Layer
# ELT pattern: load raw data as-is, transform later in silver/gold.
###############################################################################

export PROJECT_ID="${PROJECT_ID:?Set PROJECT_ID}"
export BUCKET_NAME="${BUCKET_NAME:?Set BUCKET_NAME}"

echo "Loading stores..."
bq load --source_format=CSV --skip_leading_rows=1 --replace \
  ${PROJECT_ID}:retail_bronze.raw_stores \
  gs://${BUCKET_NAME}/bronze/stores/raw_stores.csv

echo "Loading products..."
bq load --source_format=CSV --skip_leading_rows=1 --replace \
  ${PROJECT_ID}:retail_bronze.raw_products \
  gs://${BUCKET_NAME}/bronze/products/raw_products.csv

echo "Loading customers..."
bq load --source_format=CSV --skip_leading_rows=1 --replace --allow_quoted_newlines \
  ${PROJECT_ID}:retail_bronze.raw_customers \
  gs://${BUCKET_NAME}/bronze/customers/raw_customers.csv

echo "Loading transactions..."
bq load --source_format=CSV --skip_leading_rows=1 --replace --allow_quoted_newlines \
  ${PROJECT_ID}:retail_bronze.raw_transactions \
  gs://${BUCKET_NAME}/bronze/transactions/raw_transactions.csv

echo "Loading inventory..."
bq load --source_format=CSV --skip_leading_rows=1 --replace --allow_quoted_newlines \
  ${PROJECT_ID}:retail_bronze.raw_inventory \
  gs://${BUCKET_NAME}/bronze/inventory/raw_inventory.csv

echo "✅ Bronze layer loaded"
bq query --use_legacy_sql=false --format=pretty "
SELECT 'raw_stores' as tbl, COUNT(*) as row_count FROM \`${PROJECT_ID}.retail_bronze.raw_stores\`
UNION ALL SELECT 'raw_products', COUNT(*) FROM \`${PROJECT_ID}.retail_bronze.raw_products\`
UNION ALL SELECT 'raw_customers', COUNT(*) FROM \`${PROJECT_ID}.retail_bronze.raw_customers\`
UNION ALL SELECT 'raw_transactions', COUNT(*) FROM \`${PROJECT_ID}.retail_bronze.raw_transactions\`
UNION ALL SELECT 'raw_inventory', COUNT(*) FROM \`${PROJECT_ID}.retail_bronze.raw_inventory\`
ORDER BY tbl
"
