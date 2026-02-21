#!/bin/bash
###############################################################################
# Step 9: Create Silver Layer Views
# Cleansed, deduplicated, type-standardized views on top of bronze.
# Views cost $0 for storage.
###############################################################################

export PROJECT_ID="${PROJECT_ID:?Set PROJECT_ID}"
DIR="$(cd "$(dirname "$0")/../sql/silver" && pwd)"

for sql_file in ${DIR}/stg_*.sql; do
  name=$(basename ${sql_file} .sql)
  echo "Creating ${name}..."
  # Replace ${PROJECT_ID} placeholder with actual value
  sed "s/\${PROJECT_ID}/${PROJECT_ID}/g" ${sql_file} | bq query --use_legacy_sql=false
done

echo "✅ Silver views created"
bq ls ${PROJECT_ID}:retail_silver
