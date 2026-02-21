#!/bin/bash
###############################################################################
# Step 3: Create BigQuery Datasets
# One dataset per medallion layer + staging + data quality.
###############################################################################

export PROJECT_ID="${PROJECT_ID:?Set PROJECT_ID}"
export REGION="${REGION:-us-central1}"

bq mk --dataset --description "Bronze Layer - Raw ingested data from source systems" \
  --location ${REGION} ${PROJECT_ID}:retail_bronze 2>/dev/null || echo "retail_bronze exists"

bq mk --dataset --description "Silver Layer - Cleansed, deduplicated, type-standardized" \
  --location ${REGION} ${PROJECT_ID}:retail_silver 2>/dev/null || echo "retail_silver exists"

bq mk --dataset --description "Gold Layer - Star schema facts and dimensions for analytics" \
  --location ${REGION} ${PROJECT_ID}:retail_gold 2>/dev/null || echo "retail_gold exists"

bq mk --dataset --description "Staging - Temporary ETL tables (24hr auto-expire)" \
  --location ${REGION} --default_table_expiration 86400 \
  ${PROJECT_ID}:retail_staging 2>/dev/null || echo "retail_staging exists"

bq mk --dataset --description "Data Quality - Test results and pipeline metrics" \
  --location ${REGION} ${PROJECT_ID}:retail_data_quality 2>/dev/null || echo "retail_data_quality exists"

echo "✅ Datasets created"
bq ls --project_id ${PROJECT_ID}
