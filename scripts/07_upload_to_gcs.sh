#!/bin/bash
###############################################################################
# Step 7: Upload CSVs to Cloud Storage Bronze Layer
# Raw files land in GCS first (data lake pattern).
###############################################################################

export BUCKET_NAME="${BUCKET_NAME:?Set BUCKET_NAME}"

gsutil -m cp data/raw_stores.csv gs://${BUCKET_NAME}/bronze/stores/
gsutil -m cp data/raw_products.csv gs://${BUCKET_NAME}/bronze/products/
gsutil -m cp data/raw_customers.csv gs://${BUCKET_NAME}/bronze/customers/
gsutil -m cp data/raw_transactions.csv gs://${BUCKET_NAME}/bronze/transactions/
gsutil -m cp data/raw_inventory.csv gs://${BUCKET_NAME}/bronze/inventory/

echo "✅ CSVs uploaded to GCS"
gsutil ls -l gs://${BUCKET_NAME}/bronze/*/raw_*.csv
