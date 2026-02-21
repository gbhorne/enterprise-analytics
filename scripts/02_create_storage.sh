#!/bin/bash
###############################################################################
# Step 2: Create Cloud Storage Structure
# Sets up the bronze data lake with folder organization by source.
###############################################################################

export PROJECT_ID="${PROJECT_ID:?Set PROJECT_ID}"
export BUCKET_NAME="${BUCKET_NAME:?Set BUCKET_NAME}"
export REGION="${REGION:-us-central1}"

# Create bucket (skip if exists)
gsutil mb -p ${PROJECT_ID} -l ${REGION} -c STANDARD gs://${BUCKET_NAME} 2>/dev/null || echo "Bucket exists"

# Create folder structure
for folder in transactions customers products stores inventory; do
  echo "placeholder" | gsutil cp - gs://${BUCKET_NAME}/bronze/${folder}/.keep
done
echo "placeholder" | gsutil cp - gs://${BUCKET_NAME}/dead-letter/.keep
echo "placeholder" | gsutil cp - gs://${BUCKET_NAME}/dataflow-temp/.keep

echo "✅ Storage structure created"
gsutil ls gs://${BUCKET_NAME}/bronze/
