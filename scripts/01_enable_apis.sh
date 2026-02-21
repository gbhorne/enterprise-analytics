#!/bin/bash
###############################################################################
# Step 1: Enable Required GCP APIs
# These must be activated before using any GCP service.
###############################################################################

export PROJECT_ID="${PROJECT_ID:?Set PROJECT_ID}"
gcloud config set project ${PROJECT_ID}

gcloud services enable \
  bigquery.googleapis.com \
  bigquerydatatransfer.googleapis.com \
  storage.googleapis.com \
  dataflow.googleapis.com \
  pubsub.googleapis.com \
  datacatalog.googleapis.com \
  dlp.googleapis.com \
  monitoring.googleapis.com \
  logging.googleapis.com \
  cloudbuild.googleapis.com \
  cloudscheduler.googleapis.com \
  --quiet

echo "✅ APIs enabled"
