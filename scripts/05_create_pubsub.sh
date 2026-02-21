#!/bin/bash
###############################################################################
# Step 5: Create Pub/Sub Topics & Subscriptions
# Streaming infrastructure for real-time event ingestion.
###############################################################################

export PROJECT_ID="${PROJECT_ID:?Set PROJECT_ID}"
export ENV="${ENV:-dev}"

# Topics
gcloud pubsub topics create ${ENV}-retail-transactions --message-retention-duration=24h 2>/dev/null || echo "transactions topic exists"
gcloud pubsub topics create ${ENV}-retail-inventory --message-retention-duration=24h 2>/dev/null || echo "inventory topic exists"
gcloud pubsub topics create ${ENV}-retail-dead-letter 2>/dev/null || echo "dead-letter topic exists"
gcloud pubsub topics create ${ENV}-pipeline-notifications 2>/dev/null || echo "notifications topic exists"

# Subscriptions
gcloud pubsub subscriptions create ${ENV}-transactions-pull \
  --topic=${ENV}-retail-transactions --ack-deadline=60 2>/dev/null || echo "transactions sub exists"
gcloud pubsub subscriptions create ${ENV}-inventory-pull \
  --topic=${ENV}-retail-inventory --ack-deadline=60 2>/dev/null || echo "inventory sub exists"
gcloud pubsub subscriptions create ${ENV}-dead-letter-monitor \
  --topic=${ENV}-retail-dead-letter --ack-deadline=120 2>/dev/null || echo "dead-letter sub exists"

echo "✅ Pub/Sub created"
gcloud pubsub topics list --format="value(name)" | grep ${ENV}
