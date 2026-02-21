# Cost Analysis

## Lab Environment

| Configuration | Monthly Cost |
|---|---|
| GCP Sandbox | **$0** |
| Personal GCP (free tier) | **~$10-15** |
| Personal GCP + Cloud Composer | **~$300-400** |

## Why It's Cheap

- BigQuery free tier: 1 TB queries + 10 GB storage/month
- Cloud Storage: 5 GB free
- Pub/Sub: 10 GB messages free
- Silver layer uses views ($0 storage)
- Partitioning reduces bytes scanned per query

## Production Estimate (25M transactions/month)

| Component | Monthly Cost |
|---|---|
| BigQuery (queries + storage) | $225 |
| Dataflow (streaming + batch) | $9,600 |
| Cloud Storage (30 TB bronze) | $600 |
| Cloud Composer (3 nodes) | $1,350 |
| Pub/Sub (500M messages) | $200 |
| Looker (100 users) | $10,000 |
| **Total** | **~$22,000** |
| **Optimized (flat-rate slots + tuning)** | **~$19,000** |
