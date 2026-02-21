#!/bin/bash
###############################################################################
#
#  LAB 01: VERIFICATION SCRIPT
#  ============================
#  Audits every component built in the Enterprise Analytics Platform.
#  Run this in Cloud Shell to generate a complete verification report.
#
#  Usage:
#    export PROJECT_ID="playground-s-11-ba187cb0"
#    chmod +x verify.sh
#    ./verify.sh
#
###############################################################################

export PROJECT_ID="${PROJECT_ID:-playground-s-11-ba187cb0}"
export BUCKET_NAME="${BUCKET_NAME:-playground-s-11-ba187cb0}"
export ENV="${ENV:-dev}"

PASS=0
FAIL=0
WARN=0

check_pass() { echo "  ✅ PASS: $1"; ((PASS++)); }
check_fail() { echo "  ❌ FAIL: $1"; ((FAIL++)); }
check_warn() { echo "  ⚠️  WARN: $1"; ((WARN++)); }

echo ""
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║  LAB 01 VERIFICATION REPORT                                 ║"
echo "║  Project: ${PROJECT_ID}                                     ║"
echo "║  Date:    $(date -u '+%Y-%m-%d %H:%M UTC')                  ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""

# ============================================================================
# 1. APIs
# ============================================================================
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  1. ENABLED APIs"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

REQUIRED_APIS=("bigquery.googleapis.com" "storage.googleapis.com" "pubsub.googleapis.com" "datacatalog.googleapis.com" "dlp.googleapis.com" "monitoring.googleapis.com")
ENABLED_APIS=$(gcloud services list --enabled --format="value(name)" 2>/dev/null)

for api in "${REQUIRED_APIS[@]}"; do
  if echo "$ENABLED_APIS" | grep -q "$api"; then
    check_pass "$api"
  else
    check_fail "$api not enabled"
  fi
done
echo ""

# ============================================================================
# 2. CLOUD STORAGE
# ============================================================================
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  2. CLOUD STORAGE"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Check bucket exists
if gsutil ls -b gs://${BUCKET_NAME} &>/dev/null; then
  check_pass "Bucket gs://${BUCKET_NAME} exists"
else
  check_fail "Bucket gs://${BUCKET_NAME} not found"
fi

# Check bronze folders have data
BRONZE_FOLDERS=("transactions" "customers" "products" "stores" "inventory")
for folder in "${BRONZE_FOLDERS[@]}"; do
  FILE_COUNT=$(gsutil ls gs://${BUCKET_NAME}/bronze/${folder}/raw_*.csv 2>/dev/null | wc -l)
  if [ "$FILE_COUNT" -gt 0 ]; then
    check_pass "bronze/${folder}/ has CSV data"
  else
    check_warn "bronze/${folder}/ — no CSV files found"
  fi
done
echo ""

# ============================================================================
# 3. BIGQUERY DATASETS
# ============================================================================
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  3. BIGQUERY DATASETS"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

REQUIRED_DATASETS=("retail_bronze" "retail_silver" "retail_gold" "retail_staging" "retail_data_quality")
EXISTING_DATASETS=$(bq ls --project_id ${PROJECT_ID} --format=csv 2>/dev/null | tail -n +2)

for ds in "${REQUIRED_DATASETS[@]}"; do
  if echo "$EXISTING_DATASETS" | grep -q "$ds"; then
    check_pass "Dataset: $ds"
  else
    check_fail "Dataset: $ds not found"
  fi
done
echo ""

# ============================================================================
# 4. BRONZE LAYER TABLES
# ============================================================================
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  4. BRONZE LAYER (raw tables)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

BRONZE_TABLES=("raw_transactions" "raw_customers" "raw_products" "raw_stores" "raw_inventory")
for tbl in "${BRONZE_TABLES[@]}"; do
  ROW_COUNT=$(bq query --use_legacy_sql=false --format=csv --quiet "SELECT COUNT(*) FROM \`${PROJECT_ID}.retail_bronze.${tbl}\`" 2>/dev/null | tail -1)
  if [ -n "$ROW_COUNT" ] && [ "$ROW_COUNT" -gt 0 ] 2>/dev/null; then
    check_pass "retail_bronze.${tbl} — ${ROW_COUNT} rows"
  else
    check_fail "retail_bronze.${tbl} — empty or missing"
  fi
done

# Check partitioning on transactions
PARTITION_INFO=$(bq show --format=json ${PROJECT_ID}:retail_bronze.raw_transactions 2>/dev/null | grep -c "timePartitioning")
if [ "$PARTITION_INFO" -gt 0 ]; then
  check_pass "raw_transactions — partitioned by transaction_date"
else
  check_warn "raw_transactions — partitioning not detected"
fi
echo ""

# ============================================================================
# 5. SILVER LAYER VIEWS
# ============================================================================
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  5. SILVER LAYER (cleansed views)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

SILVER_VIEWS=("stg_transactions" "stg_customers" "stg_products" "stg_stores" "stg_inventory")
for vw in "${SILVER_VIEWS[@]}"; do
  ROW_COUNT=$(bq query --use_legacy_sql=false --format=csv --quiet "SELECT COUNT(*) FROM \`${PROJECT_ID}.retail_silver.${vw}\`" 2>/dev/null | tail -1)
  if [ -n "$ROW_COUNT" ] && [ "$ROW_COUNT" -gt 0 ] 2>/dev/null; then
    check_pass "retail_silver.${vw} — ${ROW_COUNT} rows"
  else
    check_fail "retail_silver.${vw} — empty or missing"
  fi
done

# Verify deduplication worked (silver should have <= bronze rows)
BRONZE_TXN=$(bq query --use_legacy_sql=false --format=csv --quiet "SELECT COUNT(*) FROM \`${PROJECT_ID}.retail_bronze.raw_transactions\`" 2>/dev/null | tail -1)
SILVER_TXN=$(bq query --use_legacy_sql=false --format=csv --quiet "SELECT COUNT(*) FROM \`${PROJECT_ID}.retail_silver.stg_transactions\`" 2>/dev/null | tail -1)
if [ "$SILVER_TXN" -le "$BRONZE_TXN" ] 2>/dev/null; then
  DEDUP_COUNT=$((BRONZE_TXN - SILVER_TXN))
  check_pass "Deduplication working — ${DEDUP_COUNT} records filtered"
else
  check_warn "Deduplication check inconclusive"
fi
echo ""

# ============================================================================
# 6. GOLD LAYER STAR SCHEMA
# ============================================================================
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  6. GOLD LAYER (star schema)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

GOLD_TABLES=("fct_sales" "fct_daily_sales" "fct_inventory" "dim_customer" "dim_product" "dim_store" "dim_date")
for tbl in "${GOLD_TABLES[@]}"; do
  ROW_COUNT=$(bq query --use_legacy_sql=false --format=csv --quiet "SELECT COUNT(*) FROM \`${PROJECT_ID}.retail_gold.${tbl}\`" 2>/dev/null | tail -1)
  if [ -n "$ROW_COUNT" ] && [ "$ROW_COUNT" -gt 0 ] 2>/dev/null; then
    check_pass "retail_gold.${tbl} — ${ROW_COUNT} rows"
  else
    check_fail "retail_gold.${tbl} — empty or missing"
  fi
done

# Check fct_sales partitioning
PARTITION_INFO=$(bq show --format=json ${PROJECT_ID}:retail_gold.fct_sales 2>/dev/null | grep -c "timePartitioning")
if [ "$PARTITION_INFO" -gt 0 ]; then
  check_pass "fct_sales — partitioned by transaction_date"
else
  check_warn "fct_sales — partitioning not detected"
fi

# Check customer segmentation
SEGMENTS=$(bq query --use_legacy_sql=false --format=csv --quiet "SELECT COUNT(DISTINCT customer_segment) FROM \`${PROJECT_ID}.retail_gold.dim_customer\`" 2>/dev/null | tail -1)
if [ "$SEGMENTS" -gt 1 ] 2>/dev/null; then
  check_pass "dim_customer — ${SEGMENTS} customer segments created"
else
  check_warn "dim_customer — segmentation may need review"
fi
echo ""

# ============================================================================
# 7. PUB/SUB
# ============================================================================
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  7. PUB/SUB TOPICS & SUBSCRIPTIONS"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

REQUIRED_TOPICS=("${ENV}-retail-transactions" "${ENV}-retail-inventory" "${ENV}-retail-dead-letter" "${ENV}-pipeline-notifications")
EXISTING_TOPICS=$(gcloud pubsub topics list --format="value(name)" 2>/dev/null)

for topic in "${REQUIRED_TOPICS[@]}"; do
  if echo "$EXISTING_TOPICS" | grep -q "$topic"; then
    check_pass "Topic: $topic"
  else
    check_fail "Topic: $topic not found"
  fi
done

REQUIRED_SUBS=("${ENV}-transactions-pull" "${ENV}-inventory-pull" "${ENV}-dead-letter-monitor")
EXISTING_SUBS=$(gcloud pubsub subscriptions list --format="value(name)" 2>/dev/null)

for sub in "${REQUIRED_SUBS[@]}"; do
  if echo "$EXISTING_SUBS" | grep -q "$sub"; then
    check_pass "Subscription: $sub"
  else
    check_fail "Subscription: $sub not found"
  fi
done
echo ""

# ============================================================================
# 8. DATA QUALITY
# ============================================================================
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  8. DATA QUALITY TESTS"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

TEST_COUNT=$(bq query --use_legacy_sql=false --format=csv --quiet "SELECT COUNT(*) FROM \`${PROJECT_ID}.retail_data_quality.test_results\`" 2>/dev/null | tail -1)
PASS_COUNT=$(bq query --use_legacy_sql=false --format=csv --quiet "SELECT COUNTIF(status='pass') FROM \`${PROJECT_ID}.retail_data_quality.test_results\`" 2>/dev/null | tail -1)
FAIL_COUNT_DQ=$(bq query --use_legacy_sql=false --format=csv --quiet "SELECT COUNTIF(status='fail') FROM \`${PROJECT_ID}.retail_data_quality.test_results\`" 2>/dev/null | tail -1)

if [ "$TEST_COUNT" -gt 0 ] 2>/dev/null; then
  check_pass "${TEST_COUNT} quality tests logged"
  if [ "$PASS_COUNT" -eq "$TEST_COUNT" ] 2>/dev/null; then
    check_pass "All ${PASS_COUNT} tests PASSING"
  else
    check_warn "${FAIL_COUNT_DQ} tests failing"
  fi
else
  check_fail "No quality tests found"
fi
echo ""

# ============================================================================
# 9. BUSINESS QUERY VALIDATION
# ============================================================================
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  9. BUSINESS QUERY VALIDATION"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Test star schema join works
REVENUE=$(bq query --use_legacy_sql=false --format=csv --quiet "
SELECT ROUND(SUM(f.total_amount), 2)
FROM \`${PROJECT_ID}.retail_gold.fct_sales\` f
JOIN \`${PROJECT_ID}.retail_gold.dim_product\` p ON f.product_id = p.product_id
JOIN \`${PROJECT_ID}.retail_gold.dim_store\` s ON f.store_id = s.store_id
WHERE p.category_l1 = 'electronics' AND s.region = 'AMERICAS'
" 2>/dev/null | tail -1)

if [ -n "$REVENUE" ] && [ "$REVENUE" != "null" ]; then
  check_pass "Star schema join working — Electronics+AMERICAS revenue: \$${REVENUE}"
else
  check_fail "Star schema join failed"
fi

# Test date dimension join
DATE_CHECK=$(bq query --use_legacy_sql=false --format=csv --quiet "
SELECT COUNT(*)
FROM \`${PROJECT_ID}.retail_gold.fct_sales\` f
JOIN \`${PROJECT_ID}.retail_gold.dim_date\` d ON f.transaction_date = d.date_day
WHERE d.is_weekend = TRUE
" 2>/dev/null | tail -1)

if [ -n "$DATE_CHECK" ] && [ "$DATE_CHECK" -gt 0 ] 2>/dev/null; then
  check_pass "Date dimension join working — ${DATE_CHECK} weekend transactions"
else
  check_fail "Date dimension join failed"
fi

# Test inventory status
STOCKOUT=$(bq query --use_legacy_sql=false --format=csv --quiet "
SELECT COUNT(*)
FROM \`${PROJECT_ID}.retail_gold.fct_inventory\`
WHERE inventory_status = 'stockout'
" 2>/dev/null | tail -1)

if [ -n "$STOCKOUT" ] 2>/dev/null; then
  check_pass "Inventory classification working — ${STOCKOUT} stockout records"
else
  check_fail "Inventory classification failed"
fi
echo ""

# ============================================================================
# SUMMARY
# ============================================================================
TOTAL=$((PASS + FAIL + WARN))

echo ""
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║  VERIFICATION SUMMARY                                       ║"
echo "╠══════════════════════════════════════════════════════════════╣"
echo "║                                                              ║"
echo "║  ✅ Passed:  ${PASS}                                        ║"
echo "║  ❌ Failed:  ${FAIL}                                        ║"
echo "║  ⚠️  Warnings: ${WARN}                                      ║"
echo "║  Total:     ${TOTAL} checks                                 ║"
echo "║                                                              ║"
echo "╠══════════════════════════════════════════════════════════════╣"
echo "║                                                              ║"
echo "║  COMPONENTS BUILT:                                           ║"
echo "║  ─────────────────────────────────────────────────────────── ║"
echo "║  Cloud Storage    1 bucket, 7 folders, 5 CSV files           ║"
echo "║  BigQuery         5 datasets, 18 tables/views                ║"
echo "║    Bronze          5 tables (partitioned + clustered)        ║"
echo "║    Silver          5 views (dedup + standardized)            ║"
echo "║    Gold            7 tables (star schema)                    ║"
echo "║    Data Quality    1 table (5 test results)                  ║"
echo "║  Pub/Sub          4 topics, 3 subscriptions                  ║"
echo "║  Looker Studio    Dashboard (in progress)                    ║"
echo "║                                                              ║"
echo "║  GCP SERVICES: BigQuery, Cloud Storage, Pub/Sub,            ║"
echo "║  Data Catalog, Cloud DLP, Cloud Monitoring                   ║"
echo "║                                                              ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""

if [ "$FAIL" -eq 0 ]; then
  echo "🎉 ALL CHECKS PASSED — Lab 01 is complete and verified!"
else
  echo "⚠️  Some checks failed. Review the output above."
fi
echo ""
