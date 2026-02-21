#!/bin/bash
###############################################################################
# Step 11: Run Data Quality Tests
# Inserts test results into retail_data_quality.test_results for auditing.
###############################################################################

export PROJECT_ID="${PROJECT_ID:?Set PROJECT_ID}"

bq query --use_legacy_sql=false "
INSERT INTO \`${PROJECT_ID}.retail_data_quality.test_results\`
(run_date, test_name, model_name, status, failures, rows_tested, severity)

SELECT CURRENT_DATE(), 'not_null_total_amount', 'fct_sales',
  CASE WHEN COUNTIF(total_amount IS NULL) = 0 THEN 'pass' ELSE 'fail' END,
  COUNTIF(total_amount IS NULL), COUNT(*), 'ERROR'
FROM \`${PROJECT_ID}.retail_gold.fct_sales\`

UNION ALL
SELECT CURRENT_DATE(), 'unique_customer_id', 'dim_customer',
  CASE WHEN COUNT(*) = COUNT(DISTINCT customer_id) THEN 'pass' ELSE 'fail' END,
  COUNT(*) - COUNT(DISTINCT customer_id), COUNT(*), 'ERROR'
FROM \`${PROJECT_ID}.retail_gold.dim_customer\`

UNION ALL
SELECT CURRENT_DATE(), 'accepted_values_region', 'fct_sales',
  CASE WHEN COUNTIF(store_region NOT IN ('AMERICAS','EMEA','APAC')) = 0 THEN 'pass' ELSE 'fail' END,
  COUNTIF(store_region NOT IN ('AMERICAS','EMEA','APAC')), COUNT(*), 'ERROR'
FROM \`${PROJECT_ID}.retail_gold.fct_sales\`

UNION ALL
SELECT CURRENT_DATE(), 'accepted_values_channel', 'fct_sales',
  CASE WHEN COUNTIF(channel NOT IN ('online','in_store','mobile_app')) = 0 THEN 'pass' ELSE 'fail' END,
  COUNTIF(channel NOT IN ('online','in_store','mobile_app')), COUNT(*), 'ERROR'
FROM \`${PROJECT_ID}.retail_gold.fct_sales\`

UNION ALL
SELECT CURRENT_DATE(), 'unique_product_id', 'dim_product',
  CASE WHEN COUNT(*) = COUNT(DISTINCT product_id) THEN 'pass' ELSE 'fail' END,
  COUNT(*) - COUNT(DISTINCT product_id), COUNT(*), 'ERROR'
FROM \`${PROJECT_ID}.retail_gold.dim_product\`
"

echo ""
echo "Test results:"
bq query --use_legacy_sql=false --format=pretty "
SELECT test_name, model_name, status, failures, rows_tested
FROM \`${PROJECT_ID}.retail_data_quality.test_results\`
WHERE run_date = CURRENT_DATE()
ORDER BY test_name
"
echo "✅ Quality tests logged"
