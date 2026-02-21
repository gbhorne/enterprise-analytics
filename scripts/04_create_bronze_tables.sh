#!/bin/bash
###############################################################################
# Step 4: Create Bronze Layer Tables
# Partitioned by date + clustered by frequently-filtered columns.
# Every table includes _ingested_at and _source_system for lineage.
###############################################################################

export PROJECT_ID="${PROJECT_ID:?Set PROJECT_ID}"

# Transactions (partitioned by date, clustered by store+customer)
bq mk --table \
  --time_partitioning_field transaction_date \
  --time_partitioning_type DAY \
  --clustering_fields store_id,customer_id \
  --description "Raw POS transaction data. Grain: one row per line item." \
  ${PROJECT_ID}:retail_bronze.raw_transactions \
  transaction_id:STRING,transaction_date:DATE,transaction_timestamp:TIMESTAMP,store_id:STRING,customer_id:STRING,product_id:STRING,quantity:INT64,unit_price:NUMERIC,discount_amount:NUMERIC,tax_amount:NUMERIC,total_amount:NUMERIC,payment_method:STRING,channel:STRING,currency_code:STRING,_ingested_at:TIMESTAMP,_source_system:STRING,_source_file:STRING \
  2>/dev/null || echo "raw_transactions exists"

# Customers (contains PII)
bq mk --table \
  --clustering_fields customer_id \
  --description "Raw customer master data from CRM. Contains PII fields." \
  ${PROJECT_ID}:retail_bronze.raw_customers \
  customer_id:STRING,first_name:STRING,last_name:STRING,email:STRING,phone:STRING,date_of_birth:DATE,gender:STRING,address_line1:STRING,city:STRING,state_province:STRING,postal_code:STRING,country_code:STRING,loyalty_tier:STRING,signup_date:DATE,last_activity_date:DATE,is_active:BOOLEAN,marketing_opt_in:BOOLEAN,_ingested_at:TIMESTAMP,_source_system:STRING \
  2>/dev/null || echo "raw_customers exists"

# Products
bq mk --table \
  --clustering_fields category_l1,brand \
  --description "Raw product catalog from MDM system." \
  ${PROJECT_ID}:retail_bronze.raw_products \
  product_id:STRING,product_name:STRING,category_l1:STRING,category_l2:STRING,category_l3:STRING,brand:STRING,supplier_id:STRING,unit_cost:NUMERIC,list_price:NUMERIC,weight_kg:FLOAT64,is_active:BOOLEAN,launch_date:DATE,discontinue_date:DATE,_ingested_at:TIMESTAMP,_source_system:STRING \
  2>/dev/null || echo "raw_products exists"

# Stores
bq mk --table \
  --description "Raw store location and metadata." \
  ${PROJECT_ID}:retail_bronze.raw_stores \
  store_id:STRING,store_name:STRING,store_type:STRING,address:STRING,city:STRING,state_province:STRING,country_code:STRING,region:STRING,timezone:STRING,square_footage:INT64,open_date:DATE,close_date:DATE,is_active:BOOLEAN,manager_name:STRING,_ingested_at:TIMESTAMP,_source_system:STRING \
  2>/dev/null || echo "raw_stores exists"

# Inventory (partitioned by snapshot date)
bq mk --table \
  --time_partitioning_field snapshot_date \
  --time_partitioning_type DAY \
  --clustering_fields store_id,product_id \
  --description "Daily inventory snapshots by store and product." \
  ${PROJECT_ID}:retail_bronze.raw_inventory \
  snapshot_date:DATE,store_id:STRING,product_id:STRING,stock_on_hand:INT64,stock_on_order:INT64,reorder_point:INT64,last_received_date:DATE,last_sold_date:DATE,_ingested_at:TIMESTAMP,_source_system:STRING \
  2>/dev/null || echo "raw_inventory exists"

# Data quality tracking
bq mk --table \
  --time_partitioning_field run_date \
  --time_partitioning_type DAY \
  --description "Data quality test results for audit and monitoring." \
  ${PROJECT_ID}:retail_data_quality.test_results \
  run_date:DATE,test_name:STRING,model_name:STRING,status:STRING,failures:INT64,rows_tested:INT64,execution_time_seconds:FLOAT64,severity:STRING \
  2>/dev/null || echo "test_results exists"

echo "✅ Bronze tables created"
bq ls ${PROJECT_ID}:retail_bronze
