-- Gold Layer: dim_date
-- 8-year calendar dimension (2020-2027).

CREATE OR REPLACE TABLE `${PROJECT_ID}.retail_gold.dim_date` AS
WITH dates AS (
    SELECT date_day FROM UNNEST(GENERATE_DATE_ARRAY('2020-01-01', '2027-12-31')) AS date_day
)
SELECT
    date_day,
    FORMAT_DATE('%Y%m%d', date_day) AS date_key,
    EXTRACT(YEAR FROM date_day) AS year,
    EXTRACT(QUARTER FROM date_day) AS quarter,
    EXTRACT(MONTH FROM date_day) AS month,
    EXTRACT(WEEK FROM date_day) AS week_of_year,
    EXTRACT(DAYOFWEEK FROM date_day) AS day_of_week,
    FORMAT_DATE('%B', date_day) AS month_name,
    FORMAT_DATE('%A', date_day) AS day_name,
    CASE WHEN EXTRACT(DAYOFWEEK FROM date_day) IN (1, 7) THEN TRUE ELSE FALSE END AS is_weekend
FROM dates;
