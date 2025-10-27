SELECT '{tbl}' AS table_name, COUNT(*) AS n FROM `{os.environ['GOOGLE_CLOUD_PROJECT']}.netflix.{tbl}`

SELECT 'users' AS table_name, COUNT(*) AS n FROM `{os.environ['GOOGLE_CLOUD_PROJECT']}.netflix.users`
UNION ALL
SELECT 'movies' AS table_name, COUNT(*) AS n FROM `{os.environ['GOOGLE_CLOUD_PROJECT']}.netflix.movies`
UNION ALL
SELECT 'watch_history' AS table_name, COUNT(*) AS n FROM `{os.environ['GOOGLE_CLOUD_PROJECT']}.netflix.watch_history`
UNION ALL
SELECT 'recommendation_logs' AS table_name, COUNT(*) AS n FROM `{os.environ['GOOGLE_CLOUD_PROJECT']}.netflix.recommendation_logs`
UNION ALL
SELECT 'search_logs' AS table_name, COUNT(*) AS n FROM `{os.environ['GOOGLE_CLOUD_PROJECT']}.netflix.search_logs`
UNION ALL
SELECT 'reviews' AS table_name, COUNT(*) AS n FROM `{os.environ['GOOGLE_CLOUD_PROJECT']}.netflix.reviews`


WITH base AS (
  SELECT COUNT(*) AS n,
         COUNTIF(country IS NULL) AS miss_country,
         COUNTIF(subscription_plan IS NULL) AS miss_subscription_plan,
         COUNTIF(age IS NULL) AS miss_age
  FROM `{os.environ['GOOGLE_CLOUD_PROJECT']}.netflix.users`
)
SELECT n,
       ROUND(100*miss_country/n,2) AS pct_missing_country,
       ROUND(100*miss_subscription_plan/n,2) AS pct_missing_subscription_plan,
       ROUND(100*miss_age/n,2) AS pct_missing_age
FROM base;

SELECT country,
       COUNT(*) AS n,
       ROUND(100*COUNTIF(subscription_plan IS NULL)/COUNT(*),2) AS pct_missing_subscription_plan
FROM `{os.environ['GOOGLE_CLOUD_PROJECT']}.netflix.users`
GROUP BY country
ORDER BY pct_missing_subscription_plan DESC;

WITH base AS (
  SELECT COUNT(*) n,
         COUNTIF(country IS NULL) miss_country,
         COUNTIF(subscription_plan IS NULL) miss_plan,
         COUNTIF(age IS NULL) miss_age
  FROM `{project}.netflix.users`
)
SELECT ROUND(100*miss_country/n,2) AS pct_missing_country,
       ROUND(100*miss_plan/n,2) AS pct_missing_plan,
       ROUND(100*miss_age/n,2) AS pct_missing_age
FROM base;

SELECT user_id, movie_id, watch_date, device_type, COUNT(*) AS dup_count
FROM `{project}.netflix.watch_history`
GROUP BY user_id, movie_id, watch_date, device_type
HAVING dup_count > 1
ORDER BY dup_count DESC
LIMIT 20

CREATE OR REPLACE TABLE `{project}.netflix.watch_history_dedup` AS
SELECT * EXCEPT(rk) FROM (
  SELECT h.*,
         ROW_NUMBER() OVER (
           PARTITION BY user_id, movie_id, {timestamp_col}, device_type
           ORDER BY {timestamp_col} DESC
         ) AS rk
  FROM `{project}.netflix.watch_history` h
)
WHERE rk = 1

SELECT 'watch_history_raw' AS table_name, COUNT(*) AS row_count
FROM `{project}.netflix.watch_history`
UNION ALL
SELECT 'watch_history_dedup' AS table_name, COUNT(*) AS row_count
FROM `{project}.netflix.watch_history_dedup`

WITH dist AS (
  SELECT
    APPROX_QUANTILES(watch_duration_minutes, 4)[OFFSET(1)] AS q1,
    APPROX_QUANTILES(watch_duration_minutes, 4)[OFFSET(3)] AS q3
  FROM `{project}.netflix.watch_history_dedup`
),
bounds AS (
  SELECT q1, q3, (q3-q1) AS iqr,
         q1 - 1.5*(q3-q1) AS lo,
         q3 + 1.5*(q3-q1) AS hi
  FROM dist
)
SELECT
  COUNTIF(h.watch_duration_minutes < b.lo OR h.watch_duration_minutes > b.hi) AS outliers,
  COUNT(*) AS total,
  ROUND(100*COUNTIF(h.watch_duration_minutes < b.lo OR h.watch_duration_minutes > b.hi)/COUNT(*),2) AS pct_outliers
FROM `{project}.netflix.watch_history_dedup` h
CROSS JOIN bounds b;

CREATE OR REPLACE TABLE `{project}.netflix.watch_history_robust` AS
SELECT *
FROM `{project}.netflix.watch_history_dedup`
WHERE watch_duration_minutes BETWEEN 0 AND 500;  -- example: keep only reasonable values


SELECT
  'Before Capping' AS source,
  MIN(watch_duration_minutes) AS min_duration,
  APPROX_QUANTILES(watch_duration_minutes, 2)[OFFSET(1)] AS median_duration,
  MAX(watch_duration_minutes) AS max_duration
FROM `{project}.netflix.watch_history_dedup`
UNION ALL
SELECT
  'After Capping' AS source,
  MIN(watch_duration_minutes) AS min_duration,
  APPROX_QUANTILES(watch_duration_minutes, 2)[OFFSET(1)] AS median_duration,
  MAX(watch_duration_minutes) AS max_duration
FROM `{project}.netflix.watch_history_robust`;

SELECT
  COUNTIF(watch_duration_minutes > 8*60) AS sessions_over_8h,
  COUNT(*) AS total,
  ROUND(100*COUNTIF(watch_duration_minutes > 8*60)/COUNT(*),2) AS pct
FROM `{project}.netflix.watch_history_robust`;

SELECT
  COUNTIF(age < 10 OR age > 100) AS extreme_age_rows,
  COUNT(*) AS total,
  ROUND(100*COUNTIF(age < 10 OR age > 100)/COUNT(*),2) AS pct
FROM `{project}.netflix.users`;

SELECT
  COUNTIF(duration_minutes < 15) AS titles_under_15m,
  COUNTIF(duration_minutes > 8*60) AS titles_over_8h,
  COUNT(*) AS total,
  ROUND(100*COUNTIF(duration_minutes < 15)/COUNT(*),2) AS pct_under_15m,
  ROUND(100*COUNTIF(duration_minutes > 8*60)/COUNT(*),2) AS pct_over_8h
FROM `{project}.netflix.movies`;

SELECT 'flag_binge' AS flag_name,
       ROUND(100*COUNTIF(watch_duration_minutes > 8*60)/COUNT(*),2) AS pct_of_rows
FROM `{project}.netflix.watch_history_robust`

UNION ALL
SELECT 'flag_age_extreme' AS flag_name,
       ROUND(100*COUNTIF(age < 10 OR age > 100)/COUNT(*),2) AS pct_of_rows
FROM `{project}.netflix.users`

UNION ALL
SELECT 'flag_duration_anomaly_under_15m' AS flag_name,
       ROUND(100*COUNTIF(duration_minutes < 15)/COUNT(*),2) AS pct_of_rows
FROM `{project}.netflix.movies`

UNION ALL
SELECT 'flag_duration_anomaly_over_8h' AS flag_name,
       ROUND(100*COUNTIF(duration_minutes > 8*60)/COUNT(*),2) AS pct_of_rows
FROM `{project}.netflix.movies`;

