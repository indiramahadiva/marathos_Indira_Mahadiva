CREATE OR REFRESH MATERIALIZED VIEW marathos.gold.dim_date
  COMMENT "Date dimension - generated daily range 2000-2030" AS
WITH date_range AS (
  SELECT explode(sequence(DATE'2000-01-01', DATE'2030-12-31', INTERVAL 1 DAY)) AS date
)
SELECT
  CAST(date_format(date, 'yyyyMMdd') AS INT) AS date_id,
  date,
  year(date)                AS year,
  month(date)               AS month,
  day(date)                 AS day,
  date_format(date, 'EEEE') AS weekday,
  quarter(date)             AS quarter
FROM date_range;