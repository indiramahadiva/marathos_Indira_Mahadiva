CREATE OR REFRESH MATERIALIZED VIEW marathos.gold.dim_country
  COMMENT "Country dimension - IOC code lookup with continent" AS
SELECT
  xxhash64(country_code) AS country_id,
  country_code,
  country_name,
  continent
FROM marathos.bronze.raw_country;