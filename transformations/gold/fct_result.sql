CREATE OR REFRESH MATERIALIZED VIEW marathos.gold.fct_results
  COMMENT "Fact table - one row per athlete-event result" AS
SELECT
  s.result_id,
  s.event_id,
  s.athlete_id,
  s.date_id,
  c.country_id,
  s.performance_seconds,
  s.performance_km,
  s.athlete_avg_speed_kmh,
  s.athlete_age
FROM marathos.silver.marathon_obt s
LEFT JOIN marathos.gold.dim_country c
  ON s.athlete_country = c.country_code;