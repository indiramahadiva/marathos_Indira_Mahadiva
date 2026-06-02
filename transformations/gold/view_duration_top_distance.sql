CREATE OR REFRESH MATERIALIZED VIEW marathos.gold.view_duration_top_distance
  COMMENT "Duration events: top athletes by distance covered" AS
SELECT
  a.athlete_id,
  a.athlete_gender,
  c.country_code,
  c.country_name,
  e.event_name,
  e.event_value AS hours,
  f.performance_km,
  d.year
FROM marathos.gold.fct_results f
JOIN marathos.gold.dim_event   e USING (event_id)
JOIN marathos.gold.dim_athlete a USING (athlete_id)
JOIN marathos.gold.dim_country c USING (country_id)
JOIN marathos.gold.dim_date    d USING (date_id)
WHERE e.event_type = 'duration'
ORDER BY f.performance_km DESC
LIMIT 200;