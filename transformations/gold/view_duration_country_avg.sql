CREATE OR REFRESH MATERIALIZED VIEW marathos.gold.view_duration_country_avg
  COMMENT "Duration events: avg distance covered per country and race length" AS
SELECT
  c.country_code,
  c.country_name,
  c.continent,
  e.event_value AS hours,
  COUNT(*)               AS runners,
  AVG(f.performance_km)  AS avg_km,
  MAX(f.performance_km)  AS best_km
FROM marathos.gold.fct_results f
JOIN marathos.gold.dim_event   e USING (event_id)
JOIN marathos.gold.dim_country c USING (country_id)
WHERE e.event_type = 'duration'
GROUP BY c.country_code, c.country_name, c.continent, e.event_value
ORDER BY runners DESC;