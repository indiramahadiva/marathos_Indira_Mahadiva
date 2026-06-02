CREATE OR REFRESH MATERIALIZED VIEW marathos.gold.view_distance_top_countries
  COMMENT "Distance events: top countries by number of finishers, avg time" AS
SELECT
  c.country_code,
  c.country_name,
  c.continent,
  e.event_value AS distance_value,
  e.event_unit,
  COUNT(*)                              AS finishers,
  AVG(f.performance_seconds) / 3600.0   AS avg_hours,
  MIN(f.performance_seconds) / 3600.0   AS fastest_hours
FROM marathos.gold.fct_results f
JOIN marathos.gold.dim_event   e USING (event_id)
JOIN marathos.gold.dim_country c USING (country_id)
WHERE e.event_type = 'distance'
GROUP BY c.country_code, c.country_name, c.continent, e.event_value, e.event_unit
ORDER BY finishers DESC;