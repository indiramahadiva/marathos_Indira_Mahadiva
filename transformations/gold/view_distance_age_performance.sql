CREATE OR REFRESH MATERIALIZED VIEW marathos.gold.view_distance_age_performance
  COMMENT "Distance events: avg performance by age, by event distance" AS
SELECT
  f.athlete_age,
  e.event_value AS distance_value,
  e.event_unit,
  COUNT(*)                              AS runners,
  AVG(f.performance_seconds) / 3600.0   AS avg_hours,
  AVG(f.athlete_avg_speed_kmh)          AS avg_speed_kmh
FROM marathos.gold.fct_results f
JOIN marathos.gold.dim_event e USING (event_id)
WHERE e.event_type = 'distance'
  AND f.athlete_age BETWEEN 18 AND 90
GROUP BY f.athlete_age, e.event_value, e.event_unit
ORDER BY f.athlete_age;