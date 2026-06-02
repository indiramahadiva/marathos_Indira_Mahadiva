CREATE OR REFRESH MATERIALIZED VIEW marathos.gold.dim_event
  COMMENT "Event dimension - one row per unique race name" AS
SELECT
  event_id,
  MAX_BY(event_name,                event_start_date) AS event_name,
  MAX_BY(event_type,                event_start_date) AS event_type,
  MAX_BY(event_unit,                event_start_date) AS event_unit,
  MAX_BY(event_value,               event_start_date) AS event_value,
  MAX_BY(event_number_of_finishers, event_start_date) AS number_of_finishers
FROM marathos.silver.marathon_obt
GROUP BY event_id;