CREATE OR REFRESH MATERIALIZED VIEW marathos.gold.dim_athlete
  COMMENT "Athlete dimension - one row per unique athlete" AS
SELECT
  a.athlete_id,
  MAX_BY(a.athlete_gender,        a.event_start_date) AS athlete_gender,
  MAX_BY(a.athlete_age_category,  a.event_start_date) AS athlete_age_category,
  MAX_BY(a.athlete_year_of_birth, a.event_start_date) AS athlete_year_of_birth,
  MAX_BY(a.athlete_country,       a.event_start_date) AS athlete_country,
  MAX_BY(c.country_id,            a.event_start_date) AS country_id
FROM marathos.silver.marathon_obt a
LEFT JOIN marathos.gold.dim_country c
  ON a.athlete_country = c.country_code
GROUP BY a.athlete_id;