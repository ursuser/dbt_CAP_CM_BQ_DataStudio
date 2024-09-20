{ { config (
  materialized = "incremental",
  unique_key = "session_id"
) } }
SELECT
  event_date,
  user_pseudo_id,
  device.category,
  platform,
  geo.city,
  CONCAT(
    user_pseudo_id,
    (
      SELECT
        value.int_value
      FROM
        UNNEST (event_params)
      WHERE
        key = 'ga_session_id'
    )
  ) session_id,
  COUNTIF(event_name = 'session_start') AS sessions
FROM
  { { source ("analytics_293084740", "events") } }
GROUP BY
  event_date,
  user_pseudo_id,
  session_id,
  device.category,
  platform,
  geo.city { % IF is_incremental () % }
WHERE
  _table_suffix > (
    SELECT
      MAX(_table_suffix)
    FROM
      { { this } }
  ) { % endif % }
