SELECT
  EXTRACT(HOUR
  FROM
    TIMESTAMP_MICROS(event_timestamp) AT TIME ZONE 'Europe/Chisinau') AS hour,
  COUNT(user_pseudo_id) AS users,
  COUNTIF(event_name = 'add_to_cart') AS add_to_cart,
  COUNTIF(event_name = 'purchase') AS purchase
FROM 
  `cap-cm-md`.`analytics_293084740`.`events_202*`
GROUP BY
  hour
ORDER BY
  users DESC