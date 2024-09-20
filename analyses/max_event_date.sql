SELECT
  MAX(event_date) AS max_date
FROM
  { { source ('analytics_293084740', 'events') } } -- just to test github connection
