SELECT
  MAX(event_date)
FROM 
  {{source('analytics_293084740', 'events_202*')}}
