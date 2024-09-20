select 
    max(event_date) as max_date
from
    {{ source("analytics_293084740", "events") }}
    
