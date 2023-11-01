select
    extract(
        hour from timestamp_micros(event_timestamp) at time zone 'Europe/Chisinau'
    ) as hour,
    count(user_pseudo_id) as users,
    countif(event_name = 'add_to_cart') as add_to_cart,
    countif(event_name = 'purchase') as purchase
from {{ source("analytics_293084740", "events") }}
group by hour
order by users desc
