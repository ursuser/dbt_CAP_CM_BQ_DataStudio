{{config(
    materialized = 'incremental',
    unique_key = 'session_id'
) }}

select
    event_date,
    concat(
        user_pseudo_id,
        (select value.int_value from unnest(event_params) where key = 'ga_session_id')
    ) as session_id,
    countif(event_name = 'page_view') as pages,
from {{ source("analytics_293084740", "events_202*") }}
group by
    event_date,
    session_id
    
{% if is_incremental() %}
    WHERE event_date > (SELECT MAX(event_date) FROM {{ this }})
{% endif %}