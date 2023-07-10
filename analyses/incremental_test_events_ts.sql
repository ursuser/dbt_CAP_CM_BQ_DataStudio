{{
    config
    (
        materialized="incremental", 
        unique_key="session_id"
    )
}}

select
    event_date,
    user_pseudo_id,
    device.category,
    platform,
    geo.city,
    concat(
        user_pseudo_id,
        (select value.int_value from unnest(event_params) where key = 'ga_session_id')
    ) session_id,
    countif(event_name = 'session_start') as sessions
from {{ source("analytics_293084740", "events_202*") }}
group by
    event_date,
    user_pseudo_id,
    session_id,
    device.category,
    platform,
    geo.city

{% if is_incremental() %}
    where
        _table_suffix > (select max(_table_suffix) from {{ this }})
{% endif %}