{{ config
    (
        materialized="incremental", 
        unique_key="session_id"
    ) 
}}

select event_date, session_id, pages
from `cap-cm-md.testing_dataset.ga4_sessions`

{% if is_incremental() %}
    where
        event_timestamp > (select max(event_timestamp) from {{ this }})
        or (
            event_timestamp = (select max(event_timestamp) from {{ this }})
            and event_date > (select max(event_date) from {{ this }})
        )
{% endif %}
