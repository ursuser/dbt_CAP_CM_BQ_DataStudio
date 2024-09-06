with
    ga4_source as (select * from {{ source("analytics_293084740", "events") }}),

    user_id_mapping as (
        select user_pseudo_id, max(user_id) as user_id
        from ga4_source
        group by user_pseudo_id
        having user_id is not null
    )

select *
from user_id_mapping
