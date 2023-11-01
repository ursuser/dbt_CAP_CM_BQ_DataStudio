with
    raw_data as (
        select
            event_date,
            user_pseudo_id,
            concat(
                user_pseudo_id,
                (
                    select value.int_value
                    from unnest(event_params)
                    where key = 'ga_session_id'
                )
            ) as session_id,
        from {{ source("analytics_293084740", "events") }}
        group by event_date, user_pseudo_id, session_id
    )

select
    event_date,
    count(distinct user_pseudo_id) as users,
    count(distinct session_id) as sessions,
from raw_data
group by event_date
order by event_date desc
