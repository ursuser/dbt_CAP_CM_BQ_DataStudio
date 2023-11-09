{% set partitions_to_replace = [
    "date_sub(current_date, interval 1 day)",
    "date_sub(current_date, interval 2 day)",
    "date_sub(current_date, interval 3 day)",
] %}

{{
    config(
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        partition_by={"field": "event_date", "data_type": "date"},
        partitions=partitions_to_replace,
    )
}}


with
    ga4_source as (
        select *
        from {{ source("analytics_293084740", "events") }}

        {% if is_incremental() %}
            where
                parse_date('%Y%m%d', regexp_extract(_table_suffix, '[0-9]+'))
                in ({{ partitions_to_replace | join(",") }})
        {% endif %}

    ),

    prev_prep as (
        select
            parse_date('%Y%m%d', event_date) as event_date,
            event_timestamp,
            user_pseudo_id,
            (
                select value.int_value
                from unnest(event_params)
                where key = 'ga_session_id'
            ) as session_id,
            (
                select value.string_value from unnest(event_params) where key = 'source'
            ) as session_source,
            (
                select value.string_value from unnest(event_params) where key = 'medium'
            ) as session_medium,
            (
                select value.string_value
                from unnest(event_params)
                where key = 'campaign'
            ) as session_campaign,
            (
                select value.string_value
                from unnest(event_params)
                where key = 'page_location'
            ) as page_location,
            (
                select value.string_value
                from unnest(event_params)
                where key = 'page_referrer'
            ) as page_referrer,

            (
                select value.int_value
                from unnest(event_params)
                where key = 'ga_session_number'
            ) as session_number

        from ga4_source
        group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
    ),

    def_prep as (
        select
            * except (
                session_source,
                page_location,
                session_medium,
                session_campaign,
                page_referrer
            ),

            concat(user_pseudo_id, '.', session_id) as real_session_id,

            first_value(session_source ignore nulls) over (
                partition by user_pseudo_id, session_id
                order by event_timestamp
                rows between unbounded preceding and unbounded following
            ) as session_source,

            first_value(session_medium ignore nulls) over (
                partition by user_pseudo_id, session_id
                order by event_timestamp
                rows between unbounded preceding and unbounded following
            ) as session_medium,

            first_value(session_campaign ignore nulls) over (
                partition by user_pseudo_id, session_id
                order by event_timestamp
                rows between unbounded preceding and unbounded following
            ) as session_campaign,

            coalesce(
                max(
                    case when page_location like '%utm_source%' then page_location end
                ) over (
                    partition by user_pseudo_id, session_id
                    order by event_timestamp
                    rows between unbounded preceding and unbounded following
                ),
                first_value(page_location ignore nulls) over (
                    partition by user_pseudo_id, session_id
                    order by event_timestamp
                    rows between unbounded preceding and unbounded following
                )
            ) as page_location,

            first_value(page_referrer ignore nulls) over (
                partition by user_pseudo_id, session_id
                order by event_timestamp
                rows between unbounded preceding and unbounded following
            ) as page_referrer

        from prev_prep
    ),

    final as (
        select
            *,

            case
                when page_location like '%utm_source%'
                then regexp_extract(page_location, r'utm_source=([^&]+)')
                when page_location like '%gclid%'
                then 'google'
                when page_location like '%fbclid%'
                then 'facebook'
                when session_source is null
                then '(direct)'
                else session_source
            end as real_session_source,

            case
                when page_location like '%utm_medium%'
                then regexp_extract(page_location, r'utm_medium=([^&]+)')
                when page_location like '%gclid%'
                then 'cpc'
                when page_location like '%fbclid%'
                then 'social'
                when session_medium is null
                then '(none)'
                else session_medium
            end as real_session_medium,

            case
                when page_location like '%utm_campaign%'
                then regexp_extract(page_location, r'utm_campaign=([^&]+)')
                when page_location like '%gclid%'
                then 'GAds_Campaign'
                when page_location like '%fbclid%'
                then '(none)'
                when session_campaign is null
                then '(none)'
                else session_campaign
            end as real_session_campaign

        from def_prep
    )


select
    event_date,
    event_timestamp,
    user_pseudo_id,
    real_session_id,
    max(concat(real_session_source, ' / ', real_session_medium)) as real_session_sm,
    max(real_session_campaign) as real_session_campaign
from final
group by 1,2,3,4
