   {{
        config(
            materialized="table",
            partition_by={
                "field": "event_date",
                "data_type": "date",
                "granularity": "day",
            },
        )
    }}


with
    variables as (select 30 as conversion_period, 30 as attribution_window),

    sessions as (
        select
            min(event_timestamp) session_start_timestamp,
            user_pseudo_id,

            concat(
                user_pseudo_id,
                '.',
                (
                    select value.int_value
                    from unnest(event_params)
                    where key = 'ga_session_id'
                )
            ) as real_session_id
        from {{ source("analytics_293084740", "events") }}, variables
        where
            regexp_extract(_table_suffix, '[0-9]+') between format_date(
                '%Y%m%d', date_sub(current_date(), interval attribution_window day)
            ) and format_date('%Y%m%d', date_sub(current_date(), interval 1 day))

        group by user_pseudo_id, real_session_id
    ),


    sessions_source as (
        select 
            session_start_timestamp,
            t1.user_pseudo_id,
            t1.real_session_id,
            real_session_sm
         from sessions as t1
    left join {{ref('int_session_real_sm')}} as t2
    on t1.real_session_id = t2.real_session_id
    and t1.session_start_timestamp = t2.event_timestamp
    ),

    events_data as (
        select
            *,

            lag(event_timestamp) over (
                partition by user_pseudo_id order by event_timestamp
            ) as previous_event_timestamp,

        from
            (
                select
                    event_timestamp,
                    event_name,
                    user_pseudo_id,

                    concat(
                        user_pseudo_id,
                        '.',
                        (
                            select value.int_value
                            from unnest(event_params)
                            where key = 'ga_session_id'
                        )
                    ) as ga_session_id,

                from {{ source("analytics_293084740", "events") }}, variables
                where
                    regexp_extract(_table_suffix, '[0-9]+')
                    between format_date(
                        '%Y%m%d',
                        date_sub(current_date(), interval conversion_period day)
                    ) and format_date(
                        '%Y%m%d', date_sub(current_date(), interval 1 day)
                    )
                    and event_name in ('purchase')
                order by user_pseudo_id, event_timestamp
            )
    ),
   mcf as (
        select
            extract(
                date from timestamp_micros(event_timestamp) at time zone 'Europe/Kiev'  -- тут потом хорошо бы сверить с базовой таблицей, если нет перекосов
            ) event_date,
            event_timestamp,
            events_data.event_name as event_name,
            events_data.user_pseudo_id as user_pseudo_id,
            events_data.ga_session_id as event_ga_session_id,
            sessions_source.real_session_id as sessions_ga_session_id,
            sessions_source.real_session_sm as source_medium
        from events_data

        left join
            sessions_source
            on events_data.user_pseudo_id = sessions_source.user_pseudo_id
            and events_data.event_timestamp >= sessions_source.session_start_timestamp
            and (
                events_data.previous_event_timestamp is null
                or events_data.previous_event_timestamp
                < sessions_source.session_start_timestamp
                or events_data.ga_session_id = sessions_source.real_session_id
            )
        order by events_data.user_pseudo_id, sessions_source.session_start_timestamp asc
    ),

final as    

(select
    event_date,
    event_timestamp,
    user_pseudo_id,
    -- sm,
    array_to_string(sm, ' > ') as user_mcf_route,
    array_length(sm) as touches,
    sm[offset(0)] as first_click,
    array_reverse(sm)[offset(0)] as last_click,

    case
        when
            not exists (
                select element
                from unnest(sm) as element
                where element != '(direct) / (none)'
            )
        then '(direct) / (none)'
        else
            (
                select element
                from unnest(array_reverse(sm)) as element
                where element != '(direct) / (none)'
                limit 1
            )
    end as last_non_direct_click,

    coalesce(
        (
            select element
            from unnest(array_reverse(sm)) as element
            where element = 'google / cpc'
            limit 1
        ),
        (
            select element
            from unnest(array_reverse(sm)) as element
            where
                element
                not in ('(direct) / (none)', 'google / organic', 'bing / organic')
            limit 1
        ),
        '(direct) / (none)'
    ) as last_ad_click

from
    (
        select
            event_date, event_timestamp, user_pseudo_id, array_agg(source_medium) as sm
        from mcf
        group by event_date, event_timestamp, user_pseudo_id
    ))


    select * from final

