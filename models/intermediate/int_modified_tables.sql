with
    tables as (
        select
            split(table_id, "_")[offset(1)] as table_name,
            extract(
                datetime
                from timestamp_millis(last_modified_time) at time zone 'Europe/Kiev'
            ) as last_modified
        from `cap-cm-md.analytics_293084740.__TABLES__`
        where table_id like 'events_20%'
    ),

    final as (
        select parse_date('%Y%m%d', table_name) as table_modified_date
        from tables
        where
            last_modified
            > datetime_sub(current_datetime('Europe/Kiev'), interval 5450 minute)
    )  -- здесь на проде нужно выставить 1450 мин

select *
from final
