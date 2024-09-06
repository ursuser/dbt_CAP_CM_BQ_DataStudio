with
    tables as (
        select
            split(table_id, "_")[offset(1)] as table_name,
            extract(
                datetime
                from timestamp_millis(last_modified_time) at time zone 'Europe/Kiev'
            ) as last_modified
        from `quick-hub-296518.analytics_251320503.__TABLES__`
        where table_id like 'events_20%'
    ),

    final as (
        select table_name
        from tables
        where
            last_modified
            > datetime_sub(current_datetime('Europe/Kiev'), 
            interval 3450 minute))  -- здесь на проде нужно выставить 1450 мин

select array(select * from final)
