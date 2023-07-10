with
    ga4_source as

    (select * from {{ source("analytics_293084740", "events_202*") }}),

   {% set partitions_to_replace = [
        "timestamp(current_date)",
        "timestamp(date_sub(current_date, interval 3 day))"
    ] %}

    {{
        config(
            materialized="incremental",
            incremental_strategy="insert_overwrite",
            partition_by={"field": "_table_suffix", "data_type": "string"},
            partitions=partitions_to_replace
        )
    }}

    events_by_device as

    (
        select distinct
            event_date,
            device.category, 
            event_name, 
            count(event_name) as quantity
        from ga4_source
        where device.category = 'desktop'
        group by 
            event_date,
            device.category, 
            event_name
    )

{% if is_incremental() %}
    where
        timestamp_trunc(timestamp_micros(event_timestamp), day)
        in ({{ partitions_to_replace | join(",") }})
{% endif %}

select *
from events_by_device
