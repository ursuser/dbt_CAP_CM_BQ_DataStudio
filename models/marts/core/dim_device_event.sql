with
    ga4_source as

    (select * from {{ source("analytics_293084740", "events") }}),

    events_by_device as

    (
        select distinct
            parse_date('%Y%m%d', event_date) as event_date,
            device.category,
            event_name,
            count(event_name) as quantity
        from ga4_source
        group by event_date, device.category, event_name
    )

    {% set partitions_to_replace = [
        "date(date_sub(current_date, interval 1 day))",
        "date(date_sub(current_date, interval 2 day))",
        "date(date_sub(current_date, interval 3 day))",
    ] %}

    {{
        config(
            materialized="incremental",
            incremental_strategy="insert_overwrite",
            partition_by={"field": "event_date", "data_type": "date"},
            partitions=partitions_to_replace,
        )
    }}

select *
from events_by_device

{% if is_incremental() %}
    where event_date in ({{ partitions_to_replace | join(",") }})
{% endif %}
