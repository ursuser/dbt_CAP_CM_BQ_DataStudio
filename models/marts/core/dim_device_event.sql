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


with
    ga4_source as

    (
        select *
        from {{ source("analytics_293084740", "events") }}

         {% if is_incremental() %}
            where
                parse_date('%Y%m%d', regexp_extract(_table_suffix, '[0-9]+'))
                in ({{ partitions_to_replace | join(",") }})
        {% endif %}

    ),

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

select *
from events_by_device
