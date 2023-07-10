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

    final as

    (
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
            ) session_id,
            countif(event_name = 'session_start') as session_start,
            countif(event_name = 'view_item') as view_item,
            countif(event_name = 'begin_checkout') as begin_checkout,
            countif(event_name = 'add_to_cart') as add_to_cart,
            countif(event_name = 'add_shipping_info') as add_shipping_info,
            countif(event_name = 'add_payment_info') as add_payment_info,
            countif(event_name = 'purchase') as purchase
        from ga4_source
        group by event_date, user_pseudo_id, session_id
    )

{% if is_incremental() %}
    where
        timestamp_trunc(timestamp_micros(event_timestamp), day)
        in ({{ partitions_to_replace | join(",") }})
{% endif %}

select *
from final
