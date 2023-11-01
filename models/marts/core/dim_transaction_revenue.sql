with
    ga4_source as

    (select * from {{ source("analytics_293084740", "events") }}),

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

    final as

    (
        select
            parse_date('%Y%m%d', event_date) as event_date,
            user_id,
            platform,
            (
                select value.string_value
                from unnest(event_params)
                where key = "transaction_id"
            ) as transaction_id,
            ecommerce.unique_items as unique_items,
            ecommerce.total_item_quantity as quantity,
            round(ecommerce.purchase_revenue, 2) as revenue
        from ga4_source
        where event_name = 'purchase'
    )

select *
from final
