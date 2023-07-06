{{
    config
    (
        materialized="incremental", 
        unique_key="session_id"
    )
}}

select
    event_date,
    user_pseudo_id,
    device.category,
    platform,
    geo.city,
    concat(
        user_pseudo_id,
        (select value.int_value from unnest(event_params) where key = 'ga_session_id')
    ) session_id,
    traffic_source.medium as first_user_medium,
    traffic_source.source as first_user_source,
    traffic_source.name as first_user_campaign,
    countif(event_name = 'session_start') as session_start,
    countif(event_name = 'view_item') as view_item,
    countif(event_name = 'begin_checkout') as begin_checkout,
    countif(event_name = 'add_to_cart') as add_to_cart,
    countif(event_name = 'add_shipping_info') as add_shipping_info,
    countif(event_name = 'add_payment_info') as add_payment_info,
    countif(event_name = 'purchase') as purchase,
    round(ecommerce.purchase_revenue, 2) as revenue
from {{ source("analytics_293084740", "events_202*") }}
group by
    event_date,
    user_pseudo_id,
    session_id,
    device.category,
    platform,
    geo.city,
    revenue,
    first_user_medium,
    first_user_source,
    first_user_campaign

{% if is_incremental() %}
    where
        event_timestamp > (select max(event_timestamp) from {{ this }})
        or (
            event_timestamp = (select max(event_timestamp) from {{ this }})
            and event_date > (select max(event_date) from {{ this }})
        )
{% endif %}
