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

            case
                when max(stitch.user_id) is not null
                then max(stitch.user_id)
                else events.user_pseudo_id
            end as user_pseudo_id,

            device.category as device_category,
            geo.country as country,
            geo.city as city,

            concat(
                events.user_pseudo_id,
                '.',
                (
                    select value.int_value
                    from unnest(event_params)
                    where key = 'ga_session_id'
                )
            ) as real_session_id,

            (
                select value.int_value
                from unnest(event_params)
                where key = 'ga_session_number'
            ) as session_number,

            max(
                (
                    select value.string_value
                    from unnest(event_params)
                    where key = 'session_engaged'
                )
            ) as session_engaged,
            max(
                (
                    select value.int_value
                    from unnest(event_params)
                    where key = 'engagement_time_msec'
                )
            ) as engagement_time_msec,
            -- ,countif(event_name = 'session_start') as session_start
            -- ,countif(event_name = 'first_visit') as first_visit
            countif(event_name = 'page_view') as page_view,
            countif(event_name = 'view_item') as view_item,
            countif(event_name = 'add_to_wishlist') as add_to_wishlist,
            countif(event_name = 'select_item') as select_item,
            countif(event_name = 'add_to_cart') as add_to_cart,
            countif(event_name = 'remove_from_cart') as remove_from_cart,
            countif(event_name = 'begin_checkout') as begin_checkout,
            countif(event_name = 'add_payment_info') as add_payment_info,
            countif(event_name = 'add_shipping_info') as add_shipping_info,
            countif(event_name = 'purchase') as purchase,
            sum(ecommerce.purchase_revenue) as total_revenue

        from ga4_source as events
        left join
            {{ ref("int_user_id_mapping") }} as stitch
            on events.user_pseudo_id = stitch.user_pseudo_id
        group by
            event_date,
            event_timestamp,
            events.user_pseudo_id,
            device_category,
            country,
            city,
            real_session_id,
            session_number
    ),

    final as (

        select
            events.event_date,
            events.event_timestamp,
            events.user_pseudo_id,
            device_category,
            country,
            city,
            events.real_session_id,
            real_session_sm,
            real_session_campaign,  -- названия переписать как было, чтобы не переделывать все отчеты в лукере еще
            session_number,
            session_engaged,
            engagement_time_msec,
            page_view,
            view_item,
            add_to_wishlist,
            select_item,
            add_to_cart,
            remove_from_cart,
            begin_checkout,
            add_payment_info,
            add_shipping_info,
            purchase,
            total_revenue
        from prev_prep as events
        left join
            {{ ref("int_session_real_sm") }} as sessions
            on events.real_session_id = sessions.real_session_id
            and events.event_timestamp = sessions.event_timestamp
    )

select *
from final
