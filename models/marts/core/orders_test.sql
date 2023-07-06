{{ config(
    materialized = 'incremental',
    unique_key = 'order_id'
) }}


SELECT
    event_date,
    order_id,
    revenue
FROM
    `cap-cm-md.testing_dataset.orders`
    
{% if is_incremental() %}
    WHERE event_date > (SELECT MAX(event_date) FROM {{ this }})
{% endif %}
