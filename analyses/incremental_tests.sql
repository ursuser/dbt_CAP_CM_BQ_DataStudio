SELECT
  event_date,
  user_pseudo_id,
  device.category,
  platform,
  geo.city,
  CONCAT(
    user_pseudo_id,
    (
      SELECT
        value.int_value
      FROM
        UNNEST (event_params)
      WHERE
        key = 'ga_session_id'
    )
  ) session_id,
  traffic_source.medium AS first_user_medium,
  traffic_source.source AS first_user_source,
  traffic_source.name AS first_user_campaign,
  COUNTIF(event_name = 'session_start') AS session_start,
  COUNTIF(event_name = 'view_item') AS view_item,
  COUNTIF(event_name = 'begin_checkout') AS begin_checkout,
  COUNTIF(event_name = 'add_to_cart') AS add_to_cart,
  COUNTIF(event_name = 'add_shipping_info') AS add_shipping_info,
  COUNTIF(event_name = 'add_payment_info') AS add_payment_info,
  COUNTIF(event_name = 'purchase') AS purchase,
  ROUND(ecommerce.purchase_revenue, 2) AS revenue
FROM
  { { source ("analytics_293084740", "events") } }
GROUP BY
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
ORDER BY
  event_date
