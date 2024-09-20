WITH
  ga4_source AS (
    SELECT
      *
    FROM
      { { source ('analytics_293084740', 'events') } }
  ),
  prep AS (
    SELECT
      event_name,
      items.item_name,
      items.item_category,
      SUM(IFNULL(items.quantity, 1)) AS items
    FROM
      ga4_source,
      UNNEST (items) AS items
    GROUP BY
      event_name,
      item_name,
      item_category
  ),
  final AS (
    SELECT
      item_name,
      item_category,
      SUM(
        CASE
          WHEN event_name = 'view_item' THEN items
          ELSE 0
        END
      ) AS view_item,
      SUM(
        CASE
          WHEN event_name = 'add_to_wishlist' THEN items
          ELSE 0
        END
      ) AS add_to_wishlist,
      SUM(
        CASE
          WHEN event_name = 'add_to_cart' THEN items
          ELSE 0
        END
      ) AS add_to_cart,
      SUM(
        CASE
          WHEN event_name = 'remove_from_cart' THEN items
          ELSE 0
        END
      ) AS remove_from_cart,
      SUM(
        CASE
          WHEN event_name = 'begin_checkout' THEN items
          ELSE 0
        END
      ) AS begin_checkout,
      SUM(
        CASE
          WHEN event_name = 'add_shipping_info' THEN items
          ELSE 0
        END
      ) AS add_shipping_info,
      SUM(
        CASE
          WHEN event_name = 'add_payment_info' THEN items
          ELSE 0
        END
      ) AS add_payment_info,
      SUM(
        CASE
          WHEN event_name = 'purchase' THEN items
          ELSE 0
        END
      ) AS purchase,
      ROUND(
        SAFE_DIVIDE(
          SUM(
            CASE
              WHEN event_name = 'purchase' THEN items
              ELSE 0
            END
          ),
          SUM(
            CASE
              WHEN event_name = 'view_item' THEN items
              ELSE 0
            END
          )
        ),
        2
      ) AS view_to_purchase_rate
    FROM
      prep
    GROUP BY
      item_name,
      item_category
    ORDER BY
      view_item DESC
  )
SELECT
  *
FROM
  final
