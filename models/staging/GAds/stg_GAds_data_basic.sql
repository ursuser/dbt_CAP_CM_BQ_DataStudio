SELECT
    segments_date,
    campaign_id,
    SUM(metrics_impressions) AS impr,
    SUM(metrics_clicks) AS clicks,
    SUM(metrics_cost_micros) / 1000000 AS cost
FROM
   {{source('google_ads_322_300_5315', 'p_ads_CampaignBasicStats_3223005315')}}
GROUP BY
  segments_date,
  campaign_id
ORDER BY
  segments_date DESC