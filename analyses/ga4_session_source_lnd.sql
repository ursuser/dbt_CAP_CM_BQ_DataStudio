-- ad-hoc analysis: session source/medium/campaign via last-non-direct-click
-- attribution across a user's sessions (lookback window per user_pseudo_id).
-- adapted from kvitu_ua_dbt/models/intermediate/int_session_real_sm.sql,
-- simplified for cap_cm: no gclid -> google ads campaign name enrichment
-- (no click-level ads data source available in this project, only
-- p_ads_CampaignBasicStats). gclid/dclid/wbraid/gbraid are still used to
-- classify source/medium as google/cpc.
--
-- edit start_date / end_date / lookback_days below, then run to check
-- which sources purchases came from in a given period.

{% set start_date = '2026-06-01' %}
{% set end_date = '2026-06-30' %}
{% set lookback_days = 30 %}

with

    ga4_source as (
        select *
        from {{ source("analytics_293084740", "events") }}
        where
            regexp_extract(_table_suffix, '[0-9]+') between format_date(
                '%Y%m%d',
                date_sub(date('{{ start_date }}'), interval {{ lookback_days }} day)
            ) and format_date('%Y%m%d', date('{{ end_date }}'))
    ),

    prev_prep as (
        select
            user_pseudo_id,
            concat(
                user_pseudo_id,
                '_',
                (
                    select value.int_value
                    from unnest(event_params)
                    where key = 'ga_session_id'
                )
            ) as session_id,
            min(event_timestamp) as session_start_at,
            min(parse_date('%Y%m%d', event_date)) as event_date,
            min_by(
                (
                    select value.string_value
                    from unnest(event_params)
                    where key = 'page_location'
                ),
                event_timestamp
            ) as page_location,
            min_by(
                bqutil.fn_eu.url_parse(
                    (
                        select value.string_value
                        from unnest(event_params)
                        where key = 'page_referrer'
                    ),
                    'HOST'
                ),
                event_timestamp
            ) as referrer
        from ga4_source
        where user_pseudo_id is not null
        group by 1, 2
    ),

    raw as (
        select
            * except (referrer),
            regexp_replace(referrer, r'^www\.', '') as referrer,
            net.reg_domain(page_location) as lp_domain,
            net.reg_domain(referrer) as referrer_domain
        from prev_prep
    ),

    parse_data as (
        select
            *,
            bqutil.fn_eu.url_param(page_location, 'utm_source') as source,
            bqutil.fn_eu.url_param(page_location, 'utm_medium') as medium,
            bqutil.fn_eu.url_param(page_location, 'utm_campaign') as campaign,
            bqutil.fn_eu.url_param(page_location, 'gclid') as gclid,
            bqutil.fn_eu.url_param(page_location, 'dclid') as dclid,
            bqutil.fn_eu.url_param(page_location, 'wbraid') as wbraid,
            bqutil.fn_eu.url_param(page_location, 'gbraid') as gbraid,
            bqutil.fn_eu.url_param(page_location, 'gclsrc') as gclsrc,
            bqutil.fn_eu.url_param(page_location, 'fbclid') as fbclid
        from raw
    ),

    sessions as (
        select
            * except (
                source, medium, campaign, gclid, dclid, wbraid, gbraid,
                gclsrc, fbclid, lp_domain, referrer_domain
            ),
            case
                when
                    gclid is not null
                    or dclid is not null
                    or wbraid is not null
                    or gbraid is not null
                    or gclsrc is not null
                then 'google'
                when source is not null
                then source
                when fbclid is not null
                then 'facebook'
                when referrer is not null
                then
                    case
                        when lp_domain = referrer_domain
                        then null
                        when
                            regexp_contains(
                                referrer,
                                r'liqpay\.|paypal\.|privat24\.|wayforpay\.|stripe\.|accounts\.(google|youtube)|login\.microsoftonline'
                            )
                        then null  -- unwanted referral (checkout/login redirects)
                        when
                            regexp_contains(
                                referrer,
                                r'^google\.com$|^google\.(com?\.)?(ar|au|br|co|hk|mx|my|ng|pe|ph|pk|sa|sg|tr|tw|ua|vn|de|es|fr|gr|hu|ie|it|nl|pl|pt|ro|ru|se|uk|ae|ca|in|za|lt|be|bg|ch|cl|il|jp|kr|th|bo|kh|dk|ee|fi|gg|no|at|id|nz|uz|uy|hr|lk)$'
                            )
                        then 'google'
                        when referrer in ('bing.com', 'cn.bing.com')
                        then 'bing'
                        when referrer in ('yahoo.com', 'yahoo.cn')
                        then 'yahoo'
                        when referrer in ('yandex.com', 'yandex.ru')
                        then 'yandex'
                        when referrer in ('duckduckgo.com')
                        then 'duckduckgo'
                        when referrer in ('ukr.net', 'search.ukr.net')
                        then 'ukr'
                        when referrer in ('go.mail.ru')
                        then 'go.mail.ru'
                        when contains_substr(referrer, 'facebook.')
                        then 'facebook'
                        when contains_substr(referrer, 'instagram.')
                        then 'instagram'
                        when contains_substr(referrer, 'youtube.')
                        then 'youtube'
                        when contains_substr(referrer, 'tiktok.')
                        then 'tiktok'
                        when contains_substr(referrer, 'linkedin.')
                        then 'linkedin'
                        when contains_substr(referrer, 'twitter.')
                        then 'twitter'
                        when regexp_contains(referrer, r'(\.|^)vk\.com$')
                        then 'vk'
                        when regexp_contains(referrer, r'(\.|^)ok\.ru$')
                        then 'ok'
                        else referrer
                    end
            end as source,
            case
                when
                    gclid is not null
                    or dclid is not null
                    or wbraid is not null
                    or gbraid is not null
                    or gclsrc is not null
                then 'cpc'
                when medium is not null
                then medium
                when source is not null
                then '(none)'
                when referrer is not null
                then
                    case
                        when lp_domain = referrer_domain
                        then null
                        when
                            regexp_contains(
                                referrer,
                                r'liqpay\.|paypal\.|privat24\.|wayforpay\.|stripe\.|accounts\.(google|youtube)|login\.microsoftonline'
                            )
                        then null
                        when
                            regexp_contains(
                                referrer,
                                r'^google\.com$|^google\.(com?\.)?(ar|au|br|co|hk|mx|my|ng|pe|ph|pk|sa|sg|tr|tw|ua|vn|de|es|fr|gr|hu|ie|it|nl|pl|pt|ro|ru|se|uk|ae|ca|in|za|lt|be|bg|ch|cl|il|jp|kr|th|bo|kh|dk|ee|fi|gg|no|at|id|nz|uz|uy|hr|lk)$'
                            )
                            or referrer in ('bing.com', 'cn.bing.com', 'yahoo.com', 'yahoo.cn', 'yandex.com', 'yandex.ru', 'duckduckgo.com', 'ukr.net', 'search.ukr.net', 'go.mail.ru')
                        then 'organic'
                        when
                            regexp_contains(
                                referrer,
                                r'(facebook|twitter|linkedin|instagram|youtube|tiktok)\.|(\.|^)(vk\.com|ok\.ru)$'
                            )
                        then 'social'
                        else 'referral'
                    end
            end as medium,
            case
                when campaign is not null
                then campaign
                when
                    gclid is not null
                    or dclid is not null
                    or wbraid is not null
                    or gbraid is not null
                then 'Campaign without UTM'  -- no click-level ads data to enrich with real campaign name
                when medium is not null or source is not null
                then '(none)'
            end as campaign
        from parse_data
    ),

    -- last-non-direct-click: source/medium/campaign are carried together from
    -- the same session (the last one with a non-null source), not picked
    -- independently per field -- avoids mixing e.g. an organic session's
    -- source/medium with a campaign_id left over from an earlier paid touch.
    last_touch as (
        select
            event_date,
            user_pseudo_id,
            session_id,
            last_value(
                if(source is not null, struct(source, medium, campaign), null)
                ignore nulls
            ) over (user) as touch
        from sessions
        window
            user as (
                partition by user_pseudo_id
                order by session_start_at
                range between {{ lookback_days * 86400000000 }} preceding and current row
            )
    ),

    final as (
        select
            event_date,
            user_pseudo_id,
            session_id,
            ifnull(touch.source, '(direct)') as source_lnd,
            ifnull(touch.medium, '(none)') as medium_lnd,
            ifnull(touch.campaign, '(none)') as campaign_lnd
        from last_touch
    ),

    purchases as (
        select
            parse_date('%Y%m%d', event_date) as event_date,
            (
                select value.string_value
                from unnest(event_params)
                where key = 'transaction_id'
            ) as transaction_id,
            ecommerce.purchase_revenue as revenue,
            concat(
                user_pseudo_id,
                '_',
                (
                    select value.int_value
                    from unnest(event_params)
                    where key = 'ga_session_id'
                )
            ) as session_id
        from ga4_source
        where
            event_name = 'purchase'
            and event_date between format_date('%Y%m%d', date('{{ start_date }}'))
            and format_date('%Y%m%d', date('{{ end_date }}'))
    )

select
    coalesce(final.source_lnd, '(direct)') as source,
    coalesce(final.medium_lnd, '(none)') as medium,
    coalesce(final.campaign_lnd, '(none)') as campaign,
    count(distinct purchases.transaction_id) as purchases,
    round(sum(purchases.revenue), 2) as revenue,
    round(safe_divide(sum(purchases.revenue), count(distinct purchases.transaction_id)), 2) as avg_order_value
from purchases
left join final on purchases.session_id = final.session_id
group by 1, 2, 3
order by revenue desc
