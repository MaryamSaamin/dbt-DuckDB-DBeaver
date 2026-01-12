with country_report as (
    select *
    from {{ ref('stg_ae_ad_network_1_country_report') }} -- your existing country report staging model
),

detailed_report_agg as (
    select
        campaign_id,
        update_date,
        country_id,
        sum(spend) as spend,
        sum(impressions) as impressions,
        sum(clicks) as clicks
    from {{ ref('stg_ae_ad_network_1_detailed_report') }} -- your existing detailed report staging model
    group by campaign_id, update_date, country_id
),

country_with_state_match as (
    select
        c.*,
        case
            when abs(c.spend - d.spend) <= 0.05
             and c.impressions = d.impressions
             and c.clicks = d.clicks
            then 1
            else 0
        end as is_state_match
    from country_report c
    left join detailed_report_agg d
        on c.campaign_id = d.campaign_id
       and c.update_date = d.update_date
       and c.country_id = d.country_id
)

select *
from country_with_state_match
