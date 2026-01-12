with raw as (
    select *
    from {{ ref('raw_ae_ad_network_1_country_report') }}
),

-- 🔑 ONE row per digits_only
geo_map as (
    select
        cast(digits_only as varchar) as digits_only,
        any_value(country_code) as country_code,
        any_value(country_code_id) as country_code_id
    from {{ ref('stg_geo_dictionary') }}
    where digits_only is not null
    group by digits_only
),

-- Safe 1-to-1 join
geo as (
    select
        r.*,
        g.country_code,
        g.country_code_id
    from raw r
    left join geo_map g
        on cast(r.country_id as varchar) = g.digits_only
),

campaign as (
    select
        campaign_id,
        update_date,
        max(campaign_name) as campaign_name
    from {{ ref('raw_ae_ad_network_1_campaign_updates') }}
    group by campaign_id, update_date
)

select
    g.*,
    c.campaign_name
from geo g
left join campaign c
    on g.campaign_id = c.campaign_id
   and g.update_date = c.update_date
