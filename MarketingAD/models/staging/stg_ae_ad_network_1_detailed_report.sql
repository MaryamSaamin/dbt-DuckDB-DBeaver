with raw as (
    select *
    from {{ ref('raw_ae_ad_network_1_detailed_report') }}
),

-- 1. Add CAMPAIGN_NAME
with_campaign_name as (
    select
        d.*,
        u.campaign_name
    from raw d
    left join {{ ref('raw_ae_ad_network_1_campaign_updates') }} u
        on d.campaign_id = u.campaign_id
       and d.update_date = u.update_date
),

-- 2. Map COUNTRY_CODE and COUNTRY_CODE_ID using STATE_ID → digits_only
with_country as (
    select
        d.*,
        g.country_code,
        g.country_code_id
    from with_campaign_name d
    left join {{ ref('stg_geo_dictionary') }} g
        on cast(d.state_id as varchar) = cast(g.digits_only as varchar)
)

select *
from with_country
