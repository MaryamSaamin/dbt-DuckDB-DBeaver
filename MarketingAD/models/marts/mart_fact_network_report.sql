with ad1 as (
    select
        campaign_id,
        campaign_name,
        cast(update_date as date) as update_date,
        spend,
        impressions,
        clicks,
        country_code_id,
        country_code,
        '1' as campaign_code
    from {{ ref('stg_ae_ad_network_1_country_report') }}
    where country_code is not null
    
),

ad2 as (
    select
        campaign_id,
        campaign_name,
        cast(update_date as date) as update_date,
        spend,
        impressions,
        clicks,
        country_code_id,
        country_code,
        '2' as campaign_code
    from {{ ref('stg_ae_ad_network_2_report') }}
    where country_code is not null
)

select *
from ad1
union all
select *
from ad2
