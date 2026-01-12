{{ config(materialized='table') }}

select
    cast(date as date)           as update_date,
    cast(campaign_id as bigint)  as campaign_id,
    cast(country_id as int)      as country_id,
    cast(spend as decimal(12,2)) as spend,
    cast(impressions as int)     as impressions,
    cast(clicks as int)          as clicks
from {{ source('marketing_raw', 'ae_ad_network_1_country_report') }}
