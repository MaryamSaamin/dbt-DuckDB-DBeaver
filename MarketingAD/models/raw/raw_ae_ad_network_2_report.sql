{{ config(materialized='table') }}

select
    cast(id as bigint)              as campaign_id,
    campaign_name                   as campaign_name,
    cast(date as date)              as update_date,
    cast(spend as decimal(10,2))    as spend,
    cast(impressions as int)        as impressions,
    cast(clicks as int)             as clicks
from {{ source('marketing_raw', 'ae_ad_network_2_report') }}
