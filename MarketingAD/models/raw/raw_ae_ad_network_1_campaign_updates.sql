{{ config(materialized='table') }}

select
    cast(campaign_id as decimal(38,0)) as campaign_id,
    cast(update_date as date)          as update_date,
    name                               as campaign_name
from {{ source('marketing_raw', 'ae_ad_network_1_campaign_updates') }}
