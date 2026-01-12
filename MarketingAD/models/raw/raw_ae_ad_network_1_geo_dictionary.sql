{{ config(materialized='table') }}

select
    cast(id as int)        as location_id,
    country_code           as country_code, 
    name                   as location_name,
    location_type          as location_type 
from {{ source('marketing_raw', 'ae_ad_network_1_geo_dictionary') }}
