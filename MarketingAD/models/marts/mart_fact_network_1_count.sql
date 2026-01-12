with mart as (
    select *
    from {{ ref('stg_ae_ad_network_1_country_report') }}
)

select count(*) as total_rows
from mart
