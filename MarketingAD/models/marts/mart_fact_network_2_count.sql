with mart as (
    select *
    from {{ ref('stg_ae_ad_network_2_report') }}
)

select count(*) as total_rows
from mart
