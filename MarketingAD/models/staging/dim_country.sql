with geo as (
    select *
    from {{ ref('raw_ae_ad_network_1_geo_dictionary') }}
)

select
    country_code,
    row_number() over (order by country_code) as country_code_id
from geo
where country_code is not null
group by country_code
