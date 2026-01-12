select distinct r.country_code
from {{ ref('stg_ae_ad_network_2_report') }} r
left join {{ ref('stg_geo_dictionary') }} g
    on r.country_code = g.country_code
where r.country_code_id is null
