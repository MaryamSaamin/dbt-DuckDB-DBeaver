select distinct r.country_id
from {{ ref('raw_ae_ad_network_1_country_report') }} r
left join {{ ref('stg_geo_dictionary') }} g
    on r.country_id = g.digits_only
where g.digits_only is null
order by r.country_id

